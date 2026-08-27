{#
  silver_customers.sql -- DEDUP-01..04/INCR-03/04/QUAL-10, D-05/D-06/D-07/
  D-11/D-14/D-15.

  `is_incremental()`-guarded, keyed off `_run_id` (never `event_ts`, D-06's
  hard constraint): a late-arriving bronze row with an OLD `event_ts` but a
  NEW `_run_id` is still picked up by the `where _run_id > {{
  watermark_floor }}` filter below -- `event_ts` decides ordering AMONG
  rows this filter already selected, never which rows get selected.

  D-07 layer 1 (business-key primary dedup): `row_number() over (partition
  by customer_id order by event_ts desc, _source_row_number desc)` -- this
  platform's one existing tie-break rule, carried forward verbatim from
  `merge.py`'s own `DISTINCT ON`/`ORDER BY` precedent (08.1-PATTERNS.md
  Section 1) -- a window function + outer filter, never the single-keyword
  `DISTINCT` shortcut Postgres's own `DISTINCT ON` uses at the gold layer
  (DEDUP-03: a dbt incremental model has no `QUALIFY` clause to lean on).
  D-07 layer 2 (exact-row-hash secondary dedup) is applied by
  `dedup_audit_post_hook`'s own classification, reading this SAME
  materialization's actual outcome, not a second independent filtering
  pass.

  Late-arrival correctness (this model's own must-have): a NEW bronze row
  for a business key already resident in silver must be ranked AGAINST
  that resident row, not merely against other bronze rows in the SAME
  incremental batch -- otherwise `delete+insert` would unconditionally
  replace a correct, later-`event_ts` silver row with a merely
  later-*arriving*, business-stale one. `existing_silver_contenders`
  brings the currently-resident row for any business key with a new
  bronze contender into the SAME ranking pool, so the winner is always the
  true latest-`event_ts` row regardless of which "side" (new bronze vs.
  already-silver) it came from.

  `_dbt_loaded_at` (D-11): dbt's own model SELECT populates this on every
  write -- migration 0023 deliberately leaves it with no Postgres
  `server_default`.
#}

{% set watermark_floor = 0 %}
{% if is_incremental() %}
  {% set watermark_floor_query %}
    select coalesce(max(_run_id), 0) from {{ this }}
  {% endset %}
  {% set watermark_floor = run_query(watermark_floor_query).columns[0].values()[0] | int %}
{% endif %}

{#-
  `post_hook_sql` is rendered HERE, eagerly, in the model's own primary
  render pass, and captured as a fully-substituted, static SQL string
  before being handed to `config(post_hook=...)`. This is deliberate, not
  redundant with dbt's own "wrap a hook macro call in an extra set of
  curly braces to defer its render" idiom (docs.getdbt.com/reference/
  resource-configs/pre-hook-post-hook, "The render method") -- that
  deferred form re-renders the hook string in a SEPARATE, EARLIER Jinja
  pass (the one dbt uses to build the node's own `model.config`, before
  the "real" compile pass that renders the model's SELECT body), verified
  empirically while developing this plan to be unreliable for BOTH a
  `run_query()`-computed `{% set %}` local (rendered as an empty string,
  producing a SQL syntax error) AND `{{ this }}`'s alias-aware name
  (rendered using the model's default, un-aliased identifier instead of
  its configured `alias`). `dedup_audit_post_hook`'s own arguments are
  therefore all plain, statically-known strings (see that macro's own
  docstring point 2) and its watermark is self-derived from
  `meta.dedup_audit`'s own history rather than passed in at all (point 3)
  -- this eager pre-render exists only so the fully-built `post_hook_sql`
  string itself is stable regardless of which pass dbt uses to read it.
-#}
{% set post_hook_sql %}
{{ dedup_audit_post_hook(
    dataset_name='customers',
    business_key_column='customer_id',
    source_schema='staging',
    source_identifier='customers',
    target_schema=target.schema,
    target_identifier='customers'
) }};
{{ reconciliation_post_hook(
    dataset_name='customers',
    source_schema='staging',
    source_identifier='customers',
    target_schema=target.schema,
    target_identifier='customers'
) }}
{% endset %}

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='customer_id',
    on_schema_change='append_new_columns',
    contract={'enforced': True},
    alias='customers',
    post_hook=post_hook_sql
) }}

with new_bronze as (
    select
        customer_id, name, country, birth_date, event_ts,
        _run_id, _file_id, _batch_id, _source_row_number,
        _record_hash, _record_hash_version
    from {{ source('bronze', 'customers') }}
    {% if is_incremental() %}
    where _run_id > {{ watermark_floor }}
    {% endif %}
),

existing_silver_contenders as (
    {% if is_incremental() %}
    select
        customer_id, name, country, birth_date, event_ts,
        _run_id, _file_id, _batch_id, _source_row_number,
        _record_hash, _record_hash_version
    from {{ this }}
    where customer_id in (select customer_id from new_bronze)
    {% else %}
    select
        null::text as customer_id, null::text as name, null::text as country,
        null::text as birth_date, null::text as event_ts,
        null::bigint as _run_id, null::bigint as _file_id, null::bigint as _batch_id,
        null::bigint as _source_row_number, null::bytea as _record_hash,
        null::smallint as _record_hash_version
    where false
    {% endif %}
),

all_contenders as (
    select * from new_bronze
    union all
    select * from existing_silver_contenders
),

business_key_ranked as (
    select
        *,
        {#
          `_run_id desc` final tie-break (debug ci-pipeline-ingestion-timeout
          ROUND 12, root cause 16): a D-18 replay re-stages BYTE-IDENTICAL
          rows (same event_ts/_source_row_number/_file_id -- discovery's
          create_file is idempotent by object_uri) under a new _run_id, so
          without this term the resident row and its replay tie on ALL
          ranking terms and the winner's _run_id lineage is arbitrary
          (observed live: a 23/27 split across 50 keys, run 32884691063) --
          a determinism violation (README section 67). Newest run wins:
          identical content, freshest lineage, deterministic result.
        #}
        row_number() over (
            partition by customer_id
            order by event_ts::timestamptz desc nulls last, _source_row_number desc,
                     _file_id desc, _run_id desc
        ) as rn
    from all_contenders
)

select
    customer_id, name, country, birth_date, event_ts,
    _run_id, _file_id, _batch_id, _source_row_number,
    _record_hash, _record_hash_version,
    now() as _dbt_loaded_at
from business_key_ranked
where rn = 1
