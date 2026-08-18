{#
  dedup_audit_post_hook.sql -- the atomic, same-transaction meta.dedup_audit
  + meta.dedup_decisions write (D-09, DEDUP-04).

  Called from a silver model's own `post_hook` config (never
  `transaction=False` -- a Postgres `post_hook` runs inside the SAME
  transaction as the model's own DML by default, 08.1-RESEARCH.md
  Architecture Pattern 2 -- so a rollback of the model's own write also
  rolls back this audit write).

  Design choices worth documenting explicitly (deviate from this plan's own
  literal `dedup_audit_post_hook(dataset_id, business_key_column,
  watermark_floor)` sketch, which assumed a pre-resolved `dataset_id`
  integer and a re-derived ranking CTE over bronze alone) -- ALL three
  verified empirically against a real, multi-invocation `dbt build` while
  developing this plan; each replaced an approach that looked correct on a
  single first run but broke on a second (incremental) run:

  1. `dataset_name` (a plain string, e.g. 'customers'), not a pre-resolved
     `dataset_id` integer, is accepted here -- `dataset_id` is resolved via
     `meta.dataset_id_for_name(text)`, a `SECURITY DEFINER` function
     (migration 0028) called INSIDE this macro's own INSERT, evaluated at
     hook-EXECUTION time. A direct `select dataset_id from meta.datasets
     where dataset_name = ...` subquery was tried first and rejected: it
     needs `SELECT` on `meta.datasets`, which fails
     `test_dbt_app_role_is_scoped_correctly` (D-08's explicit boundary --
     `dbt_app` gets zero grant on `meta.*` beyond `dedup_audit`/
     `dedup_decisions`, 08.1-01-PLAN.md) and would expose every column of
     `meta.datasets`, not just the one mapping needed. The function narrows
     the interface to exactly `dataset_name -> dataset_id`; `dbt_app` holds
     `EXECUTE` on it, never `SELECT` on the table, so D-08's boundary test
     is unaffected.

  2. `source_schema`/`source_identifier`/`target_schema`/`target_identifier`
     are all plain STRINGS (e.g. 'staging', 'customers', 'silver'), never a
     `{{ source(...) }}`/`{{ this }}` Relation object passed as one of THIS
     macro's own arguments. Passing `source('bronze', 'customers')` and/or
     `this` as arguments to this macro -- called from inside a
     `{% set post_hook_sql %}...{% endset %}` capture block -- produced
     inconsistent, WRONG relation names across otherwise-identical runs
     (observed both `source_relation` and `target_relation` independently
     resolving to this MODEL's own default, un-aliased relation name at
     different times, never a reliable pattern tied to argument position or
     order). Plain string schema/identifier pairs, interpolated directly
     into `{{ schema }}.{{ identifier }}` inside this macro's own SQL body,
     sidestep that instability entirely.

  3. There is deliberately NO `watermark_floor` argument here at all. The
     original design passed the model's own `{% set watermark_floor =
     run_query(...) %}`-computed value straight into this macro. Empirically
     wrong on any run past the first: the model's OWN body (rendered in
     dbt's "real" compile pass) correctly saw `watermark_floor = 1` on a
     second invocation, but the SAME variable, captured into the
     `post_hook_sql` string that becomes part of `config()`'s own `post_hook`
     value, was frozen at `0` -- dbt evaluates whatever Jinja a `post_hook`
     config value depends on as part of a SEPARATE, EARLIER pass used to
     build the node's `model.config` (before the "real" compile pass that
     renders the model's SELECT body), and `run_query()` is not reliable in
     that earlier pass. Rather than depend on that timing at all, this
     macro derives its OWN watermark independently, from `meta.dedup_audit`'s
     own history for this model: `coalesce(max(max_run_id), 0)` across every
     PRIOR audit row for `model_name = target_identifier` (zero prior rows
     -> floor 0, matching "process everything" on a model's first-ever
     invocation). This is provably equivalent to the model's own
     `max(_run_id) from <target>` floor under normal operation, since
     `meta.dedup_audit` and the target table are always written together in
     the SAME transaction -- and it needs no value handed across the
     unreliable config-vs-compile pass boundary at all.

  4. Rather than re-deriving a SECOND, independent `row_number()` ranking
     over bronze alone (which cannot see rows the calling model's own
     ranking compared against a pre-existing SILVER row for the same
     business key -- the exact case D-06's late-arrival-loses test proves),
     this macro compares each new bronze row directly against the target
     table's OWN, now-materialized, POST-write state: a bronze row is
     "kept" iff its own `(_file_id, _source_row_number)` matches the row
     currently resident in the target for that business key; every other
     new bronze row for that key is "dropped". This is provably consistent
     with whatever the model's own materialization actually decided (it
     reads the real outcome, not a parallel re-derivation that could drift
     from it), and it naturally covers BOTH within-batch duplicates and a
     late-arriving row losing against an already-resident silver row, with
     one same shape.

  Args:
    dataset_name: the dataset's `meta.datasets.dataset_name` (e.g.
      'customers') -- resolved to its surrogate `dataset_id` by a scalar
      subquery below.
    business_key_column: the calling model's own business-key column name
      (e.g. 'customer_id').
    source_schema: the bronze source's schema (e.g. 'staging', matching
      `dbt/models/staging/_sources.yml`'s `schema: staging`).
    source_identifier: the bronze source's table name (e.g. 'customers').
    target_schema: the schema the calling model's OWN table lives in (e.g.
      `target.schema`, always 'silver').
    target_identifier: the calling model's own configured `alias` (e.g.
      'customers') -- also used as `meta.dedup_audit.model_name`, so this
      macro's own self-derived watermark (point 3 above) stays scoped to
      the correct model across invocations.
#}
{% macro dedup_audit_post_hook(dataset_name, business_key_column, source_schema, source_identifier, target_schema, target_identifier) %}
with prior_watermark as (
    select coalesce(max(max_run_id), 0) as floor
    from meta.dedup_audit
    where model_name = '{{ target_identifier }}'
),

new_bronze as (
    select b.*
    from {{ source_schema }}.{{ source_identifier }} b
    cross join prior_watermark
    where b._run_id > prior_watermark.floor
),

current_winners as (
    select *
    from {{ target_schema }}.{{ target_identifier }}
),

classified as (
    select
        b._file_id            as bronze_file_id,
        b._source_row_number  as bronze_source_row,
        b._record_hash        as bronze_record_hash,
        b._batch_id           as bronze_batch_id,
        b.{{ business_key_column }} as business_key_value,
        w._file_id             as winner_file_id,
        w._source_row_number   as winner_source_row,
        w._record_hash         as winner_record_hash,
        w._batch_id            as winner_batch_id,
        (b._file_id = w._file_id and b._source_row_number = w._source_row_number) as is_current_winner
    from new_bronze b
    join current_winners w
      on w.{{ business_key_column }} = b.{{ business_key_column }}
),

audit_insert as (
    insert into meta.dedup_audit (
        dataset_id, dbt_invocation_id, model_name,
        min_run_id, max_run_id,
        records_received, records_accepted, records_rejected, records_deduplicated
    )
    select
        meta.dataset_id_for_name('{{ dataset_name }}'),
        '{{ invocation_id }}',
        '{{ target_identifier }}',
        (select min(_run_id) from new_bronze),
        (select max(_run_id) from new_bronze),
        (select count(*) from new_bronze),
        (select count(*) from classified where is_current_winner),
        0,
        (select count(*) from classified where not is_current_winner)
    returning dedup_audit_id
)

insert into meta.dedup_decisions (
    dedup_audit_id, record_hash, business_key,
    kept_file_id, kept_source_row, dropped_file_id, dropped_source_row, reason
)
select
    (select dedup_audit_id from audit_insert),
    c.bronze_record_hash,
    jsonb_build_object('{{ business_key_column }}', c.business_key_value),
    c.winner_file_id,
    c.winner_source_row,
    c.bronze_file_id,
    c.bronze_source_row,
    case
        when c.bronze_batch_id = c.winner_batch_id
             and c.bronze_record_hash = c.winner_record_hash
            then 'EXACT_DUP_IN_FILE'
        when c.bronze_record_hash = c.winner_record_hash
            then 'EXACT_DUP_CROSS_BATCH'
        else 'SUPERSEDED_BY_NEWER'
    end as reason
from classified c
where not c.is_current_winner
{% endmacro %}
