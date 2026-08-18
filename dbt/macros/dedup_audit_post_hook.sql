{#
  dedup_audit_post_hook.sql -- the atomic, same-transaction meta.dedup_audit
  + meta.dedup_decisions write (D-09, DEDUP-04).

  Called from a silver model's own `post_hook` config (never wrapped in a
  string/`transaction=False` -- a Postgres `post_hook` runs inside the SAME
  transaction as the model's own DML by default, 08.1-RESEARCH.md
  Architecture Pattern 2 -- so a rollback of the model's own write also
  rolls back this audit write).

  Design choice worth documenting explicitly (deviates from this plan's own
  literal `dedup_audit_post_hook(dataset_id, business_key_column,
  watermark_floor)` sketch, which assumed a pre-resolved `dataset_id`
  integer and a re-derived ranking CTE over bronze alone):

  1. `dataset_name` (a plain string, e.g. 'customers'), not a pre-resolved
     `dataset_id` integer, is accepted here -- `dataset_id` is resolved by
     a raw SQL scalar subquery against `meta.datasets` INSIDE this macro's
     own INSERT, evaluated at hook-EXECUTION time. This sidesteps a real
     compile-time ordering hazard: dbt's `config()` must textually precede
     any `is_incremental()` call that depends on it for THIS model's own
     `materialized` flag to be visible, but resolving `dataset_id` via a
     compile-time `run_query()` would need to run BEFORE the `config()`
     call that consumes it as a `post_hook` argument -- doable, but adds a
     second `run_query()` round-trip and a second Jinja-timing assumption
     for no real benefit over a plain SQL subquery that runs exactly once,
     in the same transaction, at the same time everything else here does.
     **Known gap, not fixed by this plan** (file scope is the dbt project
     only, not new migrations): `dbt_app` currently has `USAGE` on schema
     `meta` (migration 0024) and `SELECT, INSERT` on
     `meta.dedup_audit`/`meta.dedup_decisions` specifically, but no
     `SELECT` grant on `meta.datasets` itself -- a live `dbt build` running
     as `dbt_app` will fail this subquery with a permission error until a
     follow-up migration adds `GRANT SELECT ON meta.datasets TO dbt_app`.
     This plan's own integration tests (`tests/integration/test_dbt_*.py`)
     run `dbt build` against the testcontainers superuser DSN, not
     `dbt_app`, so they do not exercise this gap -- tracked in this plan's
     own SUMMARY.md, not silently left undocumented.

  2. Rather than re-deriving a SECOND, independent `row_number()` ranking
     over bronze alone (which cannot see rows the calling model's own
     ranking compared against a pre-existing SILVER row for the same
     business key -- the exact case D-06's late-arrival-loses test proves),
     this macro compares each new bronze row directly against `{{
     target_relation }}`'s OWN, now-materialized, POST-write state: a
     bronze row is "kept" iff its own `(_file_id, _source_row_number)`
     matches the row currently resident in `target_relation` for that
     business key; every other new bronze row for that key is "dropped".
     This is provably consistent with whatever the model's own
     materialization actually decided (it reads the real outcome, not a
     parallel re-derivation that could drift from it), and it naturally
     covers BOTH within-batch duplicates and a late-arriving row losing
     against an already-resident silver row, with one same shape.

  Args:
    dataset_name: the dataset's `meta.datasets.dataset_name` (e.g.
      'customers') -- resolved to its surrogate `dataset_id` by a scalar
      subquery below.
    business_key_column: the calling model's own business-key column name
      (e.g. 'customer_id').
    watermark_floor: the SAME `_run_id` floor value the calling model's own
      `is_incremental()`-guarded `WHERE` clause used -- captured ONCE by
      the caller (a `{% set %}` at the top of the model) and passed here
      unchanged, so the audit's notion of "what changed this run" can never
      drift from the model's own notion.
    source_relation: the compiled `source('bronze', <table>)` relation the
      calling model itself reads from -- passed in explicitly rather than
      re-derived from a table-name string, so this macro never has to parse
      `this.identifier` to guess which bronze source belongs to which model.
    target_relation: `{{ this }}` from the calling model's own context --
      always safe to reference here (unlike inside the model's own SELECT
      body pre-materialization) because a `post_hook` runs strictly AFTER
      the model's materialization completes, so the target table is
      guaranteed to already exist by this point, even on this model's
      very first-ever invocation (this project's silver tables are
      Alembic-created up front, migration 0023 -- `is_incremental()` is
      therefore true from dbt's very first build here, never false).
#}
{% macro dedup_audit_post_hook(dataset_name, business_key_column, watermark_floor, source_relation, target_relation) %}
with new_bronze as (
    select *
    from {{ source_relation }}
    where _run_id > {{ watermark_floor }}
),

current_winners as (
    select *
    from {{ target_relation }}
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
        (select dataset_id from meta.datasets where dataset_name = '{{ dataset_name }}'),
        '{{ invocation_id }}',
        '{{ target_relation.identifier }}',
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
