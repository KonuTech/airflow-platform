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
     that earlier pass. The first replacement (a self-derived
     `coalesce(max(max_run_id), 0)` floor over this model's own prior
     `meta.dedup_audit` rows) shared the calling model's OWN watermark bug
     (debug ci-pipeline-ingestion-timeout ROUND 16, finding 21): a run
     whose bronze rows commit after a higher `_run_id` has already been
     audited falls below the floor forever and its rows are never counted.
     Since finding 21 replaced the models' watermark with the
     `meta.dbt_processed_runs` claim ledger (migration 0040;
     claim_dbt_processed_runs.sql pre-hook, SAME transaction), this macro
     now scopes `new_bronze` to exactly THIS transaction's claimed set
     (`claimed_txid = txid_current()`) -- byte-for-byte the same
     eligibility set the calling model's own SELECT body used, evaluated in
     the database at execution time, with still no value handed across the
     unreliable config-vs-compile pass boundary (and no
     `{{ invocation_id }}` predicate -- see the partial-parsing
     stale-literal hazard reconciliation_post_hook.sql's own point 3
     documents).

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

  5. The audit INSERT is guarded on `meta.dataset_id_for_name(...) IS NOT
     NULL` -- i.e. it writes a row only when the dataset is REGISTERED in
     `meta.datasets`. On a fresh deployment, `dbt build` runs the WHOLE
     project, but `meta.datasets` rows are created only by each dataset's
     own ingestion path (`ConfigRegistry.sync()` / `get_or_create_dataset`
     -- there is no seed and no config-sync DAG yet), so a dataset whose
     first file has not yet been ingested has no row, the lookup function
     returns NULL, and an unguarded INSERT fails `dedup_audit.dataset_id`'s
     NOT NULL constraint (observed live: CI run 32873456327, silver_orders
     failing every build on a fresh cluster before orders' first ingest;
     latent locally only because 'orders' happened to be registered by
     early local ingests). The guard predicate is exactly "is the dataset
     registered": rows can only exist in `staging.<dataset>` AFTER that
     dataset's ingestion registered it, so an unregistered dataset always
     has an empty `new_bronze` and the skipped row is a zero-information
     no-op (counts 0, NULL run-id range). Deliberately NOT a
     skip-when-new_bronze-is-empty guard: idle registered-dataset builds
     keep writing 0-count rows -- the audit trail records that a build ran
     and found nothing, and downstream consumers (Grafana, sibling macro)
     were built against that shape. (`reconciliation_post_hook` no longer
     derives a floor from these rows at all -- both macros now scope to
     the `meta.dbt_processed_runs` claimed set, docstring point 3 -- so
     this is a stability choice, not a correctness dependency anymore.)
     If the registration invariant were ever
     breached (bronze rows for an unregistered dataset), the
     `meta.dedup_decisions` INSERT below would fail loudly on its NOT NULL
     `dedup_audit_id` rather than silently dropping audit data.

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
with new_bronze as (
    select b.*
    from {{ source_schema }}.{{ source_identifier }} b
    where b._run_id in (
        select run_id
        from meta.dbt_processed_runs
        where dataset_name = '{{ dataset_name }}'
          and claimed_txid = txid_current()
    )
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
    -- Registration guard -- docstring point 5. Unregistered dataset =>
    -- resolver returns NULL => write nothing (always a no-op row anyway,
    -- since staging.<dataset> is necessarily empty before first ingest).
    where meta.dataset_id_for_name('{{ dataset_name }}') is not null
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
