{#
  claim_dbt_processed_runs.sql -- the pre-hook that freezes THIS build's
  exact bronze-eligibility set into meta.dbt_processed_runs (debug/
  ci-pipeline-ingestion-timeout ROUND 16, finding 21; migration 0040).

  WHY A CLAIM LEDGER, NOT A WATERMARK: the silver models' old incremental
  filter (`_run_id > max(_run_id) from {{ this }}`) was a GLOBAL max
  watermark -- any run whose bronze rows commit AFTER a higher `_run_id` has
  already been dbt-built falls below the floor forever and is silently never
  selected by any later build. Observed live (run 33103279876): the sweep's
  late-file replay run (run 42) staged late in the replay wave, permanently
  absent from silver even though the run reached SUCCEEDED with 50 bronze
  rows. The same shape drops GENUINELY NEW rows whenever a stage retry /
  lease-reclaim completes after a higher run's dbt pass -- a direct
  no-silent-drops Core Value violation. This claim INSERT replaces the floor
  with an exact set: every bronze `_run_id` not yet in the ledger is claimed
  by THIS build; anything staged later (any `_run_id`, higher OR lower) is
  simply still unclaimed for the next build.

  WHY `claimed_txid = txid_current()`, NEVER `{{ invocation_id }}`: a
  Postgres pre-hook runs inside the SAME transaction as the model's own
  materialization (08.1-RESEARCH.md Architecture Pattern 2), so
  `txid_current()` is a shared, transaction-local identity the model's own
  SELECT body can filter on with zero values handed across Jinja render
  passes. `{{ invocation_id }}` was rejected for predicates outright:
  `reconciliation_post_hook.sql`'s docstring point 3 documents (reproduced
  live) that dbt's partial-parsing cache can silently reuse a PREVIOUS
  invocation's rendered hook string with a stale invocation-id literal
  frozen in -- a stale claim-vs-filter mismatch here would re-create the
  exact silent-drop class this ledger exists to kill. `txid_current()` is
  evaluated in the database at execution time and is immune to every
  Jinja-rendering/caching hazard by construction. A rolled-back build rolls
  its claims back with it (same transaction), so a failed build's runs stay
  claimable.

  EXACTNESS UNDER CONCURRENCY: two overlapping builds of the same model
  (both ingestion DAGs run unscoped `dbt build`s, so this is real) both
  evaluate `NOT EXISTS` before either commits; the second INSERT blocks on
  the primary key until the first transaction commits, then `ON CONFLICT DO
  NOTHING` drops the contested rows -- the loser's txid-scoped set excludes
  them and its model pass correctly no-ops for those runs. A bronze row
  committed between this INSERT and the model's SELECT is invisible to both
  (READ COMMITTED per-statement snapshots only ever ADD rows after this
  statement ran; the model filters on the CLAIMED set, not on bronze
  recency), so it stays unclaimed for the next build -- never half-processed.

  Args (all plain strings -- dedup_audit_post_hook.sql docstring point 2's
  Relation-object instability applies to every hook macro in this project):
    dataset_name: the ledger partition key, e.g. 'customers'. Deliberately
      the plain dataset name, NOT a resolved dataset_id -- no meta.datasets
      read, so D-08's dbt_app boundary is untouched (grants live on
      meta.dbt_processed_runs alone, migration 0040).
    source_schema: the bronze source's schema (e.g. 'staging').
    source_identifier: the bronze source's table name (e.g. 'customers').
#}
{% macro claim_dbt_processed_runs(dataset_name, source_schema, source_identifier) %}
insert into meta.dbt_processed_runs (dataset_name, run_id)
select distinct '{{ dataset_name }}', b._run_id
from {{ source_schema }}.{{ source_identifier }} b
where b._run_id is not null
  and not exists (
      select 1
      from meta.dbt_processed_runs p
      where p.dataset_name = '{{ dataset_name }}'
        and p.run_id = b._run_id
  )
on conflict (dataset_name, run_id) do nothing
{% endmacro %}
