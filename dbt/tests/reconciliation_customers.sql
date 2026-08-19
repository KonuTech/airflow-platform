{#
  reconciliation_customers.sql -- D-26's "second, complementary signal":
  a native, non-blocking dbt test (`config(severity='warn')`, set HERE
  rather than referenced from `silver_customers.yml`'s `tests:` block --
  dbt's model-level `tests:`/`data_tests:` YAML keys apply only to
  GENERIC, parametrized tests referenced by macro name; a standalone
  SINGULAR test file like this one is auto-discovered from `tests/`
  (`dbt_project.yml` declares no `test-paths` override, so the default
  `["tests"]`, relative to `--project-dir`, applies) and configures its
  own severity via `{{ config(...) }}` at the top of the file, the
  dbt-idiomatic mechanism for exactly this case).

  Asserts D-22's exact accounting formula (migration 0032's own docstring,
  restated verbatim in `reconciliation_post_hook.sql`'s own header comment)
  holds for the MOST RECENT `bronze_silver` reconciliation row this dataset
  has -- `input_count = output_count + dedup_count`, i.e. `discrepancy = 0`.

  Since a single `dbt build` writes ONE row per contributing `file_id`
  (D-24's grain) but every row in that same build shares the SAME
  aggregate `input_count`/`output_count`/`dedup_count` values
  (`reconciliation_post_hook.sql`'s own documented aggregate-attribution
  precedent), picking any ONE row from the most recent build (highest
  `reconciliation_id`) is sufficient to check the aggregate invariant --
  this test is deliberately NOT per-file, it is a build-level
  cross-check, complementary to (not a replacement for) the durable
  per-file macro row Task 1 already writes.

  A singular test's contract: return rows only on FAILURE. An empty
  result set means the invariant holds. `severity: warn` (D-26) means a
  violation is surfaced in `dbt build`'s own JSON run-results output as a
  `warn` outcome, never `error` -- it can never block the build or the
  model's own already-committed write.

  This test's SQL never literally calls `{{ ref('silver_customers') }}`
  (it reads `meta.reconciliation_results`/`meta.datasets` directly, not
  `silver.customers`), so without an explicit dependency edge dbt's node
  selection graph would not know this test depends on that model --
  `dbt build --select silver_customers` would silently skip it. The
  `-- depends_on:` comment below is dbt's own documented mechanism for
  exactly this case: an explicit DAG edge with no functional effect on
  the compiled SQL, ensuring this test both runs whenever
  `silver_customers` is selected and always executes AFTER it.
#}
-- depends_on: {{ ref('silver_customers') }}
{{ config(severity = 'warn') }}

with latest as (
    select rr.*
    from meta.reconciliation_results rr
    join meta.datasets d on d.dataset_id = rr.dataset_id
    where d.dataset_name = 'customers'
      and rr.hop = 'bronze_silver'
    order by rr.checked_at desc, rr.reconciliation_id desc
    limit 1
)

select *
from latest
where input_count != (output_count + dedup_count)
