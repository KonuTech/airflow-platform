{#
  reconciliation_post_hook.sql -- the atomic, same-transaction
  meta.reconciliation_results write for the bronze->silver hop (D-21, D-26).

  Called from a silver model's own `post_hook` config, appended to the SAME
  captured `post_hook_sql` string that already calls `dedup_audit_post_hook`
  -- a Postgres `post_hook` runs inside the SAME transaction as the model's
  own DML by default (08.1-RESEARCH.md Architecture Pattern 2), so this
  macro's INSERT commits or rolls back together with the model's own write
  AND with `dedup_audit_post_hook`'s write.

  Templated byte-for-byte on `dedup_audit_post_hook.sql`'s own proven
  structure. Its three hard-won, empirically-verified lessons ALL apply
  identically here (see that macro's own docstring for the full empirical
  detail of each):

  1. `dataset_name` is accepted as a plain string, resolved to `dataset_id`
     via `meta.dataset_id_for_name(text)` (a `SECURITY DEFINER` function,
     migration 0028) INSIDE this macro's own INSERT, never a direct
     `SELECT` against `meta.datasets` (fails `dbt_app`'s least-privilege
     grant test, D-08's explicit boundary).

  2. `source_schema`/`source_identifier`/`target_schema`/`target_identifier`
     are accepted as plain STRINGS, never `{{ source(...) }}`/`{{ this }}`
     Relation objects passed as macro arguments -- verified unreliable
     across `post_hook`'s two-pass Jinja evaluation (a `post_hook` config
     value is built in an EARLIER pass than the model's own "real" compile
     pass).

  3. Any prior-state value (here: the floor `_run_id` below which a file
     has already been reconciled) is derived from an audit table's OWN
     history (`coalesce(max(...), 0)`), never from a `run_query()` value
     captured into a `post_hook` config string -- the same timing hazard
     `dedup_audit_post_hook.sql`'s own `watermark_floor` argument was
     removed to avoid. This macro reuses `meta.dedup_audit`'s EXISTING
     per-model floor precedent (`max(max_run_id) where model_name = ...`)
     rather than inventing a second, independent floor mechanism for this
     hop.

     One refinement, found empirically while developing this macro against
     a real, multi-statement `dbt build`: `dedup_audit_post_hook`'s own
     INSERT runs FIRST in the same captured `post_hook_sql` string, in the
     SAME transaction, and Postgres read-own-writes semantics mean this
     macro's own `prior_watermark` query, run as a SEPARATE statement
     afterwards, already sees that row -- unlike `dedup_audit_post_hook`'s
     OWN `prior_watermark` CTE, which is part of the SAME single INSERT
     statement as its own write and therefore reads a snapshot from BEFORE
     it. Left unguarded, the floor this macro computes on a build's FIRST
     ever invocation would equal that SAME build's own just-inserted
     `max_run_id` -- excluding every row the build itself just processed,
     writing zero reconciliation rows on every build, including the first.

     The first fix attempted here was excluding the current row via
     `dbt_invocation_id != '{{ invocation_id }}'` -- rejected after a
     SECOND empirical finding: dbt's partial-parsing cache (`target/
     partial_parse.msgpack`) can skip RE-RENDERING a model's `config()`
     block entirely across separate `dbt build` invocations when it
     detects "nothing changed" in the project's source files, silently
     reusing a PREVIOUS invocation's already-rendered `post_hook_sql`
     string -- including a STALE, frozen `{{ invocation_id }}` literal
     baked in at the earlier compile. A `dbt_invocation_id !=` filter
     keyed on that frozen literal then wrongly excludes the WRONG row
     (or none at all, if the frozen id belongs to a build several runs
     back), duplicating reconciliation rows on every "nothing changed"
     rerun -- reproduced live via two consecutive `dbt build` calls
     against the SAME `target/` directory. The actual fix instead excludes
     the current build's own row by IDENTITY COLUMN, not by a
     Jinja-rendered string: `dedup_audit_id < (select max(dedup_audit_id)
     from meta.dedup_audit where model_name = ...)`. Since
     `dedup_audit_post_hook`'s insert always runs strictly BEFORE this
     macro's SELECT within the same transaction, the current build's own
     row is always the highest `dedup_audit_id` for this `model_name` at
     the moment this query runs -- true regardless of partial-parsing
     caching, invocation-id staleness, or any other Jinja-rendering-order
     hazard, since it depends only on real, transaction-local database
     state.

  4. (This macro's own, additional constraint.) D-24 is LOCKED: grain is
     per file, per hop -- one row per `(file_id, hop)`. This macro must NOT
     write a single build-level aggregate row for this hop, which would
     silently collapse D-24's per-file grain to per-build grain here only,
     contradicting migration 0032's own documented invariant that every
     hop populates `file_id`. Instead, `bronze_files` enumerates every
     DISTINCT `_file_id` that contributed rows to THIS build (the same
     `_run_id > floor` scoping the calling model's own SELECT body already
     uses), and the final INSERT's `SELECT ... FROM bronze_count,
     silver_count, dedup_count, bronze_files` cross-joins that per-file set
     against one shared set of build-level aggregate counts -- one output
     row per contributing file, every row in a given build sharing the SAME
     aggregate `input_count`/`output_count`/`dedup_count` values (mirroring
     the silver->gold hop's own documented "aggregate-attribution,
     per-pass not per-file" precedent for why the aggregate VALUES are
     shared while the per-file SPLIT is only in which rows get written). A
     build that processes zero new rows (`bronze_files` empty) writes zero
     reconciliation rows via this same cross join -- never a phantom row
     with a NULL `file_id`.

  D-22's exact accounting formula (migration 0032's own docstring, restated
  here verbatim since this macro is what computes it for this hop):

      discrepancy = input_count - (output_count + dedup_count)

  `rejected_count` is deliberately 0/omitted at this hop -- any row
  rejection already happened upstream, at the raw_bronze hop (plan 09-07);
  nothing is rejected a second time inside a silver model.

  Args:
    dataset_name: the dataset's `meta.datasets.dataset_name` (e.g.
      'customers') -- resolved to its surrogate `dataset_id` by
      `meta.dataset_id_for_name(text)` inside the INSERT below.
    source_schema: the bronze source's schema (e.g. 'staging').
    source_identifier: the bronze source's table name (e.g. 'customers').
    target_schema: the schema the calling model's own table lives in (e.g.
      `target.schema`, always 'silver').
    target_identifier: the calling model's own configured `alias` (e.g.
      'customers') -- also used as `meta.dedup_audit.model_name`, keeping
      this macro's self-derived floor scoped to the correct model.
#}
{% macro reconciliation_post_hook(dataset_name, source_schema, source_identifier, target_schema, target_identifier) %}
with prior_watermark as (
    select coalesce(max(max_run_id), 0) as floor
    from meta.dedup_audit
    where model_name = '{{ target_identifier }}'
      and dedup_audit_id < (
          select max(dedup_audit_id)
          from meta.dedup_audit
          where model_name = '{{ target_identifier }}'
      )
),

bronze_files as (
    select distinct b._file_id as file_id
    from {{ source_schema }}.{{ source_identifier }} b
    cross join prior_watermark
    where b._run_id > prior_watermark.floor
),

bronze_count as (
    select count(*) as input_count
    from {{ source_schema }}.{{ source_identifier }}
),

silver_count as (
    select count(*) as output_count
    from {{ target_schema }}.{{ target_identifier }}
),

dedup_count as (
    select coalesce(sum(records_deduplicated), 0) as dedup_count
    from meta.dedup_audit
    where model_name = '{{ target_identifier }}'
)

insert into meta.reconciliation_results (
    dataset_id, file_id, hop, input_count, output_count, dedup_count, discrepancy, checked_at
)
select
    meta.dataset_id_for_name('{{ dataset_name }}'),
    bronze_files.file_id,
    'bronze_silver',
    bronze_count.input_count,
    silver_count.output_count,
    dedup_count.dedup_count,
    bronze_count.input_count - (silver_count.output_count + dedup_count.dedup_count),
    now()
from bronze_count, silver_count, dedup_count, bronze_files
{% endmacro %}
