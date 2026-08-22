# Schema changed — drift detected and classified, never silently adapted to

Documents an already-built feature (SCHEMA-04, SCHEMA-05, SCHEMA-06), not an incident
reconstruction. See `REQUIREMENTS.md`'s `SCHEMA-04`/`SCHEMA-05` entries for the exact requirement
text.

## Symptoms

A file with an added, removed, renamed, reordered, or retyped column compared to the dataset's
last-known schema fails to load, or an operator receives a schema-drift alert against a dataset
that previously ran cleanly.

## Diagnosis

```sql
SELECT schema_version_id, version, schema_hash, compatibility, breaking_changes, valid_from, valid_to
  FROM meta.schema_versions
 WHERE dataset_id = <dataset_id>
 ORDER BY version DESC;
```

`classify_schema_change` (`packages/dataplat/src/dataplat/schema/evolution.py`) classifies every
observed change as `COMPATIBLE` or `BREAKING` per the dataset's configurable policy. Column
**reordering** is deliberately classified `BREAKING` by default (freeze), the same treatment as a
retype — not a lesser one — because `StagingLoader` maps columns by **position**, not name; a
silently-accepted reorder would stage values into the wrong columns without any structural error to
catch it. A `BREAKING` classification raises before any row stages — read `breaking_changes` for
the exact diagnosable detail (T-06-31's mitigation).

## Recovery

Drift is never silently adapted to (SCHEMA-05) — a human decision is required. Either:

- Update the dataset's YAML contract to acknowledge and accept the new schema shape (this becomes
  the new current version on the next successful sync), or
- Fix the source system if the change was unintended and the file should have kept its prior shape.

## Reprocessing

Once the contract is updated and a new `meta.schema_versions` row exists, resume normal ingestion.
SCHEMA-06 independently guarantees this doesn't disturb older files: any file whose re-derived
structure hash matches a **historical** `meta.schema_versions` row resolves to that historical
version via `SchemaRepository.resolve_by_hash`, rather than being forced through the dataset's
newest schema — so files that haven't changed shape keep working unaffected by a drift found on a
different file.

## Verification

1. `meta.schema_versions` shows the new version with the correct `compatibility` classification and
   populated `breaking_changes` detail for a `BREAKING` change.
2. The previously-failing file now stages and publishes successfully.
3. A pre-existing file matching an older schema shape (if one exists in the corpus) still resolves
   to its own historical version, not the new one — confirming SCHEMA-06 held.
