# Corrupted file — rejected before any pod launches

Documents an already-built feature (LOAD-10, LOAD-11), not an incident reconstruction. See
`REQUIREMENTS.md`'s `LOAD-10` entry for the exact requirement text.

## Symptoms

A file fails before any processing pod ever launches — wrong extension, an empty object, or an
object whose bytes could not be read/hashed. No `discover`/`ingest` task pod appears for it at all.

## Diagnosis

`integrity_gate` (`airflow/dags/_common/integrity_gate.py`) runs every LOAD-10 check resolvable
**before** a `KubernetesPodOperator` pod exists, as plain Airflow `@task` functions in the
scheduler/worker process:

- Extension matches the dataset's expected pattern.
- The object is non-empty.
- Object stability — two `HEAD` calls five seconds apart return the same metadata; an object that
  changes between them is still being written and is rejected rather than raced against.
- A real `content_sha256`, computed from the object's actual bytes (there is no external checksum
  file to compare against by default — "checksum" here means proving the object is genuinely
  readable, not verifying against a claimed value).

A file this gate rejects still gets its own `meta.files` row — nothing is silently skipped. An
empty file gets the real SHA-256 of `b""`; every other rejection reason gets a deterministic
`INTEGRITY_GATE_REJECTED:<object_uri>:<reason>` sentinel hash instead, since the real bytes are
unknown or ambiguous in those cases:

```sql
SELECT file_id, object_uri, content_sha256, created_at
  FROM meta.files
 WHERE dataset_id = <dataset_id> AND content_sha256 LIKE 'INTEGRITY_GATE_REJECTED:%';
```

If the dataset uses an optional `_BATCH_COMPLETE` control-file (LOAD-11), also check whether its
claimed `expected_row_count`/`expected_checksum` actually matches what was loaded — the manifest is
a claim to verify, never trusted as ground truth on its own.

## Recovery

Re-upload a genuinely complete, correctly-extensioned file. There is no in-place repair path for a
corrupted object — the gate's whole purpose is to keep a partially-written or malformed object out
of the pipeline entirely, so the fix always originates at the source.

## Reprocessing

A rejected file never reaches `discover_files`, so there is no staged or published data from it to
correct or roll back. Simply re-upload the corrected file to the same prefix and let the dataset's
normal discovery cadence pick it up — no special re-drive procedure applies.

## Verification

1. `meta.files` shows the corrected file with a real, non-`INTEGRITY_GATE_REJECTED` `content_sha256`.
2. The file proceeds past `integrity_gate` into `discover`/`ingest` without a `FailedScheduling`- or
   rejection-related gap in the task history.
3. If a `_BATCH_COMPLETE` manifest is in use, its claimed counts match what actually loaded.
