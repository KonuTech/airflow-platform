# Duplicate batch — content-addressed identity makes re-submission a no-op

Documents an already-built feature (LOAD-08), not an incident reconstruction. See
`REQUIREMENTS.md`'s `LOAD-08` entry for the exact requirement text.

## Symptoms

An operator worries the same logical batch was loaded twice — for example, the same file was
re-uploaded (possibly under a different filename) and they want confirmation it did not produce
duplicate rows.

## Diagnosis

```sql
SELECT batch_id, dataset_id, batch_key, created_at
  FROM meta.batches
 WHERE dataset_id = <dataset_id> AND batch_key = '<batch_key>';

SELECT file_id, object_uri, content_sha256, duplicate_of_file_id
  FROM meta.files
 WHERE dataset_id = <dataset_id> AND content_sha256 = '<sha256>';
```

`meta.batches` carries `UNIQUE (dataset_id, batch_key)` (migration `0003`) — one row per batch
identity, structurally. `batch_key` is a pure function of file **content**
(`content_sha256`-derived), not filename, so re-uploading byte-identical content under a new
filename resolves to the SAME `batch_key` and is a no-op (LOAD-03). `meta.files.duplicate_of_file_id`
records the lineage when this happens.

## Recovery

No action is needed if `batch_key` genuinely collided — the second submission was correctly
rejected or absorbed as a no-op, exactly as designed. If what actually happened is a **new** file
with **different** content intended as a correction (not a true duplicate), that is not this
scenario — see [`csv-malformed.md`](csv-malformed.md) or [`failed-backfill.md`](failed-backfill.md)
for the correction path instead, since a content-differing file always gets its own new
`batch_key`.

## Reprocessing

None required. Re-running an already-loaded batch is idempotent by construction (LOAD-01/LOAD-02/
LOAD-03) — proven live at 10-million-row scale with zero additional rows produced on rerun,
including under deliberate pod-kills and concurrent-reads-during-publish.

## Verification

1. `SELECT count(*) FROM meta.batches WHERE dataset_id = <id> AND batch_key = '<key>'` returns
   exactly `1`.
2. Row counts in the target table match the expectation for a single batch, not two.
3. Re-running the same file (or a byte-identical re-upload) produces zero additional rows.
