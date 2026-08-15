# Deferred Items — Phase 6

Out-of-scope discoveries found during plan execution. Not auto-fixed (SCOPE BOUNDARY:
only fix issues directly caused by the current task's changes). Logged here per the
executor's deviation rules.

## From plan 06-02

Found while verifying (via `mypy`, diffed against the pre-06-02 baseline at commit
`47b0f35`) that adding `columns:` as a required `DatasetConfig` field introduced no new
mypy errors in the files whose fixtures needed updating. All items below pre-date 06-02
and are unrelated to `columns:`/`DatasetConfig`.

| File | Line (current) | Issue |
|------|-----------------|-------|
| `tests/unit/test_config_hashing.py` | 23 | `import yaml` has no stubs (`import-untyped`) — no local `# type: ignore[import-untyped]` suppression, unlike `dataplat/config/loader.py`/`tools/corpus/manifest.py`'s identical import. |
| `tests/unit/test_discovery.py` | 235, 274, 287, 317, 332, 354, 383, 419, 443 | `_FakeMetadataRepository` does not fully satisfy the `MetadataRepository` Protocol per mypy structural typing (`discover_files(metadata=...)` call sites). Tests pass at runtime (duck typing); mypy flags the structural mismatch. |
| `tests/integration/test_staging_loader.py` | 155-158 | `PipelineContext(metadata=None, objects=None, db=None, log=None, ...)` — the `# type: ignore` comments guarding these `None` placeholders use an invalid/malformed ignore-code syntax mypy rejects, so the underlying `arg-type` errors show through. |
| `tests/integration/test_run_ingest.py` | 473, 545 | `dataplat.pipeline.run` does not explicitly export `StagingLoader`/`resolve_publisher` in `__all__`, so importing them via `dataplat.pipeline.run.X` (module-qualified) trips `attr-defined` under strict mypy. |

None of these affect runtime behavior (all four test files pass under `pytest`); they are
static-analysis gaps a future cleanup pass should close.
