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

## From plan 06-16

Found while running `pytest tests/unit -q` as this plan's own required regression
verification. Unrelated to any file this plan modifies
(`packages/dataplat/src/dataplat/load/staging.py`,
`packages/dataplat/src/dataplat/discovery.py`,
`packages/csv-processor/src/csv_processor/cli.py`,
`tests/unit/test_discovery.py`, `tests/integration/test_discover_files.py`,
`tests/integration/test_staging_normalization.py`) — confirmed via `git status`/`git log`
that `compression.py`/`test_compression.py` were last touched by plan 06-08, merged into
this worktree's Wave-2 base, before this plan's session began.

| File | Test | Issue |
|------|------|-------|
| `tests/unit/test_compression.py` | `test_bomb_guard_property_never_exceeds_ceiling_by_more_than_one_bounded_chunk` | Hypothesis-discovered boundary case `decompressed_size=1_000_001` (`ceiling=1_000_000`, one byte over): `open_compressed_stream`'s bomb guard trips with `bytes_read_before_trip == decompressed_size` (`1000001 == 1000001`) rather than strictly less, failing the property's own `bytes_read_before_trip < decompressed_size` assertion. Off-by-one at the exact `ceiling + 1` boundary in `open_compressed_stream`'s bounded-read loop (plan 06-08), not something plan 06-16 touches or introduces. |

Verified isolated: `pytest tests/unit -q` (no `-x`) shows exactly this one failure out of
367 collected tests; every other test, including all of this plan's own new/modified
tests, passes. A future cleanup pass (or plan 06-08's own follow-up) should tighten the
bomb guard's boundary condition.

**Resolved (orchestrator, post-wave-3 merge gate):** Re-diagnosed — this was the
property test's own second assertion being over-strict, not a guard logic bug.
`open_compressed_stream`'s bomb guard already provides and correctly enforces its real,
documented guarantee (`bytes_read_before_trip <= ceiling + _BOUNDED_READ_CHUNK_BYTES`,
already asserted one line above and matching the fixed-payload test's `< 1_000_000`
check). The removed assertion (`bytes_read_before_trip < decompressed_size`,
unconditional) is structurally unprovable whenever `decompressed_size` sits within one
`_BOUNDED_READ_CHUNK_BYTES` chunk of `ceiling`: the guard's final bounded read can
legitimately drain the entire remaining stream while still tripping, since there is no
more stream data left to distinguish "before" from "all" — not a bomb-safety violation.
Fixed by gating that assertion on `decompressed_size > ceiling + _BOUNDED_READ_CHUNK_BYTES`
(genuine bomb-scale payloads only) in `tests/unit/test_compression.py`. Verified against
the exact failing case plus 5 independent Hypothesis seeds, full `make test`/`ruff
check`/`make typecheck` — all clean.
