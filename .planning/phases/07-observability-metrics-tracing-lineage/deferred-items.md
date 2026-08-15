# Deferred Items — Phase 7

Out-of-scope discoveries found during plan execution. Not auto-fixed (SCOPE BOUNDARY:
only fix issues directly caused by the current task's changes). Logged here per the
executor's deviation rules.

## From plan 07-02

Found while running `mypy` against `tests/unit/test_pipeline_errors.py` as a diligence
check beyond this plan's own declared acceptance criteria (Task 1 only requires
`mypy packages/dataplat/src/dataplat/observability`; Task 2 has no mypy criterion at
all — `make typecheck`'s `TYPECHECK_PATHS` is `packages/dataplat/src
packages/csv-processor/src tools`, which never includes `tests/`).

| File | Line (current) | Issue |
|------|-----------------|-------|
| `tests/unit/test_pipeline_errors.py` | 44-48 | `PipelineContext(config=..., metadata=None, objects=None, db=None, log=None)` — the `# type: ignore[arg-type]` comments guarding these placeholders use a malformed-per-mypy ignore-code syntax, so the underlying `arg-type` errors show through instead of being suppressed. |
| `tests/unit/test_logging_config.py` | 101-103 | `metrics.increment(...)` is annotated `-> None`; asserting `metrics.increment(...) is None` trips mypy's `func-returns-value` check. Pre-existing — the signature was already `-> None` before this plan's Task 1 rewrote the function body, confirmed identical via `git stash`. |

Verified pre-existing, not introduced by this plan: `git stash` back to the pre-07-02
state and re-running `mypy tests/unit/test_pipeline_errors.py` reproduces the exact same
12 errors (same messages, only line numbers shift, since this plan's docstring additions
push the block down 6 lines). This is the identical bug class already logged in
`.planning/phases/06-universal-csv-engine-schema-contracts-normalization/deferred-items.md`
(plan 06-02: `tests/integration/test_staging_loader.py` lines 155-158, same
malformed-ignore-comment pattern) — a repo-wide, still-unfixed gap in how `# type:
ignore[code] -- trailing comment` is written across several `tests/` files, not something
plan 07-02 introduced.

Not auto-fixed: neither file is in plan 07-02's `files_modified` list for this reason
(`tests/unit/test_pipeline_errors.py` was touched only to fix a real regression --
`RaggedRowGuard.apply()` now reads `ctx.config.dataset` for D-04's metric labels, so
`_make_context()`'s `config=None` placeholder had to become a `SimpleNamespace` with a
real `.dataset` attribute; that fix is a Rule 1 auto-fix, verified live via
`pytest tests/unit -k pipeline -q`, 6/6 passing). Both files' tests pass at runtime under
`pytest` regardless of the mypy gap (`pytest tests/unit tests/regression -q --no-cov`:
388/388 passing). A future cleanup pass should fix the `# type: ignore` comment syntax
across every affected file at once, matching 06-02's own recommendation.
