---
phase: 03-dataplat-core-library-metadata-control-plane
fixed_at: 2026-08-13T08:31:01Z
review_path: .planning/phases/03-dataplat-core-library-metadata-control-plane/03-REVIEW.md
iteration: 1
findings_in_scope: 8
fixed: 8
skipped: 0
status: all_fixed
---

# Phase 3: Code Review Fix Report

**Fixed at:** 2026-08-13T08:31:01Z
**Source review:** .planning/phases/03-dataplat-core-library-metadata-control-plane/03-REVIEW.md
**Iteration:** 1

**Summary:**
- Findings in scope: 8 (3 critical, 5 warning — `fix_scope: critical_warning`, so the 2 Info findings were not attempted)
- Fixed: 8
- Skipped: 0

All fixes were applied in an isolated git worktree (`/tmp/sv-03-reviewfix-*`) on a
temporary branch, verified individually (re-read + `ast.parse` + `ruff check` for
every file, plus live behavioral execution against the actual worktree source for
every finding), committed one finding per commit, then fast-forwarded onto `main`.

Two findings (CR-02, CR-03) are marked `fixed: requires human verification` per the
logic/product-decision caveat below, even though both were verified with live
execution (not just static analysis) — see their entries for exactly what was run.

## Fixed Issues

### CR-01: `dataplat` CLI entrypoint crashes with a raw traceback on any usage error

**Files modified:** `packages/dataplat/src/dataplat/cli.py`, `tests/unit/test_cli_error_handling.py`
**Commit:** `c8f165a`
**Status:** fixed

**Applied fix:** Added `except click.exceptions.Exit`, `except click.exceptions.ClickException`
(covers `NoArgsIsHelpError`, `NoSuchOption`, `NoSuchCommand`, etc. — all `UsageError`
subclasses), and `except click.exceptions.Abort` alongside the existing
`except DataPlatformError` in `main()`, converting each to the same exit code
`standalone_mode=True` would have produced, exactly as REVIEW.md's suggested patch
showed. Updated both the module and `main()` docstrings, which previously claimed
"any OTHER exception ... propagates" — no longer accurate once usage errors are
handled. Rewrote `test_zero_arguments_does_not_crash` to call `main([])` directly
(capturing real stdout/stderr via `capsys`) instead of only `CliRunner.invoke()`,
per REVIEW.md's explicit instruction, and added `test_unknown_option_does_not_crash`
and `test_unknown_command_does_not_crash` covering the other two reproduction cases
from the finding. Kept the original `CliRunner`-based assertion too (renamed
`test_zero_arguments_via_cli_runner_still_exits_two`), since it still proves a
different thing (the `cli` group's own behavior, not `main()`'s error boundary).

**Verification beyond Tier 2:** Reproduced the exact pre-fix crash for
`main([])`, `main(["--bogus-option"])`, and `main(["no-such-command"])` against the
worktree source before fixing (all raised raw `click.exceptions.*`), then confirmed
all three return exit code 2 with no traceback after the fix, `main(["--version"])`
still returns 0, and all 8 tests in the file pass.

---

### CR-02: `chunked_records()` crashes with an opaque `RuntimeError` on an empty CSV file

**Files modified:** `packages/csv-processor/src/csv_processor/source.py`, `tests/unit/test_csv_chunking.py`
**Commit:** `46c20db`
**Status:** fixed: requires human verification (product-decision caveat, see below)

**Applied fix:** Wrapped `header = next(reader)` in `try/except StopIteration: return`,
matching REVIEW.md's primary suggested code exactly — a genuinely empty (zero-byte)
stream now yields zero chunks instead of letting PEP 479 turn the uncaught
`StopIteration` into `RuntimeError: generator raised StopIteration`. Updated the
function's docstring to document the empty-input behavior explicitly. Added
`test_empty_stream_yields_no_chunks_and_does_not_raise` to `tests/unit/test_csv_chunking.py`.

**Why the human-verification flag:** REVIEW.md explicitly declined to choose between
"yield zero chunks" and "raise a typed `DataPlatformError` subclass" ("a product
decision this review does not make"). I chose "yield zero chunks" because (a) it's
the literal primary code REVIEW.md supplied (raising was offered only as a code
comment alternative), and (b) inventing a new `DataPlatformError` subclass here would
contradict `errors.py`'s own documented convention that new subclasses are "added by
the phase that first raises it" — a decision `CONTEXT.md D-06` reserves for
phase-level design, not a review-fix pass. A human should confirm "zero chunks" is
the desired product behavior for an empty file before this is considered final.

**Verification beyond Tier 2:** Reproduced the exact pre-fix `RuntimeError` against
the worktree source before fixing, then confirmed `list(chunked_records(empty_stream,
chunk_size=10)) == []` after the fix, with all 8 existing + new unit tests and the
property test passing.

---

### CR-03: Unguarded TOCTOU race in "get or create dataset" (two call sites)

**Files modified:** `packages/dataplat/src/dataplat/metadata/postgres.py`, `packages/dataplat/src/dataplat/config/registry.py`
**Commit:** `20d101c`
**Status:** fixed: requires human verification (criticality caveat, see below)

**Applied fix:** Replaced the plain `SELECT` (or `SELECT ... FOR UPDATE`) followed by
a plain `INSERT` in both `PostgresMetadataRepository.get_or_create_dataset` and
`ConfigRegistry._resolve_dataset_id` with a single atomic
`INSERT ... ON CONFLICT (dataset_name) DO UPDATE SET dataset_name = EXCLUDED.dataset_name
RETURNING dataset_id`, exactly as REVIEW.md's suggested patch showed. Verified this
does not introduce an `updated_at`-bumping side effect (no update trigger exists on
`meta.datasets` — checked the migration DDL directly). Updated docstrings on both
methods and the `registry.py` module docstring, which previously described the
`SELECT ... FOR UPDATE` serialization mechanism that no longer exists in that form.

**Why the human-verification flag:** This is a critical-severity concurrency fix
touching production database write paths. Even with strong empirical verification
(below), concurrent-database-semantics fixes are exactly the class of change where a
second human pass is warranted before considering it fully closed.

**Verification beyond Tier 2 (this is the important part):** `tests/integration/`'s
own `_require_docker` preflight (`docker info`) hangs indefinitely in this sandbox
even though `docker run`/the Docker API work fine, so the repo's own integration
tests could not be run directly. Instead I wrote a standalone script
(bypassing only that preflight, using the same `testcontainers` PostgreSQL-18
container the repo's own fixtures use) that:
1. Ran `alembic upgrade head` against a real, throwaway PostgreSQL 18 container.
2. Proved sequential idempotency still holds: `get_or_create_dataset("x")` called
   twice returns the same `dataset_id` (matches `test_full_slice_round_trip`'s own
   assertion).
3. Reproduced REVIEW.md's exact concurrency methodology: two threads racing
   `get_or_create_dataset()` on the same brand-new dataset name via a
   `threading.Barrier`. **Pre-fix this raised `psycopg.errors.UniqueViolation` in the
   loser thread (per REVIEW.md's own repro); post-fix, both threads returned the
   same `dataset_id` with zero exceptions.**
4. Did the same for two concurrent first-time `ConfigRegistry.sync()` calls on a
   brand-new dataset name: zero exceptions, and the two calls correctly serialized
   into exactly one `is_new=True` insert and one `is_new=False` no-op resolving to
   the same row/version — proving the row lock taken by the upsert's `UPDATE` half
   still serializes concurrent `sync()` calls the way the module's docstring
   describes.

---

### WR-01: `S3ObjectStore.get_object()` only catches `ClientError`

**Files modified:** `packages/dataplat/src/dataplat/storage/objectstore.py`
**Commit:** `4d22227`
**Status:** fixed

**Applied fix:** Widened the `except ClientError` to `except (ClientError, BotoCoreError)`,
exactly as REVIEW.md's suggested patch showed, after confirming directly against the
installed botocore that the two exception hierarchies are disjoint (`ClientError` and
`BotoCoreError` are both direct `Exception` subclasses; `EndpointConnectionError`,
`ConnectTimeoutError` etc. are `BotoCoreError` subclasses, not `ClientError`
subclasses). Updated the docstring's `Raises:` section accordingly.

**No new test file added:** `tests/integration/test_objectstore.py`'s own module
docstring states `S3ObjectStore` is proven "against a real testcontainers MinIO, not
a mock" — a deliberate house convention. Adding a new mock-based unit test would
contradict that stated convention, and the finding's Fix section didn't ask for a
test. Verified instead with a standalone mock-based script (not committed) confirming
a `botocore.exceptions.EndpointConnectionError` raised from the client is now caught
and re-raised as `dataplat.errors.StorageError` rather than escaping raw.

---

### WR-02: `create_pool()`'s `except psycopg.OperationalError` is dead code

**Files modified:** `packages/dataplat/src/dataplat/storage/db.py`
**Commit:** `4f419b3`
**Status:** fixed

**Applied fix:** Took REVIEW.md's first offered option (remove the misleading
try/except and `Raises:` docstring claim) rather than the second (eagerly open the
pool to manufacture a failure mode) — eager-opening would defeat the function's own
explicitly documented and tested lazy-pool design (`dataplat --version` must never
pay a connection cost), which is a deliberate architectural property, not something
a fix pass should silently reverse. Removed the dead `try/except`, and the now-unused
`psycopg`/`urlsplit`/`StorageError` imports (confirmed via `ruff` that nothing else
in the module needed them). Rewrote the docstring to state plainly that construction
has no failure mode to report, and that any DSN/connectivity failure surfaces at the
later `.open()`/first-use call site instead.

**Verification beyond Tier 2:** Confirmed no test anywhere in the repo depends on
`create_pool()` itself raising `StorageError`. Re-ran a direct construction smoke
test across five DSN shapes (`""`, `"not a dsn at all"`, `"postgresql://"`,
`"://bad"`, a well-formed DSN) — all construct successfully both before and after,
confirming the behavioral contract is genuinely unchanged, only the misleading
dead code and docstring claim were removed.

---

### WR-03: `cli.py`'s catch-once handler could crash itself on a future context-key collision

**Files modified:** `packages/dataplat/src/dataplat/errors.py`, `tests/unit/test_errors.py` (new file)
**Commit:** `35f1402`
**Status:** fixed

**Applied fix — deliberately NOT REVIEW.md's first-listed option:** REVIEW.md offered
two options: (a) nest `exc.context` under one key (`context=exc.context` instead of
`**exc.context`), or (b) reserve `error_type`/`error_message` as disallowed context
keys, validated in `DataPlatformError.__init__`. **I implemented (b), not (a)**,
because (a) would silently defeat `dataplat.observability.logging`'s OBS-05 redaction
processor: `_redact()` only scans an event dict's *top-level* keys for secret
patterns (verified by reading it directly, and confirmed by
`tests/unit/test_logging_redaction.py`'s own assertions, which all pass secrets as
top-level kwargs). Nesting `context` under one key means a future `context={"dsn":
...}` would no longer be redacted — trading WR-03's narrow key-collision bug for a
much worse, silent secret-leak regression. Option (b) keeps `context` spread as
top-level keys (preserving redaction) while rejecting the two reserved key names at
`DataPlatformError.__init__` — i.e. at the raise site, with an immediate, actionable
`ValueError`, rather than three call frames away inside the CLI's own error boundary.
Confirmed no existing raise site in the codebase uses either reserved key. Created
`tests/unit/test_errors.py` (no existing test file covered `errors.py`) covering the
new guard, that it applies to every subclass, that normal context still works, and
an end-to-end reproduction of `cli.py`'s exact `log.error(..., **exc.context)` call
shape.

**Verification beyond Tier 2:** Ran the full `tests/unit/` suite (102 tests, all
passing) specifically to confirm the stricter `__init__` didn't regress any existing
`DataPlatformError` construction site, and specifically re-ran
`test_logging_redaction.py` to confirm redaction still works.

---

### WR-04: `RaggedRowGuard` reconstructs `RejectedRecord.raw_line` via a hardcoded `","`

**Files modified:** `packages/dataplat/src/dataplat/pipeline/engine.py`, `packages/dataplat/src/dataplat/models/record.py`, `tests/unit/test_pipeline_errors.py`
**Commit:** `9433063`
**Status:** fixed

**Applied fix — adapted from REVIEW.md's literal suggestion:** REVIEW.md's "at
minimum" suggestion was to join using `DIALECT.delimiter` (from
`csv_processor.source`). **This is not importable from `engine.py`**:
`dataplat.pipeline.engine` lives in the `dataplat` package, and `setup.cfg`'s
import-linter contract forbids `dataplat` from ever importing `csv_processor` (the
CSV-specific plugin) — confirmed by running `lint-imports` directly, and by
`dataplat`'s own source-agnostic design (a hardcoded "delimiter" concept doesn't even
make sense for a hypothetical non-CSV `Source`). Instead, I added a
`field_delimiter: str = ","` constructor parameter to `RaggedRowGuard` (default
reproduces today's exact behavior byte-for-byte; a future CSV-aware caller, e.g.
Phase 6's delimiter detection, can pass the real detected delimiter in without
`dataplat` ever importing `csv_processor`). Also did **not** rename
`RejectedRecord.raw_line` (REVIEW.md's other offered option) — `ARCHITECTURE.md`
already documents a future `meta.rejected_records.raw_line` database column using
this exact name; renaming the in-memory field now would create a cross-phase naming
inconsistency that's a bigger call than a review-fix pass should make unilaterally.
Instead, rewrote `RejectedRecord.raw_line`'s docstring and `RaggedRowGuard.apply()`'s
docstring to state plainly that this is a best-effort reconstruction from
already-parsed fields, exact only when no field contains the delimiter/a
newline/a quote character, addressing the "misleading for audit purposes" concern
without an architecture change or a cross-phase-inconsistent rename.

**Verification beyond Tier 2:** Ran `lint-imports` directly against the worktree
(0 broken contracts, "dataplat core must not depend on the CSV plugin KEPT") to
formally confirm no forbidden import was introduced. Added
`test_ragged_row_guard_raw_line_uses_the_configured_field_delimiter`, proving both
the default (`","`, byte-identical to pre-fix output) and a non-default (`"|"`) case.
All 6 tests in the file pass.

---

### WR-05: `_strip_nul()` silently discards NUL bytes with zero observability

**Files modified:** `packages/csv-processor/src/csv_processor/source.py`, `tests/unit/test_csv_chunking.py`
**Commit:** `b180c8e`
**Status:** fixed

**Applied fix — minor naming adaptation:** Added `metrics.increment(...)` inside
`_strip_nul()`, incrementing once per physical line that actually contained a NUL
byte, mirroring the `rows_rejected`/`rows_kept` pattern `RaggedRowGuard` already
establishes, per REVIEW.md's suggestion. **Named the metric
`lines_with_nul_stripped`, not REVIEW.md's literal suggested
`rows_with_nul_stripped`**: `_strip_nul()` genuinely operates one physical line at a
time (including continuation lines inside an open multiline quoted field), not one
parsed CSV record at a time, so "rows" would be a subtly inaccurate unit for this
specific metric. First fix attempt reassigned the `for` loop variable
(`line = line.replace(...)`), which `ruff`'s `PLW2901` rule correctly flagged;
rewrote as an `if/else` with two `yield` sites instead. Updated the function's
docstring to document the new metric and the "lines not rows" naming rationale.

**Verification beyond Tier 2:** Added
`test_nul_stripping_increments_a_metric_once_per_affected_line` (exactly one
increment for the one NUL-containing physical line in the existing `_NUL_FIXTURE`)
and `test_nul_free_stream_never_increments_the_metric` (zero increments on a
NUL-free file), both passing, plus re-ran the full existing chunking + property test
suite (10 tests) to confirm no regression.

## Skipped Issues

None — all 8 in-scope findings were fixed.

---

_Fixed: 2026-08-13T08:31:01Z_
_Fixer: Claude (gsd-code-fixer)_
_Iteration: 1_
