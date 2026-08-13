# Deferred Items — Phase 04

Issues discovered during execution that are out of scope for the plan that
found them (pre-existing, in files the plan does not touch). Logged per the
executor's scope-boundary rule rather than fixed inline.

## From Plan 04-01

### `tests/policy/test_gates_actually_fail.py` — 2 pre-existing failures, unrelated to 04-01

- **Found during:** Task 3 full-gate verification (`make check` / `uv run --frozen pytest tests/policy -q -m "not manifests"`).
- **Symptom:** `test_forbidden_import_is_rejected` and `test_good_forbidden_import_is_accepted`
  both fail with `AssertionError: the checker failed/passed without
  naming/evaluating the contract`. Both assert a plain substring
  (`f"{CONTRACT_NAME} BROKEN"` / `f"{CONTRACT_NAME} KEPT"`) is present in
  `lint-imports`' captured stdout.
- **Root cause:** The pinned `import-linter==2.13` (`import-linter>=2.13,<3`
  in `pyproject.toml`) now renders its per-contract result line with an
  inline ANSI color escape sequence between the contract name and the
  KEPT/BROKEN word (e.g. `...the plugin \x1b[31mBROKEN\x1b[0m` instead of a
  plain `...the plugin BROKEN`). The plain-substring assertion written
  against an earlier `import-linter` rendering no longer matches, even
  though the tool's actual pass/fail behavior is correct (verified
  independently: `uv run --frozen lint-imports` against the real
  `dataplat`/`csv_processor` contract in `setup.cfg` reports `1 kept, 0
  broken`, exactly as expected, both before and after 04-01's changes).
- **Not caused by 04-01:** `tests/policy/test_gates_actually_fail.py` was
  last modified by Phase 1 commit `edf4756` (`test(01-05): observe every
  gate reject a bad sample and accept a good one`). 04-01 never touches
  this file, `setup.cfg`, or any `lint-imports`/lint-invocation code —
  its own `setup.cfg` Contract 1 (`dataplat core must not depend on the CSV
  plugin`) is independently confirmed `KEPT` throughout 04-01's execution.
- **Verified reproducible on `main` before 04-01's first commit** in spirit
  (the test's own fixture/assertion logic and the installed `import-linter`
  version are both untouched by this plan; the failure is deterministic on
  every invocation, not a flake).
- **Status:** Not fixed. Out of scope for 04-01 (SCOPE BOUNDARY: only
  auto-fix issues directly caused by the current task's changes).
- **Suggested resolution for whoever picks this up:** Either strip ANSI
  codes from `proc.stdout` before the substring assertion (e.g.
  `re.sub(r"\x1b\[[0-9;]*m", "", proc.stdout)`), or invoke `lint-imports`
  with a `--no-color`/`NO_COLOR=1` environment in `_import_contract()`.

## From Plan 04-02

### Pre-existing, unrelated test failures in `tests/policy/test_gates_actually_fail.py`

- **Found during:** Task 1/2 verification (full `tests/policy` run)
- **Tests:** `test_forbidden_import_is_rejected`, `test_good_forbidden_import_is_accepted`
- **Symptom:** Both fail on an `AssertionError` comparing captured `lint-imports`
  CLI output against an expected substring. The actual `lint-imports` output now
  includes a Rich-rendered ANSI-colored banner/progress display (box-drawing
  characters, animated "Checking contracts" progress bar) that the test's plain
  substring assertion does not account for — looks like upstream `import-linter`
  (or its `grimp`/`rich` dependency) started emitting a fancier terminal UI since
  this test was last touched.
- **Confirmed pre-existing and unrelated to 04-02:** `git log -1 -- tests/policy/test_gates_actually_fail.py`
  shows it was last committed in `edf4756` (phase 01, plan 01-05), and this
  plan made zero changes to that file, `pyproject.toml`, or any import-linter
  contract. Reproduces identically on an unmodified tree.
- **Status:** Deferred — not fixed by 04-02 (out of scope: import-linter
  self-test tooling, unrelated to RBAC/secrets/Helm/image-build work).
- **Suggested owner:** whichever future plan next touches CI/lint tooling, or
  a dedicated chore plan. Likely fix: strip ANSI/box-drawing output before
  the substring match (mirroring how other tests in this same phase's own
  `tests/policy/test_no_manual_kubectl_surgery.py` mask quoted spans before
  matching), or pin `import-linter`'s output mode.

**All three plans independently confirm the same underlying issue** (import-linter
output-format drift breaking a plain-substring assertion in a Phase 1 policy
test) — three independent characterizations of the same drift, not separate bugs.
See 04-03's confirmation below.

## From Plan 04-03

`discover_files` calls `metadata.create_batch(...)` unconditionally on every
non-duplicate object, every call — including on re-discovery of an already-`PENDING`
or already-`SUCCEEDED` run. `create_batch` is not idempotent (plain `INSERT ...
RETURNING`, no `ON CONFLICT`), so a batch row is created on every re-discovery,
orphaning the previous batch (only the run's original `batch_id`, set once at first
`INSERT`, stays linked to `meta.ingestion_runs`; later batches get a
`meta.batch_files` row but no `ingestion_runs` reference).

- **Found during:** Task 2 verification.
- **Impact:** Does not affect this plan's own behavior guarantees (file identity,
  dedup, run re-offering/exclusion, and the fan-out cap are all unaffected — proven
  by `tests/unit/test_discovery.py`), but is a real, silently-accumulating metadata
  inefficiency worth fixing before batches carry more meaning (e.g. multi-file
  batches in a later phase).
- **Status:** Deferred (design gap inherited from 04-01-PLAN.md's interface).
- **Suggested fix:** Either an idempotent `create_batch`/`get_or_create_batch`
  (keyed on `batch_key`, mirroring `create_file`/`get_or_create_ingestion_run`'s
  upsert pattern) or reordering `discover_files` to only create a batch on a run's
  first-ever allocation.

04-03 also independently reproduced the `tests/policy/test_gates_actually_fail.py`
import-linter output-format drift documented above (same root cause, same two
tests, confirmed unrelated to this plan's diff).

## Merge note (orchestrator, wave 2)

04-03's worktree forked from a stale pre-wave-1 base (a worktree-provisioning
quirk) and so never saw 04-01's `get_or_create_ingestion_run`, duplicate-aware
`create_file`, `ObjectStore.list_objects`/`put_object`. Per its scope-boundary
rule, 04-03 reimplemented that subset itself from 04-01-PLAN.md's spec verbatim
(with its own integration tests) so Task 2 could proceed. Merging 04-03 back into
main therefore produced content conflicts in `metadata/repository.py`,
`metadata/postgres.py`, `storage/objectstore.py`, and
`tests/integration/test_objectstore.py` against 04-01's already-merged originals.
Resolved by keeping 04-01's original implementations (already covered by 04-01's
own tests) and layering 04-03's additional discovery-specific test coverage on
top where it tested something 04-01's suite didn't.
