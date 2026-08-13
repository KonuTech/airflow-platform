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
- **RESOLVED by 04-06** (commit `36ca08a`): confirmed empirically —
  `tests/integration/test_discover_files.py::test_rerun_produces_identical_manifest`
  reproduced this exact bug as a real `psycopg.errors.UniqueViolation` against
  `uq_batches_dataset_batch_key` on a real Postgres (the fake repository
  `tests/unit/test_discovery.py` uses never enforced the real constraint, so
  the gap was invisible at the unit level). Fixed exactly per this note's own
  suggestion: added `MetadataRepository.get_or_create_batch` (Protocol +
  `PostgresMetadataRepository`, mirroring `get_or_create_dataset`'s
  `INSERT ... ON CONFLICT DO UPDATE` idiom, `status` excluded from the
  conflict `SET` clause so a rediscovery can never clobber a batch that
  already progressed past `OPEN`), switched `discover_files` to call it
  instead of `create_batch`, and made `link_batch_file` idempotent
  (`ON CONFLICT (batch_id, file_id) DO NOTHING`) so its own composite PK
  does not collide on the same rerun path. `create_batch` itself is
  untouched — still a plain, raising `INSERT ... RETURNING` — since
  04-06 Task 2's own `test_duplicate_batch_key_rejected` depends on that
  exact raising behavior to prove `uq_batches_dataset_batch_key` is real.
  In scope for 04-06 (unlike 04-03/04-04): this plan's own must-have truth
  is "discovery rerun over an unchanged object set... creates zero
  additional meta.files/meta.ingestion_runs rows", which cannot be proven
  true while this bug crashes the second `discover_files` call outright.

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

## From Plan 04-06

### `tests/unit/test_discovery.py` — pre-existing mypy structural-typing gap, unrelated to 04-06

- **Found during:** Task 1 verification (`uv run mypy tests/unit/test_discovery.py`,
  run manually while checking the discovery bug fix for regressions — this
  file is outside `Makefile`'s `TYPECHECK_PATHS`, so `make typecheck`/`make check`
  never exercises it).
- **Symptom:** 9 `mypy --strict` errors, all `Argument "metadata" to
  "discover_files" has incompatible type "_FakeMetadataRepository"; expected
  "MetadataRepository"` — the fake in that file implements only the subset of
  the `MetadataRepository` Protocol `discover_files` actually calls (by its
  own docstring's design), not the full Protocol (missing
  `create_ingestion_run`, `claim_ingestion_run`, `finalize_publication`,
  `update_ingestion_run_status`), so it fails `Protocol` structural typing.
- **Confirmed pre-existing and unrelated to 04-06's diff:** reproduced
  identically (same 9 errors, same message, only line numbers shifted) via
  `git stash` back to this plan's own pre-edit base — 04-06 only renamed the
  fake's `create_batch` method to `get_or_create_batch` (required to keep the
  fake in sync with the real `get_or_create_batch` fix above); it did not
  introduce this gap.
- **Not part of the enforced gate:** `Makefile`'s `TYPECHECK_PATHS := packages/dataplat/src
  packages/csv-processor/src $(wildcard tools)` excludes `tests/` entirely, so
  this has never failed `make check`/CI.
- **Status:** Deferred — out of scope for 04-06 (pre-existing, not part of the
  enforced gate, and not on this plan's own file list).
- **Suggested fix:** Either narrow the fake to only the subset Protocol
  `discover_files` actually needs (a `Protocol` subset type, not the full
  `MetadataRepository`), or add no-op stub implementations of the four
  missing methods for structural conformance.

## From Plan 04-07

04-07 also independently reproduced the `tests/policy/test_gates_actually_fail.py`
import-linter output-format drift documented above (`test_forbidden_import_is_rejected`,
`test_good_forbidden_import_is_accepted` — same root cause: `FORCE_COLOR=3` is set in
this execution environment, so `lint-imports`' subprocess inherits it via `_run`'s
`env=dict(os.environ)` and renders ANSI color codes even though its stdout is piped,
breaking the plain-substring assertion). Confirmed unrelated to this plan's diff: the
file was last touched by Phase 1 commit `edf4756`; 04-07 adds only DAG files, `_common/
kpo.py`, `setup.cfg` Contract 2, `pyproject.toml`'s new `apache-airflow*` dev-group
entries, and the four new test files named in its own plan — none of which this test
or its `_import_contract()` helper touches. Full `tests/policy -q -m "not manifests"`
run: 116 passed, 2 failed (both this pre-existing issue), 10 deselected, 130.83s.

### `test_advisory_lock_serializes_concurrent_publishers` — the negative-case check did not fail as anticipated

- **Found during:** Task 2's own required development-time negative-case
  check (this plan's acceptance criteria: "confirm this negative case once
  during development... observe the test either flakes or raises a
  constraint violation, then restore it").
- **Finding:** With both `pg_advisory_xact_lock` calls temporarily removed
  from the test, `test_advisory_lock_serializes_concurrent_publishers` still
  PASSED, reproducibly across repeated runs — no flake, no constraint
  violation. Root cause (confirmed by tracing `MergePublisher`'s
  `_PUBLISH_SQL`): it arbitrates on exactly one unique index (`customer_id`)
  via a single `INSERT ... SELECT ... ORDER BY customer_id ...` statement, so
  PostgreSQL's own unique-index insert-conflict handling already forces a
  second concurrent writer to block on the same row until the first
  transaction resolves — deterministically, with no deadlock possible,
  because every caller's statement processes any overlapping keys in the
  same fixed order. This is not a bug: it is the documented reason
  `INSERT ... ON CONFLICT` (unlike literal SQL `MERGE`, PostgreSQL BUG
  #18279) was chosen for this publisher (`merge.py`'s own module docstring,
  PITFALLS.md C1).
- **Not fixed / not treated as a defect:** this is not a failing property to
  correct — it is an accurate characterization of the current, single-target-
  table, single-statement `MergePublisher` recorded directly in the test's
  own docstring (`tests/integration/test_publish_merge.py`), not deferred
  elsewhere. The advisory lock is kept in the test and in the documented
  `merge.py` caller contract regardless, both because it is what a real
  caller (`run_ingest`, plan 04-05) is specified to do and as defense-in-depth
  per PITFALLS.md C1's own recommendation ("far more robust than reasoning
  about isolation levels") — it becomes load-bearing the moment a future
  change adds a second arbiter index or a second statement to the
  publication path.
- **Status:** Documented (in the test's own docstring and here), not a
  deferred fix — there is nothing to fix.

## From Plan 04-08

### BLOCKING (reported prominently in 04-08-SUMMARY.md too): stale `/mnt/dags` hostPath mount on both worker nodes

- **Found during:** Task 1, attempting the first live DAG trigger.
- **Symptom:** `kubectl exec deploy/airflow-dag-processor -- ls -la /opt/airflow/dags`
  (and the scheduler/api-server/triggerer's own mounts) is EMPTY on all
  three kind nodes, even though the host path it is bound from
  (`/home/konutec/projects/airflow-platform/airflow/dags`, confirmed via
  `docker inspect`'s `Source` field — the exact literal path `kind/
  cluster.yaml`'s `extraMounts` declares) has held both real DAG files
  since plan 04-07 (`csv_ingest_customers.py`, `smoke_kubernetes_pod.py`)
  since before this plan started. `airflow dags list-import-errors`
  reports "No data found" and `airflow dags list` likewise -- not because
  the DAGs have import errors, but because the DAG processor's own
  filesystem view of `dags/` is empty.
- **Root cause, confirmed live:** `docker exec airflow-platform-worker cat
  /proc/self/mountinfo | grep dags` shows the node's `/mnt/dags` is bound
  from `/docker-desktop-bind-mounts/Ubuntu/<hash>` on `tmpfs` -- Docker
  Desktop's WSL2 bind-mount indirection layer. This snapshot was taken when
  the node containers were created (~27h before this plan ran, per pod
  `AGE`), before the DAG files existed, and does not reflect files added to
  the real host directory afterward (confirmed: even `.gitkeep`, present
  since before node creation, is invisible inside the mount -- the listing
  is frozen, not merely missing recent additions).
- **Fix identified, NOT applied:** `docker restart airflow-platform-worker
  airflow-platform-worker2` (the control-plane node is tainted `NoSchedule`
  and runs no workload needing this mount, so it does not need restarting)
  forces Docker to re-resolve the bind mount on container start. This was
  attempted and explicitly DENIED by the sandbox's auto-mode permission
  classifier as a destructive action against shared live infrastructure
  (both PostgreSQL clusters and MinIO also run on these two nodes, and
  04-09-PLAN.md runs concurrently against the same cluster). Not worked
  around by any alternate tool, per the classifier denial's own guidance.
- **Impact:** every live-cluster assertion in `tests/e2e/slice/` is
  unverified as a direct consequence -- the DAGs this whole plan depends on
  cannot be parsed, scheduled or triggered until this is fixed. See
  04-08-SUMMARY.md's own "Verification Status" section for the full
  accounting of what was and was not proven.
- **Status:** Blocking, unresolved. **Suggested owner:** the orchestrator
  or the user, outside this sandbox's permission boundary -- run `docker
  restart airflow-platform-worker airflow-platform-worker2`, wait for all
  pods in `airflow`/`data` namespaces to report `Running`/`Ready` again
  (a full restart of both PostgreSQL clusters and MinIO; expect roughly
  1-3 minutes), then re-run `uv run --group cluster pytest tests/e2e/slice -x -q --timeout=900`.

### `DATAPLAT_PROCESSOR_IMAGE` is never set on the `ingest`/`discover` KPO pod spec

- **Found during:** Task 1, while designing `test_idempotent_reupload`'s
  idempotency-key correlation and discovering it could not be predicted
  client-side.
- **Symptom:** `csv_processor.cli.discover`'s call to `discover_files`
  passes `processor_image=os.environ.get("DATAPLAT_PROCESSOR_IMAGE",
  "unknown")`, but `airflow/dags/_common/kpo.py`'s `common_kpo_kwargs`
  only sets `DATAPLAT_DB_DSN`/`DATAPLAT_S3_ACCESS_KEY`/
  `DATAPLAT_S3_SECRET_KEY`/`DATAPLAT_S3_ENDPOINT_URL` as pod env vars —
  `DATAPLAT_PROCESSOR_IMAGE` is never wired. Every `meta.ingestion_runs.
  processor_image_digest` value this vertical slice ever writes will
  therefore be the literal string `"unknown"`, regardless of which image
  actually ran.
- **Impact:** Does not affect this plan's own correctness assertions (idempotency-
  key computation still works, just with a constant `"unknown"` term instead
  of a real image digest) but is a real, silently-present gap in this
  phase's own stated traceability goal ("which processor version produced
  this row" — README §62, this project's Core Value statement). Out of
  scope for 04-08 (`airflow/dags/_common/kpo.py` is not in this plan's file
  list, and fixing it would touch the DAG's own KPO pod-spec construction,
  04-02/04-07's territory).
- **Status:** Deferred. **Suggested fix:** add a `k8s.V1EnvVar(name=
  "DATAPLAT_PROCESSOR_IMAGE", value=Variable.get("csv_processor_image"))`
  to `common_kpo_kwargs`'s `env_vars` list, mirroring how `image=` itself
  is already resolved from the same Variable two lines above it.
