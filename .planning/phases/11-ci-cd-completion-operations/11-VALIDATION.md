---
phase: 11
slug: ci-cd-completion-operations
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-22
---

# Phase 11 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.1.1 (already pinned), markers: `cluster`, `manifests`, `integration`, `dagtest`, `dbt`, `slow`, `regression` (`pyproject.toml`) — this phase adds a new `chaos` marker (QUAL-15) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` / `[tool.coverage.*]` |
| **Quick run command** | `uv run pytest tests/unit tests/regression -q --cov --cov-report=term-missing` (existing `make test`) |
| **Full suite command** | `make ci` (existing offline gate: `check manifest-policy gitleaks gitleaks-selftest`) for non-cluster work; `make cluster-verify`-equivalent CI jobs (new `publish.yml`/E2E workflows) for cluster-dependent suites — deliberately NOT folded into `make ci` |
| **Estimated runtime** | ~5 min quick (no cluster) / PR smoke subset target <15 min (D-20) / merge-triggered full E2E + rebuild-from-raw plausibly 45-120+ min (Pitfall 9 — no prior job at this scale; `tests/e2e/observability` alone took ~8 min on an already-warm cluster) / chaos suite runs in parallel with full E2E (D-27), similar order of magnitude |

---

## Sampling Rate

- **After every task commit:** `make test` (existing quick unit/regression + coverage, no cluster)
- **After every plan wave:** `make ci` (existing offline gate) + the relevant new cluster-dependent suite run manually against a local `cluster-up` before trusting CI to catch it first
- **Before `/gsd:verify-work`:** All of — `make ci` green, PR smoke subset green, merge-triggered full E2E green (including rebuild-from-raw), chaos suite green
- **Max feedback latency:** 300 seconds (quick tier); live-cluster and CI-workflow tiers sampled per-wave/phase-gate, not per-commit

---

## Per-Task Verification Map

Draft — written before planning, from `11-RESEARCH.md`'s Phase Requirements → Test Map, Wave 0
Gaps, and Common Pitfalls sections. Real plan IDs and wave numbers supersede the `TBD` placeholder
below once `/gsd:plan-phase 11`'s planner runs.

| Task ID | Plan | Wave | Requirement | Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|----------|-----------|--------------------|-------------|--------|
| TBD | TBD | TBD | CICD-05 | Coverage reported via `$GITHUB_STEP_SUMMARY` table + uploaded `htmlcov/` artifact; no `fail_under` gate introduced | CI-workflow-level | Observe job summary contains `## Coverage Report` + `coverage-html` artifact present after a run | ❌ Wave 0 |
| TBD | TBD | TBD | CICD-06 | Images tagged by git SHA on merge to main (`csv-processor`, `dbt`, Airflow) | CI-workflow-level | `gh api /users/<owner>/packages/container/csv-processor/versions` shows a `sha`-tagged version after merge | ❌ Wave 0 |
| TBD | TBD | TBD | CICD-06 (D-09/D-13, Pitfall 5) | PR builds pushed to GHCR as `pr-<number>`, signed + SBOM'd identically to merge images | CI-workflow-level | `gh api .../csv-processor/versions` shows `pr-<N>` tag; `cosign verify` against it succeeds with the same identity matcher as merge images | ❌ Wave 0 |
| TBD | TBD | TBD | CICD-06 (D-03) | GitHub Release creation additionally tags all three images with the release's semver | CI-workflow-level | Create a test Release; `gh api` shows the semver tag alongside the existing `sha` tag | ❌ Wave 0 |
| TBD | TBD | TBD | CICD-06 (D-11) | `pr-<number>` GHCR package version deleted automatically when the PR closes | CI-workflow-level | Close a PR; `gh api .../versions` no longer lists the `pr-<N>` tag | ❌ Wave 0 |
| TBD | TBD | TBD | CICD-08 | trivy fails the build on HIGH/CRITICAL for every published image; `.trivyignore` entries dated+justified (D-07) | CI-workflow-level, policy | Policy test grepping `publish.yml` for `trivy image --exit-code 1 --severity HIGH,CRITICAL` (mirrors `test_supply_chain_guards.py`'s existing pattern) | ❌ Wave 0 |
| TBD | TBD | TBD | CICD-09 (D-20) | PR smoke subset: cluster boots, all core services + Kyverno Helm-release healthy, one DAG run reaches `SUCCEEDED`, Vault + Kyverno positive/negative checks pass | cluster (live) | New composed smoke test/job run inside the ephemeral-kind PR workflow (reuses existing `cluster`-marked tests where possible) | ❌ Wave 0 |
| TBD | TBD | TBD | CICD-09 (D-19/D-24/D-27/D-30) | Merge-triggered full E2E: existing local suite + 2yr sweep green, rebuild-from-raw runs LAST | cluster (live) | `pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q -m cluster` (existing `cluster-verify` target) wired into new workflow | ✅ target exists; workflow wiring ❌ Wave 0 |
| TBD | TBD | TBD | D-14/D-15/D-18 (Kyverno) | Signed project image admitted; unsigned/tampered image denied at admission (positive+negative) | cluster (live) | `tests/e2e/cluster/test_kyverno_admission.py` (mirrors `test_minio_buckets.py` shape) | ❌ Wave 0 |
| TBD | TBD | TBD | D-16 | Pinned upstream third-party images (Airflow/MinIO/Vault/CNPG/Prometheus) boot via exception list without requiring a signature; exception list stays in sync with `helm/versions.env` | cluster (live) + policy | Same file as above for the live check; policy test asserting exception list ⊆ pinned image refs (mirrors `test_supply_chain_guards.py`) | ❌ Wave 0 |
| TBD | TBD | TBD | D-17/Pitfall 3 | Kyverno's real footprint (4 controllers, ~400m CPU steady-state) fits the CI profile's effective budget | policy | `pytest tests/policy/test_manifest_resources.py -k ci_profile_fits_runner -q` (existing dynamic-summing test, extended by `helm/values/ci/kyverno.yaml`'s addition) | ✅ test exists; new values file ❌ Wave 0 |
| TBD | TBD | TBD | D-40 (verify, not build — Pitfall 1) | `raw` bucket denies `s3:DeleteObject`/`DeleteObjectVersion` to the app credential, permits the admin credential | cluster (live) | `tests/e2e/cluster/test_minio_buckets.py::test_raw_delete_is_denied_for_app_credential` / `::test_raw_delete_is_permitted_for_admin_credential` | ✅ already exists (Phase 2, `c600905`) |
| TBD | TBD | TBD | QUAL-15 | 11 named §84 scenarios pass (pod crash, DB/MinIO/Vault unavailable, malformed CSV, invalid encoding, OOM, task timeout, duplicate batch, secret rotation, unauthorized secret access) | cluster (live), new `chaos` marker | `pytest tests/e2e/chaos -q -m cluster` (11 new modules, own ephemeral-kind cluster per D-25) | ❌ Wave 0 |
| TBD | TBD | TBD | OBS-06 | 18 runbook docs exist under `docs/runbooks/` (verified §89 list), each with symptoms/diagnosis/recovery/reprocessing/verification headings | structural/policy | Policy test asserting `docs/runbooks/*.md` count == 18, each containing the 5 required headings | ❌ Wave 0 |
| TBD | TBD | TBD | INFRA-11 (D-38) | Retention DAG dry-runs by default; hard-delete requires explicit `retention.enforce: true` opt-in | unit + dagtest | `pytest tests/unit/test_retention_*.py -q` (dry-run logic) + `pytest tests/dagtest -q -k retention` (DAG structure, not in any ingestion DAG's task graph per D-35) | ❌ Wave 0 |
| TBD | TBD | TBD | INFRA-11 (D-39) | Per-dataset `retention:` YAML block validated by the same Pydantic contract model as the rest of the dataset config | unit | Extends existing dataset-config validation test module with a `retention` block case | ❌ Wave 0 |
| TBD | TBD | TBD | INCR-07 (D-28/D-29 pts 1–3) | Post-rebuild row counts, business-column content hash (lineage columns excluded per Pitfall 7), and SCD2 version/state all match the pre-drop snapshot | cluster (live), part of full E2E | `pytest tests/e2e/slice/test_rebuild_from_raw.py -q -m cluster` (new; uses `_table_checksum(columns=...)` corrected variant) | ❌ Wave 0 |
| TBD | TBD | TBD | INCR-07 (D-29 pt 4 / D-34) | Rebuild's own backfill re-exercises `record_reconciliation` per reprocessed file; previously-resolved quarantine rows return to PENDING (expected, not excluded from comparison) | cluster (live) | Same test module — asserts `meta.reconciliation_results` rows written during rebuild backfill; asserts D-34's expected quarantine-history-loss behavior | ❌ Wave 0 |
| TBD | TBD | TBD | INCR-07 (D-32/D-33) | `make rebuild-from-raw` drops ETL-owned schemas, wipes MinIO `validated`/`processed`/`quarantine` layers, runs `alembic upgrade head`, triggers full-history backfill DagRuns | integration/e2e | Same test module invokes the Make target directly (one implementation, two callers per D-32) | ❌ Wave 0 |
| TBD | TBD | TBD | D-12 | Rollback Make target redeploys a prior git-SHA-tagged image | integration | New test: deploy SHA A, run rollback target to SHA B, assert deployed pod image tag == B | ❌ Wave 0 |
| TBD | TBD | TBD | D-22 | Merge-triggered full E2E/chaos failure auto-files or updates (idempotent) a GitHub issue tagging the failing commit | CI-workflow-level | Policy test asserting the failure step's `gh issue list --search` + create-or-comment logic is present; functional check via a deliberately-failing scratch run | ❌ Wave 0 |

*Status column tracks EXECUTION, not planning — all rows are "pending" until `/gsd:execute-phase
11` runs.*

*Status legend: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/e2e/chaos/` — new directory + `chaos` pytest marker registration in `pyproject.toml`, covering QUAL-15's 11 scenarios.
- [ ] `tests/e2e/cluster/test_kyverno_admission.py` — D-18 positive+negative (signed admitted / unsigned denied).
- [ ] `tests/e2e/slice/test_rebuild_from_raw.py` (or extension of the 2-year-sweep module) — INCR-07/D-29, using a column-exclusion-aware `_table_checksum` variant (Pitfall 7; do not call the existing function unchanged — see Common Pitfalls #7 in RESEARCH.md).
- [ ] `tests/unit/test_retention_*.py` + `tests/dagtest/test_platform_retention*.py` — INFRA-11 dry-run + DAG-structure tests.
- [ ] `helm/values/{local,ci}/kyverno.yaml`, `helm/versions.env` entry (`KYVERNO_CHART_VERSION=3.8.2`), `scripts/stages/25-kyverno.sh` (deploy EARLY — see Pitfall 4, not appended after `85-monitoring.sh`), and a Kyverno `ImageValidatingPolicy` manifest + its own apply stage — none exist yet.
- [ ] `.github/workflows/publish.yml`, plus the new full-E2E/smoke/chaos workflow(s) or jobs — only `ci.yml` exists today.
- [ ] `docs/runbooks/` directory — does not exist yet (18 files per the verified §89 list, including a short stub for the "CDC failure" scenario per Open Question 1 — CDC is out of v1 scope).
- [ ] Extend `tests/policy/test_manifest_resources.py`'s `test_ci_profile_fits_runner` coverage once `helm/values/ci/kyverno.yaml` lands (Pitfall 3 — real footprint is ~400m CPU across 4 controllers, not the ~100-200m D-17 assumed; disable `cleanupController`).
- [ ] Extend `test_supply_chain_guards.py`-style policy coverage to assert D-16's Kyverno exception list stays in sync with `helm/versions.env`.
- [ ] Confirm during Wave 0 (Open Question 2): render the Kyverno chart (`helm template`) and check whether it needs `cert-manager` or fully self-manages webhook certs, before finalizing values files.

*Every item above traces to a `❌ Wave 0` cell in the Per-Task Verification Map or to a named
Pitfall/Open Question in `11-RESEARCH.md` — none are speculative additions.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Runbook usability by someone who did not build the platform | OBS-06 (success criterion 5) | The structural check (file count, required headings) is automated above, but whether a runbook is actually followable to diagnosis/recovery/verification is a human-judgment property — this is the phase's own success-criterion wording, not an automatable assertion | Have a second person (or a fresh-context review pass) walk one non-trivial runbook end-to-end against a deliberately-broken local cluster and confirm they reach recovery without out-of-band help |
| Docker Hub PAT provisioning (D-21) | CICD-09 | Creating the Docker Hub account/PAT and adding it as a GitHub repo secret is a one-time manual credential-provisioning step, not a test — RESEARCH.md confirms this secret is "not yet provisioned" | Provision the PAT, add as a repo secret, confirm `docker/login-action` step succeeds in a real CI run (falls back to anonymous rate limits if skipped — degraded, not broken) |

*Carried from `11-RESEARCH.md` Open Question 3: the exact placement of the `ImageValidatingPolicy`
manifest (a new numbered stage script vs. a small wrapper chart) is a Wave-0 implementation choice
within CONTEXT.md's own "Claude's Discretion" grant, not a gap requiring manual verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies — pending planner output
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify — pending planner output
- [ ] Wave 0 covers all MISSING references — draft table above enumerates every `❌ Wave 0` cell from `11-RESEARCH.md`'s own Phase Requirements → Test Map, Wave 0 Gaps, and Common Pitfalls sections
- [ ] No watch-mode flags — pending planner output
- [ ] Feedback latency < 300s (quick tier) — pending planner output
- [ ] `nyquist_compliant: true` set in frontmatter — pending, will flip once the planner's real plan IDs replace the `TBD` placeholders above

**Approval:** pending
