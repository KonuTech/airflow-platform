---
phase: 5
slug: vault-secrets-workload-identity
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-14
---

# Phase 5 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest `9.1.1` (`[tool.pytest.ini_options]`, root `pyproject.toml`) |
| **Config file** | `pyproject.toml` — `testpaths = ["tests"]`; markers `slow`/`regression`/`cluster`/`manifests` already defined. `cluster` already exists and fits a live-Vault-cluster test exactly — no new marker required. |
| **Quick run command** | `make check` (offline: unit + regression + lint + typecheck; does NOT touch a live cluster or Vault) |
| **Full suite command** | `make check && make test-integration && make cluster-verify` (existing) `+ make vault-verify` (**NEW** target this phase adds, following `cluster-verify`'s exact `RUN_CLUSTER`/live-cluster pattern) |
| **Estimated runtime** | ~5s quick (no Docker, no cluster) / existing integration+cluster tiers unchanged / the new cluster-gated Vault E2E tier (unseal-survives-restart, audit-log, rotation, positive+negative auth) is materially longer and only runs before `/gsd:verify-work`, never per-commit |

---

## Sampling Rate

- **After every task commit:** `make check` (fast, offline; catches `resolver.py`'s new `vault://` unit-test regressions immediately)
- **After every plan wave:** `make check && make test-integration && make vault-verify`
- **Before `/gsd:verify-work`:** Full suite green, exactly as established for Phases 2–4
- **Max feedback latency:** ~90 seconds for the offline gate (consistent with prior phases); the cluster-gated Vault E2E tier (unseal/restart, audit-log, rotation, negative-auth) is phase-gate-only, not part of per-commit latency budget

---

## Per-Task Verification Map

Task ID / Plan / Wave columns are filled in by the planner as it creates `PLAN.md` files — this
table pre-registers the requirement → test mapping the planner's tasks must satisfy. Threat refs
(T-05-01 through T-05-06) are drawn from `05-RESEARCH.md`'s Security Domain → Known Threat Patterns
table; additional threats identified during planning are recorded in each plan's own
`<threat_model>` block per the Security Threat Model Gate (ASVS L1).

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|------------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | INFRA-06 | T-05-02 | Vault survives a kind cluster restart with data intact; the documented `make vault-unseal` procedure restores service with no data loss | e2e (cluster) | `pytest tests/e2e/vault/test_unseal_survives_restart.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-01 | T-05-06 | Each migrated credential's old Kubernetes Secret (`csv-processor-db`, `csv-processor-s3`, `airflow-minio-connection`, Airflow metadata-DB connection) is deleted/emptied only after its Vault-backed path is confirmed working — never a batch cleanup | policy + manual verification | `pytest tests/policy/test_no_stale_secrets.py -x` (extends `test_workflow_secrets.py`'s D-14 pattern) | ❌ W0 | ⬜ pending |
| — | — | — | SEC-03 | — | No credential in Python source; `vault://` references are opaque strings by design, same invariant `resolve_secret()` already enforces | policy | Existing `gitleaks` / `test_workflow_secrets.py` machinery — no new test needed | ✅ (existing gate covers new code automatically) | ⬜ pending |
| — | — | — | SEC-04 | — | No secret baked into either image | policy/CI | Existing `trivy image --scanners secret` (CLAUDE.md §I) — no new test needed | ✅ | ⬜ pending |
| TBD | TBD | TBD | SEC-05 | — | With the Airflow metadata-DB connection deleted and every `AIRFLOW_CONN_*` unset, DAGs still resolve connections and run — proving Airflow's `VaultBackend` actually served them | e2e (cluster) | `pytest tests/e2e/vault/test_airflow_backend.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-06 / SEC-07 | T-05-01 | `csv-processor` ServiceAccount in `etl` namespace authenticates via Kubernetes auth and reads exactly its own Vault path — least-privilege identity match, `namespace`+`service_account_name` bound to the role (PITFALLS #13) | e2e (cluster) | `pytest tests/e2e/vault/test_positive_auth.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-08 | T-05-04 | Vault's audit log shows which workload read which path, when, and whether it succeeded, with no secret values present (Vault's default HMAC-SHA256 hashing, never `log_raw = true`) | e2e (cluster) | `pytest tests/e2e/vault/test_audit_log.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-09 | — | One credential path rotated in Vault; a running workload's next read returns the new value with no pod restart required (D-03 live-demonstrated proof); which credentials are read-once-at-start vs. read-per-use documented regardless | e2e (cluster) | `pytest tests/e2e/vault/test_rotation.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-12 | T-05-03 | The `default` ServiceAccount's Kubernetes-auth login against `csv-processor`'s Vault path is denied — asserted by an automated negative test, not merely inferred from policy text | e2e (cluster) | `pytest tests/e2e/vault/test_negative_auth.py -x` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | SEC-13 | T-05-02 | Dev-only Vault-stored secrets are marked, isolated, and reproducible on a fresh local rebuild, following the same regenerate-on-`cluster-up` discipline as Phase 2's D-14 | e2e (cluster) + manual (`cluster-rebuild` re-run) | `pytest tests/e2e/vault/test_dev_secrets_reproducible.py -x` | ❌ W0 | ⬜ pending |
| — | — | — | SEC-14 | — | Substitution path to a production secrets manager documented while the design is fresh | manual-only (documentation review) | N/A — see Manual-Only Verifications below | N/A | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/e2e/vault/__init__.py` + `tests/e2e/vault/conftest.py` — mirror `tests/e2e/cluster/conftest.py`'s `_require_cluster` skip-with-reason pattern exactly
- [ ] `tests/e2e/vault/test_positive_auth.py`, `test_negative_auth.py`, `test_airflow_backend.py`, `test_audit_log.py`, `test_rotation.py`, `test_unseal_survives_restart.py`, `test_dev_secrets_reproducible.py` — all new
- [ ] `tests/unit/test_secrets_resolver.py` extended with `vault://` success/failure cases using a mocked `hvac.Client` — the existing `test_vault_scheme_fails_closed_rather_than_passing_through` test asserts PRE-Phase-5 behavior and must be updated/replaced once `vault://` becomes a real scheme, not merely a rejected one
- [ ] `Makefile`: `vault-verify` target (mirrors `cluster-verify`'s shape), plus `vault-bootstrap`, `vault-unseal`, `vault-audit-tail` targets (naming per D-02/D-04, adjustable to match the `make image-csv-processor` / `make ingest-demo` family)
- [ ] Framework install: none — pytest/`hvac`/mocking libraries are already available or being added as the phase's own dependency (`hvac` → `packages/dataplat/pyproject.toml`)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Substitution path to a production secrets manager is documented end-to-end | SEC-14 | Documentation completeness/quality is not automatable — no test can assert prose content | Review `docs/` for explicit coverage of: auto-unseal (cloud KMS/transit) vs. this phase's scripted single-key local convenience (D-02); a genuine multi-key-holder ceremony vs. local dev shortcut; OpenBao as the OSI-licensed escape hatch (CLAUDE.md §E); VSO/ESO as alternatives to direct SA-token login |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 90s (offline gate); cluster-gated Vault E2E tier explicitly exempted as phase-gate-only
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
