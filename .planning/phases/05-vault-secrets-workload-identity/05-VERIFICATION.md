---
phase: 05-vault-secrets-workload-identity
verified: 2026-08-14T17:13:25Z
status: gaps_found
score: 12/13 must-haves verified
overrides_applied: 0
gaps:
  - truth: "Development secrets are reproducible when rebuilding the local environment from scratch (ROADMAP SC5 / SEC-13's literal wording)"
    status: failed
    reason: "scripts/vault-bootstrap.py's _ensure_etl_secrets() and _ensure_airflow_secrets() source the Vault KV credential VALUES they write from three Kubernetes Secrets (csv-processor-db, csv-processor-s3, airflow-minio-connection) that no script in the committed tree creates anymore. scripts/etl-secrets.sh -- their sole creator -- was deleted outright in plan 05-03 once the D-01 migration completed. Confirmed by direct code read (scripts/vault-bootstrap.py lines 416-504) and by confirming, live, that all three Secrets are NotFound in the cluster and that grepping the whole repo finds no other creation site. On a genuinely fresh cluster (make cluster-down && make cluster-up && make vault-unseal && make vault-bootstrap), every earlier bootstrap step (KV mounts, kubernetes auth method, both policies, both roles, the audit device) succeeds, but _ensure_etl_secrets/_ensure_airflow_secrets then hit the InvalidPath branch, call _kubectl_get_secret_field against a Secret that was never created, and raise RuntimeError -- main() catches it and exits 1. Vault is left structurally bootstrapped with zero credential VALUES ever written, so every KPO pod's resolve_secret(\"vault://etl/...\") and Airflow's VaultBackend lookup of minio_default fail, with no documented recovery path. This is not a hypothetical: it was independently found and documented in 05-REVIEW.md as CR-01 (pre-existing, not fixed since), and this verification pass re-confirmed it by reading the current code directly rather than trusting that report."
    artifacts:
      - path: "scripts/vault-bootstrap.py"
        issue: "_ensure_etl_secrets (lines 416-463) and _ensure_airflow_secrets (lines 466-504) have no working credential source once scripts/etl-secrets.sh (their sole prior source) is gone -- confirmed present in the current tree, unfixed"
      - path: "docs/secrets-architecture.md"
        issue: "Section 6 documents 'make cluster-down && make cluster-up && make vault-unseal && make vault-bootstrap && make vault-verify' from a clean checkout as the expected, working manual SEC-13 verification procedure. This claim is currently false given the defect above, and the document's own stated rigor bar (\"nothing here is stated without a citation to what actually proved it\") is not met for this one claim -- nothing in this phase's SUMMARY/VALIDATION artifacts shows this exact sequence was re-run after plan 05-03 deleted scripts/etl-secrets.sh."
    missing:
      - "Repoint _ensure_etl_secrets()/_ensure_airflow_secrets() at a live credential source a fresh cluster rebuild actually creates today -- e.g. the still-live minio-app Secret (scripts/minio-credentials.sh) for the MinIO access/secret key, and CNPG's own generated analytics-db-app Secret for the analytics DSN -- mirroring the pattern scripts/airflow-metadata-secret.sh already uses for the metadata DB, or generate/prompt for these values explicitly at bootstrap time instead of assuming a since-deleted Secret exists"
      - "Add an e2e or policy test that exercises the 'Vault has no KV data yet' bootstrap path specifically (not just the already-populated-Vault idempotency tests/e2e/vault/test_dev_secrets_reproducible.py currently proves), so this class of regression is caught automatically rather than only on a manual clean-checkout run"
      - "Once fixed, actually re-run the documented four-command clean-checkout sequence from a real fresh cluster and confirm it before leaving docs/secrets-architecture.md's SEC-13 claim stated as proven"
human_verification: []
---

# Phase 5: Vault Secrets & Workload Identity Verification Report

**Phase Goal:** Vault is the only source of runtime credentials, and workload identity is real enough that an unauthorized service account is provably denied
**Verified:** 2026-08-14T17:13:25Z
**Status:** gaps_found
**Re-verification:** No — initial verification

## Verification Approach Note

ROADMAP.md sets `**Mode:** mvp` on Phase 5 — and on all 11 phases in this roadmap identically (`grep -c "^\*\*Mode:\*\*" ROADMAP.md` → 11/11 phases). Running the phase goal through the User Story format guard (`gsd-sdk query user-story.validate`) confirms it does **not** match `As a ..., I want to ..., so that ....`: `{"valid": false, "errors": ["Must begin with \"As a \".", "Must contain \", I want to \".", "Must contain \", so that \".", "Must end with a period."]}`. Since this is a uniform, repository-wide default rather than a deliberate per-phase choice (an infra/security phase such as this one is not naturally expressible as a user-facing story), and forcing a User Flow Coverage table onto a non-story goal would produce a low-quality, misleading section per the MVP-mode guidance's own warning, this verification proceeds with **standard goal-backward verification** against ROADMAP's five numbered Success Criteria and the five PLAN.md frontmatter `must_haves` blocks — not the MVP User Flow Coverage format. This is noted for the user's awareness; it does not affect the substance of the findings below.

## Goal Achievement

### Observable Truths

Merged from ROADMAP's five Success Criteria (the non-negotiable contract) and the five PLAN.md frontmatter `must_haves.truths` blocks, deduplicated per the verification process's Step 2c.

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | [SC1/SEC-05] With the Airflow metadata-DB connection's env var unset and no `minio_default` DB row, DAGs still resolve connections through Vault's `VaultBackend` and run to a genuine terminal status | ✓ VERIFIED | Ran live, fresh, this session: `pytest tests/e2e/vault/test_airflow_backend.py -m cluster` (3 of 4 sub-tests, excluding only the slow full-DAG-trigger one to avoid an unrelated ~545-run Airflow backlog) — all 3 passed (env var absent from every component incl. the `airflow-worker` pod template; zero `minio_default` rows in `connection`; `airflow connections get minio_default` resolves the real MinIO host). The 4th clause (a real DAG run reaching `SUCCEEDED`) is corroborated by a **fresh** query against the live analytical DB: `meta.ingestion_runs` shows runs `SUCCEEDED` as recently as `2026-08-14 16:29:57Z` — after all five of this phase's plans completed — with **zero** `FAILED` rows currently in the table. |
| 2 | [SC2/SEC-06/SEC-07] `csv-processor` (namespace `etl`) authenticates via Kubernetes auth and reads exactly its own two Vault KV paths | ✓ VERIFIED | Ran live, fresh: `pytest tests/e2e/vault/test_positive_auth.py -m cluster` — 1 passed. Live `vault read auth/kubernetes/role/csv-processor` confirms `bound_service_account_names=[csv-processor]`, `bound_service_account_namespaces=[etl]`, `policies=[csv-processor]`; live `vault policy read csv-processor` shows exactly two `read`-only paths (`etl/data/analytics-db`, `etl/data/minio`). |
| 3 | [SC2/SEC-12] The `default` ServiceAccount (and, as a stretch case, a different-namespace `airflow-scheduler` SA) is denied login against the `csv-processor` Vault role, by an automated test | ✓ VERIFIED | Ran live, fresh: `pytest tests/e2e/vault/test_negative_auth.py -m cluster` — 2 passed (both the required `default` case and the stretch `airflow-scheduler` case; both assert `client.token is None` after the raised exception, not just the exception itself). |
| 4 | [SC3/INFRA-06] Vault runs as a persistent, non-dev StatefulSet; a `vault-0` restart reseals it without data loss; `make vault-unseal` alone restores service | ✓ VERIFIED | Live `vault status`: `Storage Type: file`, `Sealed: false`, `Total Shares/Threshold: 1/1` (Shamir, not dev mode). **Unplanned, real evidence surfaced mid-verification:** during this session's own test run, `vault-0`'s pod sandbox was recreated by the container runtime (`kubectl get events -n vault`: `SandboxChanged... Pod sandbox changed, it will be killed and re-created`, restart count 0→1) — an environmental event, not a test-triggered one (the same event also bumped `airflow-db-1`/`analytics-db-1` restart counts, consistent with a node/containerd-level blip, not a Vault-specific issue). This resealed Vault mid-session: a live pytest run failed several tests with `hvac.exceptions.VaultDown: Vault is sealed`, and `tests/e2e/vault/test_dev_secrets_reproducible.py`'s own unseal-idempotency test correctly detected the real seal and printed `"unsealed"` (not `"already unsealed"`) — proving `scripts/vault-unseal.py` genuinely restores service from a real reseal, not merely a staged one. Re-running the full batch immediately after confirmed Vault fully recovered (`Sealed: false` again) with **zero data loss** (all roles/policies/KV versions unchanged; `test_rerunning_vault_bootstrap_against_a_live_vault_changes_nothing` passed with an identical before/after snapshot). The dedicated `tests/e2e/vault/test_unseal_survives_restart.py` (which stages the identical scenario deliberately) was read in full and is substantive and correctly wired into `make vault-verify`, but was not re-executed here — redundant given the stronger, unplanned live proof just described. |
| 5 | [SC4/SEC-08] Vault's audit log records both a successful and a denied login with identity/outcome, and never contains a plaintext secret value; `make vault-audit-tail` renders it human-readably | ✓ VERIFIED | Ran live, fresh: `pytest tests/e2e/vault/test_audit_log.py -m cluster` — 3 passed (successful `csv-processor` login recorded; denied `default` login recorded; current plaintext DSN/access-key/secret-key/conn_uri all absent from the raw tailed log text, non-vacuity control also passed). Also ran `python scripts/vault-audit-tail.py --lines 20` live: produced clean, compact, human-readable lines (timestamp, path, identity, outcome) with zero raw JSON dumped, including live `airflow:airflow-api-server` reads of `airflow/data/connections/minio_default` timestamped seconds before this check. |
| 6 | [SC5/SEC-01/SEC-03/SEC-04] No credential literal exists in any script or Helm values file for this phase's migrated credentials; the three legacy Kubernetes Secrets no longer exist anywhere in the live cluster | ✓ VERIFIED | Live `kubectl get secret -n etl csv-processor-db csv-processor-s3` and `kubectl get secret -n airflow airflow-minio-connection`: all three `NotFound`. `grep -rln AIRFLOW_CONN_MINIO_DEFAULT` across the whole repo: zero matches in `helm/`, only in test files asserting its *absence* and one migration-source comment in `vault-bootstrap.py`. `pytest tests/policy/test_workflow_secrets.py` (17/17) and `tests/policy/test_no_stale_secrets.py` (4/4) both pass, run fresh. |
| 7 | [SC5/SEC-13] Development secrets (`.secrets/vault-init.json`) are marked, never committed, isolated from production, **and reproducible when rebuilding the local environment from scratch** | ✗ **FAILED** | Marked/never-committed/isolated all hold: live `git check-ignore .secrets/vault-init.json` exits 0, `git ls-files .secrets` is empty, file confirmed mode `0600`. **The reproducibility clause is false**, confirmed by direct code reading (not by trusting 05-REVIEW.md): see Gaps below (CR-01). `tests/e2e/vault/test_dev_secrets_reproducible.py` only proves idempotent re-run against an *already-bootstrapped* Vault (which passed live, fresh, this session) — its own docstring explicitly states the fresh-cluster case is "deliberately NOT run here." |
| 8 | [05-01] `make vault-bootstrap` is idempotent against an already-configured Vault: re-running performs zero additional writes | ✓ VERIFIED (scoped) | Ran live, fresh: `pytest tests/e2e/vault/test_dev_secrets_reproducible.py -m cluster` — 4 passed, including a real subprocess re-run of `scripts/vault-bootstrap.py` whose before/after snapshot (auth methods, mounts, audit devices, role definitions, KV version numbers) was byte-identical. This truth's literal scope ("against an already-configured Vault") holds; see truth 7 for the separate, failed from-empty-state claim. |
| 9 | [05-02] `resolve_secret()` interprets `vault://mount/path#field`, authenticates once per process, fails closed on malformed refs / hvac errors / missing fields | ✓ VERIFIED | Code read in full (`packages/dataplat/src/dataplat/secrets/resolver.py`). `pytest tests/unit/test_secrets_resolver.py` — 6/6 passed (mocked `hvac.Client`, no live cluster). Live-exercised for real by truths 1–2 above (the same code path a real KPO pod and `test_positive_auth.py` both use). |
| 10 | [05-03] The Vault `airflow` role's `bound_service_account_names` reflects empirically-justified identities, not an uncorrected guess | ✓ VERIFIED | Live `vault read auth/kubernetes/role/airflow`: `bound_service_account_names=[airflow-api-server airflow-triggerer airflow-worker airflow-scheduler]` — matches `scripts/vault-bootstrap.py`'s own per-SA justification comments (3 of 4 live-observed this phase's session; the 4th, `airflow-scheduler`, is explicitly labeled in-code as an architectural-necessity addition for CI's LocalExecutor, not live-observed under this profile's KubernetesExecutor — an honestly disclosed distinction, not a hidden gap). |
| 11 | [05-04/SEC-09] Rotating `minio_default`'s value in Vault is reflected on Airflow's very next read, with no pod restart | ✓ VERIFIED | Ran live, fresh: `pytest tests/e2e/vault/test_rotation.py -m cluster` — 1 passed (rotated, read via the same running `airflow-api-server` pod, observed the new value, restored the original in `finally`, re-confirmed restoration). |
| 12 | [05-05] A permanent regression guard prevents the three migrated Secret names from ever again being a creation target | ✓ VERIFIED | `pytest tests/policy/test_no_stale_secrets.py` — 4/4 passed (the guard itself, two non-vacuity mutation tests including one inside a Kubernetes `env:` list — the exact real historical shape — and one false-positive control). Wired into `make policy`/`make check` automatically (no separate invocation needed, confirmed by its presence in the full `tests/policy` run). |
| 13 | [05-05/SEC-14] The secrets architecture is documented end-to-end in one place (injection, trust boundaries, production substitution) | ✓ VERIFIED (with a caveat) | `docs/secrets-architecture.md` exists (263 lines), covers all 6 named sections, cites concrete files/tests throughout. **Caveat, tied to truth 7's gap:** §6's clean-checkout reproducibility claim is currently inaccurate (see Gaps) — the document's own self-imposed citation discipline is not fully met for that one section. |

**Score:** 12/13 truths verified (1 FAILED — see Gaps below)

### Required Artifacts

All 17 artifacts named across the five plans' frontmatter were checked at three levels (exists, substantive, wired). All passed.

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `helm/values/local/vault.yaml` | Non-dev StatefulSet, file storage, persistent audit storage | ✓ VERIFIED | 95 lines; `helm template` renders (exit 0) a `StatefulSet` with `storage "file"` + `listener "tcp" { tls_disable = 1 }`; live-deployed and running |
| `helm/values/ci/vault.yaml` | Same shape, CI-sized | ✓ VERIFIED | 55 lines; `helm template` renders (exit 0); diverges from local only on resource sizing (D-06), confirmed by `test_values_profiles.py` |
| `scripts/vault-bootstrap.py` | Idempotent hvac bootstrap | ✓ VERIFIED (wired), ⚠️ see Gaps | 628 lines; live-run repeatedly this session, zero-write idempotent against an already-configured Vault; **not** functional against a from-empty-state Vault (CR-01, truth 7) |
| `scripts/vault-unseal.py` | D-02 scripted init-or-unseal | ✓ VERIFIED | 270 lines; live-exercised twice this session (once via an unplanned real reseal, once via the standing test suite) |
| `tests/e2e/vault/test_unseal_survives_restart.py` | INFRA-06 live proof | ✓ VERIFIED | 266 lines; read in full, substantive, correctly wired into `make vault-verify`; not re-executed (redundant given the unplanned live restart evidence, truth 4) |
| `packages/dataplat/src/dataplat/secrets/resolver.py` | `vault://` scheme dispatch | ✓ VERIFIED | 111 lines; unit-tested (6/6) and live-exercised via real KPO pods and `test_positive_auth.py` |
| `tests/e2e/vault/test_positive_auth.py` | SEC-06/SEC-07 live proof | ✓ VERIFIED | 91 lines; ran live, fresh — passed |
| `tests/e2e/vault/test_negative_auth.py` | SEC-12 live proof | ✓ VERIFIED | 82 lines; ran live, fresh — passed (2/2) |
| `helm/values/local/airflow.yaml` | `config.secrets` = VaultBackend, no `AIRFLOW_CONN_MINIO_DEFAULT` | ✓ VERIFIED | 303 lines; `helm template` renders; live cluster confirmed matching config; zero `secretKeyRef` blocks remain |
| `tests/e2e/vault/test_airflow_backend.py` | SEC-05 live proof | ✓ VERIFIED | 220 lines; 3/4 sub-tests ran live, fresh, passed; 4th corroborated via fresh DB query (see truth 1) |
| `tests/e2e/vault/test_rotation.py` | D-03 live rotation proof | ✓ VERIFIED | 193 lines; ran live, fresh — passed |
| `scripts/vault-audit-tail.py` | D-04 human-readable audit renderer | ✓ VERIFIED | 272 lines; ran live, fresh — clean output; ⚠️ see Anti-Patterns (WR-05, a real but unhit crash risk on an explicit JSON `null` field) |
| `tests/e2e/vault/test_audit_log.py` | SEC-08 live proof | ✓ VERIFIED | 263 lines; ran live, fresh — passed (3/3) |
| `tests/e2e/vault/test_dev_secrets_reproducible.py` | SEC-13 live proof (already-bootstrapped case) | ✓ VERIFIED | 254 lines; ran live, fresh — passed (4/4); its own docstring correctly discloses the fresh-cluster case is out of its scope |
| `tests/policy/test_no_stale_secrets.py` | SEC-01 permanent regression guard | ✓ VERIFIED | 259 lines; ran fresh — passed (4/4); wired into `make policy` |
| `docs/secrets-architecture.md` | SEC-14 end-to-end documentation | ✓ VERIFIED (caveat) | 263 lines, 6 cited sections; one section's claim is inaccurate (tied to truth 7's gap) |
| `docs/adr/0009-openbao-licence-escape-hatch.md` | Vault BUSL-1.1 licence ADR | ✓ VERIFIED | 164 lines; 5 `## ` sections confirmed via `grep -c`; `docs/adr/README.md` updated (Records table has the `0009` row, deferred-records table now shows "none currently outstanding") |

### Key Link Verification

| From | To | Via | Status | Details |
|------|-----|-----|--------|---------|
| `scripts/stages/80-vault.sh` | `helm/values/{local,ci}/vault.yaml` | `helm_install vault hashicorp/vault ... hookOnly` | ✓ WIRED | Confirmed by live-deployed, running `vault-0`; `bash -n` syntax-checks clean |
| `scripts/vault-bootstrap.py` | Vault's kubernetes auth method | `client.sys.enable_auth_method("kubernetes")` | ✓ WIRED | Live `vault auth list` shows `kubernetes/` enabled and configured |
| `scripts/vault-unseal.py` | `.secrets/vault-init.json` | `json.dump` of unseal key + root token, `chmod 600` | ✓ WIRED | Live file confirmed, mode `0600`, gitignored |
| `airflow/dags/_common/kpo.py` | `packages/dataplat/.../resolver.py` | `value="vault://etl/...#..."`, resolved by `resolve_secret()` inside the pod | ✓ WIRED | `kpo.py` sets exactly the literal `vault://etl/...` values; `csv_processor.cli._build_common()` resolves them via a confirmed double-`resolve_secret()` call (the real bug plan 05-03 found and fixed); corroborated by fresh `meta.ingestion_runs` SUCCEEDED rows |
| `packages/dataplat/.../resolver.py` | Vault's `csv-processor` Kubernetes-auth role | `client.auth.kubernetes.login(role="csv-processor", jwt=...)` | ✓ WIRED | Live-exercised by `test_positive_auth.py`, passing |
| `helm/values/{local,ci}/airflow.yaml` | Vault's `airflow` Kubernetes-auth role | `config.secrets.backend_kwargs.kubernetes_role = "airflow"` | ✓ WIRED | Live `airflow config get-value secrets backend_kwargs` returns the exact configured JSON; live audit log shows successful `airflow:airflow-api-server` logins seconds before this check |
| `scripts/vault-bootstrap.py` | `airflow/connections/minio_default` | `create_or_update_secret(mount_point="airflow", path="connections/minio_default")` | ✓ WIRED | Live `vault kv metadata get airflow/connections/minio_default` shows an active, versioned (v11) secret |
| `scripts/vault-audit-tail.py` | `/vault/audit/audit.log` inside `vault-0` | `kubectl exec -i -n vault vault-0 -- tail -n <N> ...` | ✓ WIRED | Ran live, produced correct output |
| `docs/adr/README.md` | `docs/adr/0009-openbao-licence-escape-hatch.md` | Records table row + removal from deferred-records table | ✓ WIRED | Confirmed via direct read |

### Data-Flow Trace (Level 4)

This phase is backend/infrastructure, not a UI rendering dynamic data, but the equivalent trace — does the credential reference actually resolve to a real, working connection, not a stub — was performed live end-to-end:

| Artifact | Data Variable | Source | Produces Real Data | Status |
|----------|---------------|--------|---------------------|--------|
| `csv_processor.cli._build_common()` | `dsn`, `access_key`, `secret_key` | `resolve_secret(resolve_secret("env://DATAPLAT_DB_DSN"))` → `vault://etl/analytics-db#dsn` | Yes — a real `postgresql://` DSN and real MinIO credentials, read live via `test_positive_auth.py` and confirmed by fresh `meta.ingestion_runs` SUCCEEDED rows (the pipeline actually connects and completes) | ✓ FLOWING |
| Airflow `minio_default` Connection | resolved by `BaseHook.get_connection` | `VaultBackend` → `airflow/connections/minio_default#conn_uri` | Yes — live `airflow connections get minio_default` names the real `minio.data.svc.cluster.local` endpoint; a real, deferred `S3KeySensor` poke against it succeeds (corroborated by fresh SUCCEEDED ingestion runs) | ✓ FLOWING |

### Behavioral Spot-Checks

All checks below were run live against the actual cluster during this verification session (not inherited from SUMMARY.md).

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Vault is unsealed, non-dev, file-backed | `kubectl exec -n vault vault-0 -- vault status` | `Sealed: false`, `Storage Type: file`, `Total Shares/Threshold: 1/1` | ✓ PASS |
| Three legacy Secrets are gone | `kubectl get secret -n etl csv-processor-db csv-processor-s3`; `kubectl get secret -n airflow airflow-minio-connection` | All three `NotFound` | ✓ PASS |
| `AIRFLOW_CONN_MINIO_DEFAULT` fully removed | `grep -rln AIRFLOW_CONN_MINIO_DEFAULT helm/` + live env checks | Zero matches in `helm/`; absent from every live component | ✓ PASS |
| Unit tests (secrets resolver + CLI) | `uv run pytest tests/unit/test_secrets_resolver.py tests/unit/test_csv_processor_cli.py -q` | 14 passed | ✓ PASS |
| Full offline policy suite | `uv run pytest tests/policy -q -m "not manifests"` | 124 passed, 10 deselected, 0 failed | ✓ PASS |
| Phase-5-specific policy tests, verbose | `pytest tests/policy/test_gates_actually_fail.py tests/policy/test_no_stale_secrets.py tests/policy/test_values_profiles.py tests/policy/test_no_manual_kubectl_surgery.py tests/policy/test_offline_gate_stays_offline.py tests/policy/test_dag_thinness.py tests/policy/test_workflow_secrets.py -v` | 56 passed | ✓ PASS |
| Live e2e Vault suite (positive/negative auth, audit log, reproducibility, rotation) | `uv run --frozen --group cluster pytest tests/e2e/vault/test_positive_auth.py tests/e2e/vault/test_negative_auth.py tests/e2e/vault/test_audit_log.py tests/e2e/vault/test_dev_secrets_reproducible.py tests/e2e/vault/test_rotation.py -v -m cluster` | 11 passed (after the incidental reseal event described in truth 4 self-resolved) | ✓ PASS |
| Live e2e Airflow-backend suite (env/DB-row/CLI clauses) | `pytest tests/e2e/vault/test_airflow_backend.py -v -m cluster -k "not test_dag_still_resolves_its_connection_and_runs"` | 3 passed | ✓ PASS |
| `make vault-audit-tail` produces human-readable output | `python scripts/vault-audit-tail.py --lines 20` | Clean, compact lines; zero raw JSON | ✓ PASS |
| Helm templates render for both profiles (airflow + vault) | `./tools/bin/helm template ...` ×4 | All exit 0 | ✓ PASS |
| Recent real DAG runs reach SUCCEEDED via Vault-resolved credentials | `psql ... SELECT status, count(*) FROM meta.ingestion_runs GROUP BY status` | `SUCCEEDED: 23`, `PENDING: 1`, `FAILED: 0`; most recent SUCCEEDED at `2026-08-14 16:29:57Z` | ✓ PASS |
| Full fresh-cluster bootstrap (the one behavior the gap concerns) | Not run (would require destroying the live cluster) — assessed by direct code read instead | `scripts/vault-bootstrap.py`'s `_ensure_etl_secrets`/`_ensure_airflow_secrets` depend on Secrets confirmed absent and never re-created | ✗ FAIL (see Gaps) |

### Probe Execution

No probe scripts declared or found (`find scripts -path '*/tests/probe-*.sh'` — none; no PLAN/SUMMARY references). SKIPPED — not applicable to this phase.

### Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|---|---|---|---|---|
| INFRA-06 | 05-01 | Vault deployed in-cluster, survives cluster restart without manual data loss | ✓ SATISFIED | Live restart-survival confirmed (incidental real event, this session) |
| SEC-01 | 05-02, 05-03, 05-05 | Dedicated secrets-management solution is the only source of runtime credentials | ✓ SATISFIED* | True for live runtime reads today; *the underlying bootstrap tooling cannot itself be reconstituted from empty state — tracked under SEC-13 |
| SEC-03 | 05-02 | No credential hard-coded in Python source | ✓ SATISFIED | Grep-clean; `test_workflow_secrets.py` 17/17 |
| SEC-04 | 05-02 | No secret baked into any container image | ✓ SATISFIED | No image/Dockerfile changes this phase; only non-secret config added |
| SEC-05 | 05-03 | Airflow resolves connections through Vault backend, verified with DB conn deleted + env unset | ✓ SATISFIED | Live, fresh test run (3/4) + fresh DB corroboration (4th) |
| SEC-06 | 05-02, 05-03 | Task pods obtain only their own required credentials via explicit namespace/SA matched to Vault role | ✓ SATISFIED | Live role/policy inspection + live test |
| SEC-07 | 05-02, 05-03 | Workloads authenticate with least-privilege K8s SA identity, not shared root token | ✓ SATISFIED | Root token confined to host-side bootstrap tooling only (code-confirmed) |
| SEC-08 | 05-04 | Secret access auditable (who/when/success) without logging values | ✓ SATISFIED | Live, fresh test run + live audit-tail run |
| SEC-09 | 05-04 | Rotation documented, including restart-vs-dynamic-refresh distinction | ✓ SATISFIED | `docs/secrets-architecture.md` §4 + live, fresh rotation test |
| SEC-12 | 05-02 | Negative test proves unauthorized SA denied | ✓ SATISFIED | Live, fresh test run (2/2) |
| SEC-13 | 05-01, 05-04, 05-05 | Dev secrets marked/isolated/never committed/**reproducible on fresh rebuild** | ✗ **BLOCKED** | CR-01: bootstrap cannot complete from an empty Vault — see Gaps |
| SEC-14 | 05-05 | Secrets architecture documented end-to-end | ✓ SATISFIED* | Document exists, 6 cited sections; *one section's claim is inaccurate, tied to the SEC-13 gap |

No orphaned requirements: every ID ROADMAP.md maps to "Phase 5" (`INFRA-06, SEC-01, SEC-03, SEC-04, SEC-05, SEC-06, SEC-07, SEC-08, SEC-09, SEC-12, SEC-13, SEC-14`) appears in at least one plan's frontmatter `requirements:` list, and no plan claims a requirement ROADMAP.md does not also map to this phase.

### Anti-Patterns Found

All confirmed by direct code reading during this verification pass (not inherited from 05-REVIEW.md without re-checking).

| File | Line(s) | Pattern | Severity | Impact |
|------|---------|---------|----------|--------|
| `scripts/vault-bootstrap.py` | 279-286 (`_ensure_policy`) | Idempotency check compares only the policy **name**, never the live HCL **body**, against the target | ⚠️ WARNING | If `_CSV_PROCESSOR_POLICY`/`_AIRFLOW_POLICY` is ever edited (e.g. to narrow access after a mistake), a re-run of the idempotent bootstrap silently keeps the old, wider policy live — undermines "least-privilege is maintainable over time." Same root-cause family as the SEC-13 gap above (idempotent "ensure" functions that converge on presence, not on target state); does not currently violate an observable must-have since no policy body has changed since bootstrap, but is a real, confirmed, currently-dormant defect (05-REVIEW.md CR-02) |
| `scripts/vault-bootstrap.py` | 335-344 (`_ensure_kubernetes_role`) | Drift check compares only `bound_service_account_names`, never `bound_service_account_namespaces`/`policies`/TTLs | ⚠️ WARNING | A future change to a role's namespace list or attached policies, with the SA-name set unchanged, would silently not apply on re-run — same convergence-on-presence class of gap as above |
| `packages/dataplat/src/dataplat/secrets/resolver.py` | 91-109 | The `vault://` branch's `try/except` only catches `hvac.exceptions.VaultError` and `KeyError`; `_vault_client()` (called inside the same `try`) can raise raw `KeyError` (unset env var), `OSError` (missing SA token file), or `requests.exceptions.ConnectionError`/`Timeout` (Vault transiently unreachable) — the last of these is not caught at all, and `KeyError` from a missing env var is mis-reported as "vault secret has no field" | ⚠️ WARNING | Contradicts the module's own documented "fails closed... every unsupported case raises [`SecretResolutionError`] instead" contract for a realistic failure mode (Vault unreachable right after a `vault-0` restart) |
| `packages/csv-processor/src/csv_processor/cli.py` | 197 (`discover()`) | No `except Exception:` fallback (unlike `ingest()`, which has one per 04-REVIEW.md WR-01) | ⚠️ WARNING | A non-`DataPlatformError` failure inside `_build_common()` (more likely now, given the Vault-auth code path this phase added) skips the documented `{"status": "FAILED", ...}` forensic XCom payload |
| `scripts/vault-audit-tail.py` | 174-177 (`_format_entry`) | `entry.get("request", {}).get("path", "?")` does not guard against `entry["request"]` being an explicit JSON `null` (only an absent key) | ⚠️ WARNING | A single audit-log entry with `"request": null` (plausible for a pre-auth/error-path row) would raise `AttributeError` and crash the entire tail render, contradicting the module's own "never crashes over one bad line" promise |
| `helm/versions.env` | 27 | `VAULT_CHART_VERSION` is pinned but there is no companion `VAULT_IMAGE_TAG`/`VAULT_VERSION`, unlike every other component (`MINIO_IMAGE_TAG`, `AIRFLOW_IMAGE_TAG`) | ℹ️ INFO | The deployed Vault server binary version floats with the chart's own default `appVersion`; a future chart bump could silently change the running Vault major/minor with no reviewable literal change |

**Debt markers:** Zero `TBD`/`FIXME`/`XXX`/`TODO`/`HACK`/`PLACEHOLDER` markers found across all 29 files this phase created or modified.

### Human Verification Required

None. This phase is entirely backend/infrastructure; every claim was directly, programmatically testable and was tested live during this session.

### Gaps Summary

**One confirmed, currently-real gap blocks a clean pass: SEC-13's "reproducible when rebuilding the local environment" clause is false.**

`scripts/vault-bootstrap.py`'s two credential-populating steps — `_ensure_etl_secrets()` and `_ensure_airflow_secrets()` — read their source values from three Kubernetes Secrets (`csv-processor-db`, `csv-processor-s3`, `airflow-minio-connection`) that this phase's own plans (05-02, 05-03) deleted, along with the only script that ever created them (`scripts/etl-secrets.sh`, deleted in plan 05-03). This was independently identified and documented in `05-REVIEW.md` as **CR-01** and remains unfixed at the time of this verification — confirmed here by reading the current code directly, not by trusting that report. On the **currently-live, already-bootstrapped** cluster this is invisible: every idempotency check takes the "already present" branch and the bug is never reached, which is exactly why `tests/e2e/vault/test_dev_secrets_reproducible.py` (live, passing) does not catch it — its own docstring honestly discloses this scope limit. But a genuinely fresh cluster rebuild (`make cluster-down && make cluster-up && make vault-unseal && make vault-bootstrap`) — the exact scenario ROADMAP's SC5 names ("reproducible... on a fresh local rebuild") and this project's own WSL2-realistic framing treats as routine, not exceptional — would get Vault's structural scaffolding built (mounts, auth method, policies, roles, audit device all succeed) and then fail with an uncaught `RuntimeError` the moment it tries to populate a KV secret's value, leaving every workload's credential resolution broken with no documented recovery path. `docs/secrets-architecture.md` §6 currently documents this exact four-command sequence as an expected-to-work manual verification procedure — that specific claim is not currently true.

A related, currently-dormant defect (**CR-02**, also independently confirmed by direct code read, also unfixed) shares the same root cause: `_ensure_policy()` only checks whether a policy **name** exists, never whether its live HCL body matches the target, so editing `_CSV_PROCESSOR_POLICY`/`_AIRFLOW_POLICY` and re-running the idempotent bootstrap would silently leave the old, wider policy in effect. This does not currently violate any observable must-have (no policy body has changed since bootstrap), so it is reported as a WARNING-level anti-pattern rather than a second formal gap — but it is real, confirmed, and belongs to the same "idempotent 'ensure' functions converge on presence, not on target state" concern as the SEC-13 gap, and should be fixed alongside it.

Everything else this phase set out to prove — Vault as the sole live source of runtime credentials, positive and negative workload-identity proofs, audit visibility with no leaked secret values, live-observed rotation with no restart, and (via an unplanned real pod restart that occurred mid-verification) genuine restart-survival with scripted recovery — is verified against the live cluster, today, with fresh evidence gathered in this session, not carried over from SUMMARY.md claims.

---

_Verified: 2026-08-14T17:13:25Z_
_Verifier: Claude (gsd-verifier)_
