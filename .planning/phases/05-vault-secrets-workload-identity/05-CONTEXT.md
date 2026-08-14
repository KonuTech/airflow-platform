# Phase 5: Vault Secrets & Workload Identity - Context

**Gathered:** 2026-08-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Vault becomes the only source of runtime credentials, and workload identity becomes real enough that an unauthorized service account is provably denied. Concretely: deploy Vault in-cluster with persistent, restart-surviving storage; wire Airflow's own secrets backend to Vault (proven by deleting the Airflow metadata-DB connection and unsetting every `AIRFLOW_CONN_*`, then confirming DAGs still resolve and run); add a `vault://` scheme to `dataplat`'s existing `SecretsResolver`; authenticate the `csv-processor` ServiceAccount in the `etl` namespace via Kubernetes auth against its own Vault path, with a negative test proving the `default` ServiceAccount is denied; make Vault's audit log show who read what, when, and whether it succeeded, without ever logging a secret value; and leave no credential in git history, Python source, Dockerfiles, manifests, Airflow Variables or CI workflow files.

Out of scope (per ROADMAP.md deviation D3 and prior-phase decisions): this phase does not touch `csv_processor`'s parsing/validation logic, does not add new datasets, does not build observability (Prometheus/Grafana/OTel — Phase 7), and does not change the `etl` namespace, `csv-processor` ServiceAccount, or RBAC Role identities Phase 2/4 already established — Vault's Kubernetes-auth role binds to those existing identities, it does not redefine them.

</domain>

<decisions>
## Implementation Decisions

### Credential Migration Scope
- **D-01:** Once a credential is served from Vault, its old Kubernetes Secret is **deleted or emptied**, not left in place as an unused fallback. This applies to all of Phase 4's dev-only Secrets that get a Vault-backed replacement: `csv-processor-db`, `csv-processor-s3`, `airflow-minio-connection`, plus the Airflow metadata-DB connection object itself. Rationale (user-selected, Recommended): SEC-01 says Vault must be the ONLY source of runtime credentials — leaving old Secrets in place, even unused, is a weaker proof of that claim than actually removing them. The migration should read as provably additive-then-subtractive, not merely additive.
- Sequencing implication for planning: a credential's old Secret can only be removed AFTER its Vault-backed path is confirmed working end-to-end (the pod/DAG actually reads from Vault successfully) — removal is the last step per credential, never a batch cleanup at the end that could strand a workload if one Vault path silently didn't work.

### Unseal Ceremony
- **D-02:** Use a **scripted, single-command unseal** for local development — e.g. `make vault-unseal` reading locally-stored, gitignored unseal key(s) and unsealing automatically. Rationale (user-selected, Recommended): SC3 requires cluster-restart survival via "the documented unseal procedure," which already rules out throwaway dev-mode (dev-mode loses everything on restart — ROADMAP's own flagged pitfall). This project runs on WSL2, where a cluster restart is a realistic every-morning event; a real manual Shamir's-Secret-Sharing multi-key ceremony would be the more production-realistic choice but adds daily friction disproportionate to what a local dev environment needs. The scripted approach still uses real persistent storage and real seal/unseal mechanics — only the ceremony itself is automated for local convenience.
- This is explicitly a **local-dev-only convenience**, not the production design — SEC-14's "how a production secrets manager would substitute" documentation should note that a real deployment would use auto-unseal (cloud KMS / transit) or a genuine multi-key-holder ceremony, not a single local key file.

### Rotation Proof Depth
- **D-03:** Build an **automated, live-demonstrated rotation test** — rotate a credential's value in Vault, then assert a running workload's *next* read of that path returns the new value with no pod restart required. Rationale (user-selected, Recommended): SEC-09 only requires documenting which credentials need a restart vs. refresh dynamically, but the user wants proof over prose here, consistent with this project's Core Value (traceable, trusted, verifiable — not just claimed). Documentation of the rotation story (which credentials are read once at pod-start vs. read per-use) is still required by SEC-09 and should be written regardless, but it must be backed by this live test, not stand alone.
- Scope note for planning: this only needs ONE credential path demonstrated end-to-end (rotate → observed without restart) to satisfy the intent — it is a proof of the *mechanism*, not an exhaustive rotation test of every credential Vault now serves.

### Audit Visibility Tooling
- **D-04:** Build a **convenience script/make target** (e.g. `make vault-audit-tail`) that parses and presents Vault's audit log in a human-readable form. Rationale (user-selected, Recommended): matches the developer-experience bar Phase 4 already set with `make ingest-demo` — the audit log's raw JSON-per-line format technically satisfies SEC-08/SC4 on its own, but a project whose Core Value is about traceability being genuinely *usable*, not just technically present, benefits from the same UX investment Phase 4 made for its own live-cluster developer workflow.

### Claude's Discretion
- **Secret delivery mechanism** — `SecretsResolver`'s own docstring (`packages/dataplat/src/dataplat/secrets/resolver.py`) illustrates a `file:///vault/secrets/...` example, which reads like the Vault Agent Injector sidecar pattern. However, `.planning/research/STACK.md` §E already locks (HIGH confidence) "Vault Kubernetes auth, direct SA-token login (`hvac` + Kubernetes auth)" over the Agent Injector, CSI driver, and VSO alternatives — the app's own pod reads its ServiceAccount token and calls `auth/kubernetes/login` directly, with the secret never landing in a file the Injector wrote or a Kubernetes Secret at all. This means the actual mechanism is almost certainly a **new `vault://` scheme** added to `resolve_secret()` that performs this direct login+read, not reuse of the existing `file://` scheme against injector-mounted paths. Not asked as a user question because it is already resolved at HIGH confidence in STACK.md — flagged here so research/planning don't get misled by the resolver's illustrative docstring example into the (rejected) Agent Injector design.
- Exact Vault deployment topology (single-node dev-mode-disabled server vs. any HA consideration), the specific unseal-key storage location/format for D-02's local convenience script, and whether Vault's own bootstrap/root-token handling needs a distinct auth tier from the workload-facing Kubernetes-auth tier are all left to research/planning — STACK.md's "two-tier pattern" recommendation should be read first.
- Whether the negative "`default` SA denied" test (SEC-12) also needs a *third* identity (e.g., Airflow's own scheduler SA reaching for csv-processor's path) beyond just `default`, or whether `default` alone satisfies the requirement's literal wording, is left to planning — ROADMAP's SC2 wording only names `default`.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Vault architecture & secret-delivery decision
- `.planning/research/STACK.md` §E (HashiCorp Vault) — chart/server version pins, the secret-delivery mechanism comparison table (Agent Injector vs. CSI driver vs. VSO vs. ESO vs. direct SA-token login), and the "two-tier pattern" recommendation
- `.planning/research/ARCHITECTURE.md` — the full secrets trust-boundary chain from KPO pod through Vault to `SecretsResolver` (referenced explicitly in `03-CONTEXT.md` as informing `SecretsResolver`'s design)
- `.planning/research/PITFALLS.md` — item #13 (explicit `namespace`/`service_account_name` matched to the Vault role — the anti-pattern is widening the role when they mismatch, which silently voids least privilege), the WSL2 clock-drift-causes-spurious-auth-failures pitfall, the `disable_iss_validation` / Kubernetes-1.21-era `"iss" is invalid` false trail, and the dev-mode-loses-everything-on-restart pitfall
- `.planning/research/SUMMARY.md` — deviation D3 (Vault comes after the slice, behind `SecretsResolver`, and the retrofit must be a configuration change, not a redesign, because `SecretsResolver` already exists)

### Prior-phase decisions this phase must respect, not re-decide
- `.planning/phases/02-kind-cluster-core-infrastructure/02-CONTEXT.md` — the `etl` namespace was created in Phase 2 specifically as "the Phase 5 identity seam"; Vault policies bind to `system:serviceaccount:etl:<name>`, chosen so the role can be written narrowly the first time
- `.planning/phases/03-dataplat-core-library-metadata-control-plane/03-CONTEXT.md` — `SecretsResolver` (`packages/dataplat/src/dataplat/secrets/resolver.py`) already implements `env://`/`file://`; `vault://` is explicitly this phase's addition, designed to touch only this module's internals, never a call site (D3)
- `.planning/phases/04-vertical-slice-csv-to-analytical-postgresql/04-CONTEXT.md` and `04-02-PLAN.md`/`04-02-SUMMARY.md` — the exact identities and existing dev-only Secrets this phase migrates: ServiceAccount `csv-processor` in namespace `etl`; Secrets `csv-processor-db`, `csv-processor-s3`, `airflow-minio-connection`; RBAC at `kubernetes/rbac-etl.yaml`; the `kubectl exec -i`/`kubectl apply -f -` (stdin-only) precedent for any Vault-bootstrap mutation that needs the same treatment

### Requirements
- `.planning/REQUIREMENTS.md` — INFRA-06, SEC-01, SEC-03, SEC-04, SEC-05, SEC-06, SEC-07, SEC-08, SEC-09, SEC-12, SEC-13, SEC-14 (all Phase 5, all currently "Pending")
- `.planning/ROADMAP.md` § "Phase 5: Vault Secrets & Workload Identity" — goal, 5 success criteria, Spike U2 pass criteria (positive own-path-read + negative default-SA-denied), and the full plan-guidance block including the research-stage recommendation to run `/gsd-plan-phase 5 --research-phase` first because the kind-specific JWT-issuer caveat is explicitly unverified on this cluster

### Project-level constraints
- `.claude/CLAUDE.md` §E (HashiCorp Vault) — chart `0.34.0`, Vault `2.0.3`, `hvac 2.4.0`, the secret-delivery comparison table, and the licensing note that Vault is now BUSL-1.1/IBM-owned with OpenBao as a documented OSI-licensed escape hatch (relevant to SEC-14's production-substitution documentation)
- `.planning/PROJECT.md` — Core Value (traceable, explained, reprocessed, trusted — informs D-03's live-rotation-proof choice); §81 secrets constraints (no credential in Git, Python source, Dockerfiles, manifests, Airflow Variables, or CI workflow files)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `packages/dataplat/src/dataplat/secrets/resolver.py` — `resolve_secret(ref: str) -> str`, the single opaque-reference interpretation point. Currently handles `env://` and `file://`; any other scheme (including `vault://` today) raises `SecretResolutionError` — fail-closed by design (SEC-15). Adding `vault://` support means extending this one function's scheme dispatch, not touching any call site.
- `kubernetes/rbac-etl.yaml`, `scripts/etl-secrets.sh` (Phase 4/02) — the existing `etl` namespace RBAC Role and dev-only Secret application pattern; a Vault Kubernetes-auth role/policy follows the same least-privilege, namespace-scoped shape.
- `scripts/ingest-demo.py` (Phase 4/09) — the established pattern for a developer-facing convenience script reaching the live cluster (kubectl port-forward, credential-via-Secret-read, one-tunnel-per-connection) — the closest existing analog for D-04's audit-tail tooling and D-02's unseal script.

### Established Patterns
- Every credential in this codebase is an **opaque reference string**, never a raw value, from the point it's created to the point `resolve_secret()` interprets it — this phase's `vault://` addition must preserve that invariant exactly.
- `kubectl exec -i` (stdin) / `kubectl apply -f -` (stdin) are the two sanctioned "manual kubectl surgery" exceptions already codified in `tests/policy/test_no_manual_kubectl_surgery.py` — any Vault bootstrap script (auth method, policy, role creation) that needs to mutate the live cluster should extend this same policy-test allowlist rather than inventing a new exception shape.
- Dev-only credentials are deliberately regenerated on every `cluster-up` (Phase 2, D-14) so nothing can quietly depend on a specific value — this phase's Vault-stored secrets should follow the same discipline once Vault owns them.

### Integration Points
- Airflow's own secrets backend configuration (chart values, `AIRFLOW__SECRETS__BACKEND` etc.) is new to this phase — no prior phase touched Airflow's secret-backend wiring.
- `csv_ingest_customers` DAG / KPO pods (Phase 4, `airflow/dags/_common/kpo.py`) currently receive `env://`-scheme references as env vars built by `common_kpo_kwargs()`; this phase's `vault://` refs will most likely flow through the same env-var-holds-a-reference mechanism, resolved inside the pod by `resolve_secret()`, not injected as literal values.

</code_context>

<specifics>
## Specific Ideas

No specific UI/behavior references beyond the four decisions above — this is backend/infrastructure work with no interactive surface. The `make vault-audit-tail` and `make vault-unseal` naming in D-04/D-02 are suggestions, not locked command names; planning should match whatever naming convention keeps consistency with Phase 4's `make image-csv-processor` / `make ingest-demo` / `make cluster-verify` family.

</specifics>

<deferred>
## Deferred Ideas

None raised during this discussion — stayed within phase scope. (Observability/Prometheus/Grafana/OTel remains Phase 7's explicitly-scoped territory per ROADMAP, not re-raised here.)

</deferred>

---

*Phase: 05-vault-secrets-workload-identity*
*Context gathered: 2026-08-14*
