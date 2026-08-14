# Phase 5: Vault Secrets & Workload Identity - Research

**Researched:** 2026-08-14
**Domain:** HashiCorp Vault deployment on kind, Kubernetes-auth workload identity, Airflow native secrets backend integration, `dataplat.SecretsResolver` `vault://` scheme
**Confidence:** HIGH

## Summary

This phase's two explicit unknowns were both resolved by direct, empirical verification rather than inference — the strongest possible outcome for a `--research-phase` run.

**Question 1 (kind JWT-issuer caveat):** RESOLVED — the caveat does not apply. The live `kind-airflow-platform` cluster's OIDC discovery document (`kubectl get --raw /.well-known/openid-configuration`) returns `issuer: "https://kubernetes.default.svc.cluster.local"`, which is exactly the standard in-cluster kubeadm default, not a kind-specific oddity. Official Vault documentation, fetched directly, confirms `disable_iss_validation` has defaulted to `true` since Vault 1.9.0 specifically *because* the Kubernetes TokenReview API performs the same validation — so on this project's pinned Vault major (2.x), no `issuer` value ever needs to be read from or matched against that discovery document. ARCHITECTURE.md's flagged claim describes the pre-1.9, Kubernetes-1.21-era workaround; it is obsolete for this stack. PITFALLS.md's own D3 entry already reached this conclusion by reasoning from the changelog — this research upgrades that conclusion from reasoned inference to empirically verified fact, against both the actual cluster and the actual current docs.

**Question 2 (`auth_type: kubernetes` provenance):** RESOLVED, with one correction to prior research. The live Airflow scheduler pod's installed packages were read directly (`pip list` inside the running pod): `apache-airflow-providers-hashicorp==4.7.1` and `hvac==2.4.0` are **already installed** in the stock `apache/airflow:3.3.0` image — this project never needs a custom Airflow Dockerfile for Vault support, and never needs to install this provider. The exact tag to verify against is therefore `providers-hashicorp/4.7.1`, not `4.8.0` (STACK.md/CLAUDE.md cited 4.8.0 as the PyPI-latest at research time, correctly noting the constraints file pins 4.7.1 — but the running cluster settles which one is authoritative). Fetched directly from `raw.githubusercontent.com` at that exact tag: `kubernetes_role` and `kubernetes_jwt_path` (default `/var/run/secrets/kubernetes.io/serviceaccount/token`) are the only two kubernetes-auth-specific constructor parameters; both are validated present-and-non-empty before any auth attempt; the underlying call is `hvac.api.auth_methods.Kubernetes(client.adapter).login(role=..., jwt=..., mount_point=...)` — a thin, direct wrapper with no hidden parameters, no `audience` field, and no additional Airflow-side configuration required beyond `backend_kwargs`.

**Primary recommendation:** Deploy Vault via the already-locked chart/version pins (STACK.md §E) with `server.standalone.enabled: true`, `server.dev.enabled: false`, file storage, `injector.enabled: false`, `csi.enabled: false` — the chart's own `authDelegator.enabled: true` default already creates the `system:auth-delegator` binding this project needs, removing one manual step prior research assumed would be needed. Add exactly one new Python dependency (`hvac`, already `[OK]` per slopcheck and already running live in the Airflow image) to `packages/dataplat/pyproject.toml`; extend `resolve_secret()` with a `vault://<mount>/<path>#<field>` scheme that authenticates once per process (module-level cached client) and reads via `hvac`'s KV v2 API. Both Vault-Kubernetes-auth integrations (Airflow's native `VaultBackend` and `dataplat`'s `SecretsResolver`) are separate roles/policies bound to different ServiceAccounts, and — a new, concrete finding — Airflow's side is genuinely uncertain about *which* ServiceAccount identity actually performs the login in Airflow 3's Task-SDK-isolated architecture; this must be settled empirically during the phase's own bootstrap-and-test spike, not assumed, exactly as PITFALLS B5/D3 already warn.

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| INFRA-06 | Vault deployed in-cluster, survives restart without manual data loss | Vault chart 0.34.0/0.34.1 standalone + file storage config verified against pulled `values.yaml`; D-02's scripted unseal addressed in Architecture Patterns and Code Examples |
| SEC-01 | Vault is the only source of runtime credentials | D-01's Runtime State Inventory (below) enumerates every existing Secret/env-var wiring that must be removed, in dependency order |
| SEC-03 | No credential hard-coded in Python source | `resolve_secret()`'s existing fail-closed design (already built, Phase 3) extended with `vault://`; no literal ever needed |
| SEC-04 | No secret baked into any image | Confirmed: `hvac` install is a dependency addition only; the Airflow image needs zero changes (provider + hvac already present) |
| SEC-05 | Airflow resolves connections through Vault, proven by deleting the connection + unsetting `AIRFLOW_CONN_*` | Exact `VaultBackend` config keys verified against provider source; the concrete existing connection (`minio_default`, `AIRFLOW_CONN_MINIO_DEFAULT`) and its current Secret-based delivery mechanism identified live |
| SEC-06 | Task pods obtain only their own credentials via explicit `namespace`/`service_account_name` matched to a Vault role | `csv-processor`/`etl` identity already exists (Phase 4); Vault role/policy design and the still-open "which Airflow SA" question documented |
| SEC-07 | Least-privilege ServiceAccount identity, not a shared root token | Two-tier pattern (STACK.md, locked) — separate `csv-processor` and `airflow` Vault roles/policies |
| SEC-08 | Auditable secret access, no secret values in the log | Vault audit log format verified (JSON, HMAC-SHA256 hashing of string values by default) via official docs; `file_path=stdout` vs. persistent-file trade-off documented with a policy-test-compatible implementation path |
| SEC-09 | Documented secret rotation, restart vs. dynamic-refresh | Airflow's `SecretCache` (`AIRFLOW__SECRETS__USE_CACHE`, default `False`) verified; ETL-pod-side "resolved once per process" behavior confirmed by reading the actual `_build_common()` call site |
| SEC-12 | Negative test: unauthorized SA denied | Exact Vault Kubernetes-auth failure mode (`"service account name not authorized"`) verified via HashiCorp's own issue trackers/support articles; hvac exception-handling guidance given |
| SEC-13 | Dev secrets marked, isolated, reproducible on rebuild | D-14 precedent (Phase 2) and `etl-secrets.sh`'s idempotent-`ensure` pattern carried forward as the template for Vault-stored dev secrets |
| SEC-14 | End-to-end documented secrets architecture, including production substitution | OpenBao noted (STACK.md, licence escape hatch); VSO named as the documented next step if long-lived-Deployment rotation-without-restart is ever required |
</phase_requirements>

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**D-01 — Credential Migration Scope:** Once a credential is served from Vault, its old Kubernetes Secret is deleted or emptied, not left in place as an unused fallback. This applies to `csv-processor-db`, `csv-processor-s3`, `airflow-minio-connection`, plus the Airflow metadata-DB connection object itself. Sequencing implication: a credential's old Secret can only be removed AFTER its Vault-backed path is confirmed working end-to-end — removal is the last step per credential, never a batch cleanup.

**D-02 — Unseal Ceremony:** Use a scripted, single-command unseal for local development (e.g. `make vault-unseal` reading locally-stored, gitignored unseal key(s)). Explicitly a local-dev-only convenience — SEC-14's production-substitution documentation should note a real deployment would use auto-unseal (cloud KMS / transit) or a genuine multi-key-holder ceremony.

**D-03 — Rotation Proof Depth:** Build an automated, live-demonstrated rotation test — rotate a credential's value in Vault, then assert a running workload's *next* read of that path returns the new value with no pod restart required. Only ONE credential path needs demonstrating end-to-end; this is a proof of the mechanism, not an exhaustive rotation test.

**D-04 — Audit Visibility Tooling:** Build a convenience script/make target (e.g. `make vault-audit-tail`) that parses and presents Vault's audit log in human-readable form, matching the developer-experience bar `make ingest-demo` already set.

### Claude's Discretion

- Secret delivery mechanism is already resolved at HIGH confidence (STACK.md §E): direct SA-token login (`hvac` + Kubernetes auth), a NEW `vault://` scheme in `resolve_secret()` — NOT the Agent Injector pattern the resolver's docstring illustratively mentions. This is confirmed correct by this research (see Architecture Patterns).
- Exact Vault deployment topology, unseal-key storage location/format, and whether Vault's bootstrap/root-token handling needs a distinct auth tier from the workload-facing tier — addressed below.
- Whether the negative test needs a third identity beyond `default` — addressed in Common Pitfalls / Open Questions.

### Deferred Ideas (OUT OF SCOPE)

None raised during discussion. Observability/Prometheus/Grafana/OTel remains Phase 7's territory, not touched here.
</user_constraints>

## Project Constraints (from CLAUDE.md)

- Vault chart `0.34.0`, Vault server `2.0.3` (major is 2.x, BUSL-1.1, IBM-owned) — pinned, HIGH confidence per CLAUDE.md's own stack table. This research found `0.34.1`/`2.0.4` are now the latest patch releases (3 days after CLAUDE.md's 2026-08-11 verification date) — see State of the Art for the freshness note.
- `apache-airflow-providers-hashicorp 4.8.0 (constraints pin 4.7.1)`, `hvac 2.4.0` — this research corrects which version is actually authoritative: **4.7.1 is what is genuinely installed and running**, confirmed live.
- Secret delivery: Vault Kubernetes auth, direct SA-token login (no Agent Injector, no CSI driver, no VSO/ESO) — locked, HIGH confidence, not re-opened here.
- No credential may exist in Git, Python source, Dockerfiles, Kubernetes manifests, Airflow Variables, or CI workflow files — runtime injection only (§81). This phase is the one that makes this claim newly true for the credentials Phase 4 left in Kubernetes Secrets.
- `kubectl exec -i` (stdin) / `kubectl apply -f -` (stdin) are the two sanctioned manual-kubectl exceptions in `tests/policy/test_no_manual_kubectl_surgery.py`; any Vault bootstrap script extends this same allowlist rather than inventing a new shape. This research identifies that `make vault-audit-tail` fits the *existing* `exec -i` pattern with zero test changes if it reads a persistent audit-log file (recommended) — see Common Pitfalls.
- Dev-only credentials are regenerated on every `cluster-up` (Phase 2, D-14) so nothing quietly depends on a specific value — Vault-stored secrets should follow the same discipline once Vault owns them.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Vault server deployment, storage, unseal | Kubernetes Platform (Vault StatefulSet + PVC) | — | A stateful in-cluster service; owns its own file-storage volume, no dependency on any other new tier |
| Vault Kubernetes-auth method config (auth mount, roles, policies) | Vault (server-side config) | Kubernetes Platform (ServiceAccounts/RBAC it binds to must already exist) | Vault-side objects, but every `bound_service_account_names`/`_namespaces` value is a foreign key into K8s identity created by Phase 2/4 |
| Airflow connection resolution (`minio_default`, future DB conn) | Airflow Control Plane (whichever component actually calls the secrets backend — see Open Questions) | Vault | The `[secrets] backend` config lives in Airflow's Helm values; the actual HTTP call happens inside an Airflow process, not a separate service |
| ETL pod credential resolution (`vault://` scheme) | `dataplat` Library (`SecretsResolver`) | ETL Task Pod (presents the identity) | The library owns interpreting the reference; the pod's ServiceAccount is what Vault actually authenticates — a clean split matching SEC-15's existing opaque-reference design |
| Workload identity (ServiceAccounts, Kubernetes-auth roles) | Kubernetes Platform | Vault | ServiceAccounts are K8s objects created by Phase 2/4; Vault roles are a read-only binding against them, never the reverse |
| Audit visibility (`make vault-audit-tail`) | Vault (audit device, the source of truth) | Developer tooling (`scripts/`, a thin renderer) | Vault produces and durably stores the log; the script only formats it for a human |
| Negative-test enforcement (SEC-12) | Vault (role/policy boundary — where the guarantee actually lives) | Test suite (`tests/e2e/vault/` — where the guarantee is proven) | The boundary is Vault config; a test that only asserts application-level behavior without exercising the actual `auth/kubernetes/login` call would not prove anything about the boundary itself |

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| `hashicorp/vault` Helm chart | `0.34.0` (locked, STACK.md) — `0.34.1` now current, see State of the Art | Vault server deployment | `[VERIFIED: pulled values.yaml + templates at v0.34.0 tag]` — standalone mode, file storage, `authDelegator.enabled: true` default all confirmed against the actual chart source |
| Vault server | `2.0.3` (chart default) | Secrets engine + Kubernetes auth method | `[VERIFIED: official docs]` `disable_iss_validation` default `true` since 1.9; major-2.x behavior confirmed unchanged for this default |
| `hvac` | `2.4.0` | Python client for both the ETL pod's direct login and (transitively, inside the Airflow image) the `VaultBackend` provider | `[VERIFIED: live cluster pip list + PyPI + slopcheck OK]` — already running in the deployed Airflow scheduler pod; latest on PyPI; passed `slopcheck install hvac` with `[OK]` disposition |
| `apache-airflow-providers-hashicorp` | `4.7.1` (this is what is **actually installed**, not `4.8.0`) | Airflow's native Vault secrets backend | `[VERIFIED: live cluster pip list + GitHub raw source at providers-hashicorp/4.7.1 tag]` — already installed, zero new install needed |

### Supporting

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `vault` CLI binary | N/A — recommend NOT adding it | Manual `vault operator init/unseal/write` | `[ASSUMED — recommendation, not a locked decision]` Prefer an `hvac`-based Python bootstrap script over shelling out to a new pinned binary: `hvac` is already a dependency, avoids growing `tools/bin/`'s pinned-binary-installer pattern for a tool used only at bootstrap, and is directly unit-testable with a mocked `hvac.Client`. If the planner prefers the `vault` CLI for operational familiarity, it follows the exact `tools/k8s/install_{helm,kind,kubeconform}.sh` pattern already established (pinned SHA-256, verify-before-execute) — this is a real, viable alternative, just not the one this research recommends. |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Direct SA-token login (locked) | Vault Agent Injector | Already rejected at HIGH confidence in STACK.md — not re-opened. A sidecar per short-lived KPO pod doubles pod count and adds startup latency for zero benefit this phase needs. |
| `hvac`-based Python bootstrap | `vault` CLI + shell script | The CLI is more familiar to Vault operators and has richer error output; the Python approach avoids a new pinned binary and is more testable. Either is legitimate — flagged as an open implementation choice, not a research-blocking question. |
| File audit device with `file_path=/vault/audit/audit.log` (persistent) | `file_path=stdout` (HashiCorp's own Kubernetes-recommended pattern) | stdout mixes Vault's operational logs with audit entries (mitigated by a configured `prefix`) and is read via `kubectl logs`, which is NOT currently in this repo's `test_no_manual_kubectl_surgery.py` permitted read-only set (`get`, `wait` only) — using it requires a one-line, well-justified policy-test extension. The persistent-file approach reads via the ALREADY-permitted `kubectl exec -i` pattern with zero test changes. Recommend persistent file for that reason, unless durability-through-restart (a PVC) is judged not worth the extra `auditStorage` volume. |

**Installation:**
```bash
# packages/dataplat/pyproject.toml — add to [project.dependencies]
# "hvac>=2.4,<3",
uv lock
uv sync --locked --all-packages --no-dev
```

**Version verification performed this session:**
```bash
pip index versions hvac                     # 2.4.0 confirmed latest
kubectl -n airflow exec deploy/airflow-scheduler -c scheduler -- pip list | grep -i "hashicorp\|hvac"
#  apache-airflow-providers-hashicorp  4.7.1
#  hvac                                2.4.0
curl -s https://api.github.com/repos/hashicorp/vault-helm/releases/latest | grep tag_name   # v0.34.1
curl -s https://api.github.com/repos/hashicorp/vault/releases/latest | grep tag_name        # v2.0.4
```
`apache-airflow-providers-hashicorp` needs **no verification command of its own** — it is already resolved, pinned by the Airflow image, and confirmed running.

## Package Legitimacy Audit

| Package | Registry | Age | Downloads | Source Repo | slopcheck | Disposition |
|---------|----------|-----|-----------|-------------|-----------|-------------|
| `hvac` | PyPI | 10+ years (first release 2015-era; `2.x` line active) | High (official HashiCorp-ecosystem client; also a transitive dependency of `apache-airflow-providers-hashicorp`, already vetted by the Airflow project itself) | `github.com/hvac/hvac` | `[OK]` | Approved — add to `packages/dataplat/pyproject.toml` |
| `apache-airflow-providers-hashicorp` | PyPI | Official Apache Software Foundation provider | Bundled in every `apache/airflow` reference image | `github.com/apache/airflow` (monorepo, `providers/hashicorp/`) | Not re-checked — already installed, not a new install this phase performs | No action — already present at `4.7.1` |

**Packages removed due to slopcheck `[SLOP]` verdict:** none.
**Packages flagged as suspicious `[SUS]`:** none.

`hvac`'s package-name provenance: discovered via the already-locked, HIGH-confidence STACK.md (§E, an authoritative prior-research source) AND independently re-confirmed this session via `pip list` inside the live, already-deployed Airflow pod (an authoritative empirical source — not merely "registry existence"). Both conditions of the provenance rule are satisfied, so `[VERIFIED]` is used above rather than `[ASSUMED]`.

## Architecture Patterns

### System Architecture Diagram — corrected secrets trust-boundary chain

ARCHITECTURE.md §9.1 (the file the CONTEXT.md canonical refs point at) diagrams the Vault Agent Injector pattern — the design this project explicitly does NOT use (STACK.md's two-tier pattern supersedes it, locked HIGH confidence). The diagram below is the corrected chain for what this project actually builds, verified against this session's live-cluster reads and the provider source.

```
┌─────────────────────────────── TIER 1: Airflow's own connections ───────────────────────────────┐
│                                                                                                     │
│  Airflow component (scheduler / triggerer / api-server — WHICH one actually performs the login    │
│  is an open question, see Open Questions)                                                          │
│         │  reads its own projected SA token from                                                   │
│         │  /var/run/secrets/kubernetes.io/serviceaccount/token  (DEFAULT_KUBERNETES_JWT_PATH,       │
│         │  verified in provider source — no explicit mount needed, every pod already has this)      │
│         ▼                                                                                           │
│  VaultBackend.__init__(auth_type="kubernetes", kubernetes_role="airflow", mount_point="airflow")    │
│         │  → hvac.api.auth_methods.Kubernetes(client.adapter).login(role="airflow", jwt=<token>)    │
│         ▼                                                                                           │
│  POST /v1/auth/kubernetes/login  {role: "airflow", jwt: <token>}                                    │
│         │                                                                                           │
│         ▼   ── Vault validates via Kubernetes TokenReview API (NOT issuer matching — Q1) ──         │
│  Vault role "airflow": bound_service_account_names/_namespaces match?  → token_policies: ["airflow"]│
│         ▼                                                                                           │
│  Policy "airflow":  path "airflow/data/connections/*" { capabilities = ["read"] }                   │
│         ▼                                                                                           │
│  KV v2 read →  airflow resolves Connection `minio_default` (conn_uri or discrete fields)            │
│                — NEVER a Kubernetes Secret, NEVER an env var, from this point forward               │
│                                                                                                       │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────── TIER 2: ETL pod's own credentials ────────────────────────────────┐
│                                                                                                       │
│  KubernetesPodOperator creates the pod                                                              │
│      namespace: etl   serviceAccountName: csv-processor                                             │
│      env: DATAPLAT_DB_DSN = "vault://etl/analytics-db#dsn"      (a LITERAL string value now —       │
│           DATAPLAT_S3_ACCESS_KEY = "vault://etl/minio#access_key" not a secretKeyRef — kpo.py change)│
│           DATAPLAT_S3_SECRET_KEY = "vault://etl/minio#secret_key"                                    │
│         ▼                                                                                            │
│  csv_processor.cli._build_common()  (called ONCE per pod, at CLI-command start — verified live)      │
│         │  resolve_secret("vault://etl/analytics-db#dsn")  [× 3, one per env var]                    │
│         ▼                                                                                            │
│  dataplat.secrets.resolver — vault:// handler (NEW this phase):                                      │
│      1. lazily authenticate ONCE per process (module-level cached hvac.Client) —                     │
│         reads /var/run/secrets/.../token, POSTs auth/kubernetes/login {role: "csv-processor", ...}   │
│      2. per resolve_secret() call: KV v2 read at mount_point=<netloc>, path=<url path>,               │
│         return data["data"][<fragment>]                                                              │
│         ▼                                                                                            │
│  Policy "csv-processor":  path "etl/data/analytics-db" { capabilities = ["read"] }                   │
│                           path "etl/data/minio"         { capabilities = ["read"] }                  │
│                           # nothing else — cannot read "airflow/data/connections/*"                  │
│         ▼                                                                                            │
│  psycopg ConnectionPool(dsn), S3ObjectStore(access_key, secret_key)                                  │
│      — the credential value is now resolved for the pod's ENTIRE lifetime; a NEW pod (the next       │
│        task run) re-resolves fresh, which is what makes D-03's rotation proof structural for this    │
│        tier (see Common Pitfalls / Open Questions on why this is NOT the same as "no restart")       │
│                                                                                                       │
└───────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

```
kubernetes/
├── rbac-vault.yaml           # NEW — Vault's own ServiceAccount (if not chart-managed) is
│                              #   already created by the chart; this file, if needed at all,
│                              #   is for anything the chart's authDelegator: true does NOT
│                              #   cover (verify against the pulled chart first — likely nothing).
helm/values/local/vault.yaml  # NEW — server.standalone, server.dataStorage, injector: false,
helm/values/ci/vault.yaml     # NEW — dev.enabled: true (CI keeps dev mode, per existing STACK.md guidance)
helm/values/local/airflow.yaml # EDIT — add config.secrets.backend / backend_kwargs;
helm/values/ci/airflow.yaml    #   EDIT — same, CI's Vault is dev-mode so backend_kwargs' url differs
scripts/
├── vault-bootstrap.sh (or .py) # NEW — idempotent: enable kv-v2 mounts, kubernetes auth method,
│                                #   both roles, both policies — mirrors etl-secrets.sh's `ensure` shape
├── vault-unseal.sh             # NEW — D-02: reads gitignored .secrets/vault-init.json
├── vault-audit-tail.sh         # NEW — D-04: kubectl exec -i into the vault pod, tail the
│                                #   persistent audit log, pipe through jq for readability
├── stages/80-vault.sh          # NEW — numbered after 75-etl.sh; calls the three scripts above
packages/dataplat/src/dataplat/secrets/
├── resolver.py                 # EDIT — add the vault:// scheme branch + module-level cached client
tests/
├── unit/test_secrets_resolver.py   # EDIT — extend with vault:// success/failure cases (mocked hvac)
├── e2e/vault/                      # NEW — mirrors tests/e2e/cluster/'s shape exactly
│   ├── conftest.py                 #   live-cluster fixtures, same _require_cluster pattern
│   ├── test_positive_auth.py       #   csv-processor SA reads its own path
│   ├── test_negative_auth.py       #   default SA denied (SEC-12)
│   ├── test_airflow_backend.py     #   SEC-05: delete connection + unset AIRFLOW_CONN_*, DAG still runs
│   ├── test_audit_log.py           #   SEC-08: log entry exists, no secret value present
│   ├── test_rotation.py            #   D-03: rotate → next read reflects new value, no restart
│   └── test_unseal_survives_restart.py  # SC3
docs/adr/0009-openbao-licence-escape-hatch.md  # NEW — next free ADR number; STACK.md already
│                                                 asked for this and it was never created
```

### Pattern 1: Module-level cached authenticated client (recommended, not yet built)

**What:** `resolver.py`'s `vault://` handler should authenticate to Vault exactly once per process (lazy singleton), not once per `resolve_secret()` call.

**When to use:** Any call site that resolves multiple `vault://` references in one process lifetime — which is every current call site (`_build_common()` resolves 3 references in a row; Airflow's `VaultBackend` resolves at least 1 connection per DAG's sensor).

**Why it matters:** Without caching the client, a pod that resolves 3 secrets performs 3 separate `auth/kubernetes/login` round trips at startup — needless Vault load and needless audit-log noise (SEC-08's log becomes 3x larger for one pod's one legitimate credential need). `hvac.Client` objects are cheap to reuse and `hvac`'s own token-renewal helpers exist if a long-lived process needs them (not needed here — KPO pods are short-lived).

**Example:**
```python
# Source: this project's existing env://Zfile:// pattern in resolver.py, extended by inference
# from the verified hvac.Kubernetes(...).login() signature (GitHub, providers-hashicorp/4.7.1)
# and the verified "resolve_secret() called 3x per pod" fact (cli.py, read live this session).
_client: hvac.Client | None = None

def _vault_client() -> hvac.Client:
    global _client
    if _client is None:
        client = hvac.Client(url=os.environ["VAULT_ADDR"])
        token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
        client.auth.kubernetes.login(
            role=os.environ["VAULT_K8S_ROLE"],   # e.g. "csv-processor" — set per-pod, not hardcoded
            jwt=token_path.read_text(),
        )
        _client = client
    return _client
```

### Pattern 2: `vault://` URI shape — `scheme://mount/path#field`

**What:** `vault://etl/analytics-db#dsn` → `mount_point="etl"`, `path="analytics-db"`, `field="dsn"`.

**When to use:** Every `vault://` reference this phase creates. This is a RECOMMENDATION (Claude's discretion area per CONTEXT.md), not a locked decision — flagged here so the planner has a concrete, minimal-new-code starting point rather than an open design question.

**Why this shape:** `urlsplit()` — already imported and used by `resolver.py` for `env://`/`file://` — parses this with zero new parsing logic: `.netloc` is the mount point, `.path.lstrip("/")` is the KV path, `.fragment` is the field name. This exactly mirrors the existing two schemes' minimalism and keeps the whole `vault://` addition inside `resolve_secret()`'s scheme dispatch, touching no call site — which is D3's explicit requirement.

**Example:**
```python
# Source: pattern inferred from resolver.py's existing urlsplit() usage (packages/dataplat/
# src/dataplat/secrets/resolver.py, read directly this session) + STACK.md's confirmed
# mount_point="etl"/"airflow" convention (§E, locked).
if parsed.scheme == "vault":
    mount_point = parsed.netloc
    path = parsed.path.lstrip("/")
    field = parsed.fragment
    if not (mount_point and path and field):
        msg = f"malformed vault:// ref (need scheme://mount/path#field): {ref!r}"
        raise SecretResolutionError(msg, context={"ref": ref})
    try:
        client = _vault_client()
        secret = client.secrets.kv.v2.read_secret_version(mount_point=mount_point, path=path)
        return secret["data"]["data"][field]
    except hvac.exceptions.VaultError as exc:
        msg = f"vault read failed for ref {ref!r}: {exc}"
        raise SecretResolutionError(msg, context={"ref": ref}) from exc
    except KeyError as exc:
        msg = f"vault secret at {mount_point}/{path} has no field {field!r}"
        raise SecretResolutionError(msg, context={"ref": ref}) from exc
```

### Pattern 3: Airflow `VaultBackend` config — exact keys, verified against source at the running version

**What:** `[secrets] backend` + `backend_kwargs`, wired via the Airflow Helm chart's `config:` block (converts to `AIRFLOW__SECRETS__*` env vars).

**Example:**
```yaml
# Source: providers-hashicorp/4.7.1 vault_client.py __init__ signature (fetched verbatim this
# session) + STACK.md §E's already-locked mount_point/connections_path convention.
# helm/values/{local,ci}/airflow.yaml, under the existing `config:` key (if absent, create it).
config:
  secrets:
    backend: airflow.providers.hashicorp.secrets.vault.VaultBackend
    backend_kwargs: >-
      {"url": "http://vault.vault.svc.cluster.local:8200",
       "auth_type": "kubernetes",
       "kubernetes_role": "airflow",
       "mount_point": "airflow",
       "connections_path": "connections",
       "variables_path": null,
       "config_path": null,
       "kv_engine_version": 2}
```
`variables_path: null` is not just a generic best practice here — it is confirmed necessary for a REAL reason already live in this codebase: `airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()` calls `Variable.get("csv_processor_image")` **at DAG-parse time** (inside the `@dag`-decorated function body, not inside a `@task`). That Variable must keep resolving from the Airflow metadata DB exactly as it does today; routing Variable lookups through Vault would put a network call on the DAG processor's parse-time critical path for a value this phase never intends to move.

### Anti-Patterns to Avoid

- **Assuming a single Airflow ServiceAccount presents to Vault.** Airflow 3's Task-SDK isolation means it is genuinely unclear, without testing, whether the scheduler, triggerer, api-server, or dag-processor is the process that actually performs `VaultBackend`'s login for a given lookup — and this project's own live DAG (`S3KeySensor(deferrable=True)`) exercises exactly the ambiguous case (a deferred sensor's connection resolution). Binding `bound_service_account_names` to a guess and then "fixing" a permission-denied by widening it to `["*"]` is precisely PITFALLS B5/D3's warned anti-pattern. See Open Questions.
- **Reusing the resolver's `file:///vault/secrets/...` docstring example as a design.** That path only exists under the Agent Injector pattern (a sidecar-rendered tmpfs file), which this project does not deploy. CONTEXT.md already flags this; this research confirms the flag is correct and the docstring should be corrected or annotated when `vault://` is implemented, so a future reader is not misled the same way.
- **Setting `server.dev.enabled: true` for local (non-CI) Vault.** Confirmed still correct to avoid: dev mode is in-memory and loses all configuration — including the Kubernetes auth method itself — on every pod restart, which on this project's WSL2 environment is functionally "every morning" (PITFALLS D1, unchanged by this research).
- **Reading Vault's audit log via a bare `kubectl logs` in a committed script without extending the policy test first.** `kubectl logs` is currently outside `test_no_manual_kubectl_surgery.py`'s permitted read-only set (`get`, `wait` only) — this will fail CI the moment it is written, unless the persistent-file + `exec -i` approach is used instead (recommended, zero test changes) or the policy test is deliberately, narrowly extended first.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|--------------|-----|
| Validating a Kubernetes ServiceAccount JWT | A custom JWT/issuer validator | Vault's built-in Kubernetes auth method (TokenReview-backed) | This IS the entire reason Vault's Kubernetes auth method exists; it already handles issuer validation, expiry, signature verification and the TokenReview round trip correctly — reinventing any part of it is the single most likely place to introduce an actual authentication bypass in this phase |
| Cross-namespace / cross-ServiceAccount access control | Application-level "is this caller allowed" checks in Python | Vault policies + `bound_service_account_names`/`_namespaces` | Least-privilege enforcement belongs at the trust boundary (Vault), not in the application that would be compromised alongside any bypass — SEC-12's negative test exists specifically to prove the boundary is real, not merely asserted in code |
| Detecting Vault unseal state before every operation | A custom "is Vault ready" poller | Vault's own `/v1/sys/health` endpoint + `hvac.Client.sys.is_sealed()` | Already exists, already correct, already what `hvac` wraps |
| Rendering the audit log for human consumption | A bespoke JSON-log parser reinventing HMAC-awareness | A thin `jq`-based formatter over Vault's own documented JSON schema | Vault's audit format is documented and stable (`type`, `time`, `auth`, `request`, `response` top-level keys); the HMAC-hashing of sensitive fields is Vault's own guarantee, not something a custom parser should try to re-derive or, worse, accidentally defeat |

**Key insight:** every "don't hand-roll" item in this phase is really the same insight stated four ways: Vault's entire value proposition is that the trust-boundary logic (authentication, authorization, audit) lives in one well-reviewed place instead of scattered across application code. Reimplementing any fragment of it — even a small one, even "just for local dev" — reintroduces exactly the risk profile Vault exists to remove, and directly undermines SEC-01's "only source of runtime credentials" claim.

## Runtime State Inventory

> This phase migrates existing credential-delivery state (Kubernetes Secrets → Vault), which is the same "what still points at the old mechanism after the code changes" question the standard Runtime State Inventory targets, scoped to this phase's actual migration surface rather than a full rename audit.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Live service config (Kubernetes Secrets, D-01's literal migration targets) | `csv-processor-db` (namespace `etl`, key `dsn`); `csv-processor-s3` (namespace `etl`, keys `access_key`/`secret_key`); `airflow-minio-connection` (namespace `airflow`, key `AIRFLOW_CONN_MINIO_DEFAULT`) — all three created by `scripts/etl-secrets.sh`, confirmed live this session | Delete or empty each, ONE AT A TIME, only after its Vault-backed replacement is confirmed working end-to-end (D-01's explicit sequencing rule) — never a batch cleanup |
| Live service config (Helm values wiring) | `helm/values/{local,ci}/airflow.yaml`'s `secretKeyRef: {name: airflow-minio-connection, ...}` env wiring on `scheduler`/`triggerer`/`workers.kubernetes` (confirmed present via 04-02-PLAN.md's Task 2, not yet re-read verbatim from the live file but structurally required by the DAG's live use of `aws_conn_id="minio_default"`) | Remove once `VaultBackend` serves `minio_default` directly — this is a code edit (values file), not a data migration |
| Code (env-var construction) | `airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()` — currently builds `k8s.V1EnvVar(..., value_from=k8s.V1EnvVarSource(secret_key_ref=...))` for `DATAPLAT_DB_DSN`/`DATAPLAT_S3_ACCESS_KEY`/`DATAPLAT_S3_SECRET_KEY` (confirmed live, read verbatim this session) | Change to `k8s.V1EnvVar(name=..., value="vault://etl/...#...")` — a literal string holding an opaque reference, not a `secretKeyRef`. This is the ONE call site this phase touches outside `resolver.py` itself, and it is unavoidable: the env var's SOURCE mechanism is inherently pod-spec-level, not resolver-level |
| Secrets/env vars (names, unaffected) | `DATAPLAT_DB_DSN`, `DATAPLAT_S3_ACCESS_KEY`, `DATAPLAT_S3_SECRET_KEY`, `DATAPLAT_S3_ENDPOINT_URL`, `AIRFLOW_CONN_MINIO_DEFAULT` — the NAMES stay identical; only the value-population mechanism changes | None — `_build_common()`'s `_DB_DSN_REF = "env://DATAPLAT_DB_DSN"` style constants need no changes at all, since they already only name the env var, never its source |
| Stored data | Nothing found — no database stores "which secret backend served this row" as a key, collection name, or identity | None — verified by reading `meta.ingestion_runs`' column list (ARCHITECTURE.md §2.1); no column encodes secret-delivery mechanism |
| OS-registered state | Nothing found — no Task Scheduler/pm2/launchd/systemd unit involved anywhere in this stack | None — this project runs entirely inside the kind cluster and host-side Make targets; no OS-level service registration exists to carry stale state |
| Build artifacts / installed packages | Nothing found — no compiled binary or installed package name embeds "how secrets are delivered" | None — `hvac`'s addition is a pure dependency-list change (`pyproject.toml` + `uv.lock`), not a rename of anything already built |

**The canonical question, answered for this phase:** after the Vault-backed paths are proven working and the code is switched over, three Kubernetes Secrets and one block of `secretKeyRef` Helm-values wiring are the only runtime state that still needs deleting. Nothing else in the cluster, the metadata DB, or the host filesystem caches or references the old delivery mechanism by name.

## Common Pitfalls

### Pitfall 1: Which Airflow ServiceAccount actually presents to Vault is genuinely unverified

**What goes wrong:** Binding the `airflow` Vault role's `bound_service_account_names` to a guessed single ServiceAccount (e.g., only `airflow-scheduler`) works in some test but fails for a different code path — most plausibly, this project's own live `S3KeySensor(deferrable=True)`, whose connection resolution happens somewhere in the scheduler → triggerer → (possibly) api-server chain, and Airflow 3's Task-SDK DB-isolation redesign may have moved where a plain `Hook`/`Connection` lookup actually executes compared to Airflow 2 folklore.

**Why it happens:** This project's own live Airflow ServiceAccount list (confirmed this session: `airflow-api-server, airflow-create-user-job, airflow-dag-processor, airflow-migrate-database-job, airflow-scheduler, airflow-triggerer, airflow-worker, default`) has five plausible candidates, and Airflow 3's documented architecture ("API server is the sole metadata-DB access point for tasks and workers") does not, on its own, settle whether a *secrets-backend* lookup (as opposed to a metadata-DB query) is proxied through the API server the same way.

**How to avoid:** Do not guess. Bind the role to the SINGLE most likely candidate first (`airflow-api-server`, per the "sole access point" principle), attempt the SEC-05 acceptance test (delete the connection, unset `AIRFLOW_CONN_MINIO_DEFAULT`, trigger the DAG), and read the Vault audit log this phase already builds (D-04) — a DENIED login attempt from a DIFFERENT ServiceAccount is not a bug, it is the answer to this exact question, discovered the same way 04-02-PLAN.md discovered its own two-subject RoleBinding requirement (by reading the live cluster, not by assuming). Document whichever SA(s) actually appear in the audit log as the `bound_service_account_names` list, with a one-line comment recording how it was determined — mirroring `kubernetes/rbac-etl.yaml`'s own header-comment discipline.

**Warning signs:** A Vault audit-log entry showing `"errors":["service account name not authorized"]` for a ServiceAccount that is not `default` (i.e., a LEGITIMATE Airflow component being denied, not the intentional SEC-12 negative test).

### Pitfall 2: `kubectl logs` is not in this repo's permitted-kubectl set

**What goes wrong:** A first draft of `make vault-audit-tail` (D-04) shells out to `kubectl logs -f vault-0` (matching HashiCorp's own documented Kubernetes recommendation of `file_path=stdout`) and fails CI's `tests/policy/test_no_manual_kubectl_surgery.py`, since `logs` is not in `_PERMITTED_READ_ONLY_SUBCOMMANDS = frozenset({"get", "wait"})`.

**Why it happens:** That policy test was written and last extended (04-02) against this project's own prior needs, none of which needed `kubectl logs`. It is read-only and cannot mutate cluster state, but the test's current permitted set does not yet reflect that.

**How to avoid:** Prefer a persistent audit-log file (`server.auditStorage.enabled: true`, `vault audit enable file file_path=/vault/audit/audit.log`) read via `kubectl exec -i vault-0 -n vault -- tail -n 200 /vault/audit/audit.log` — this fits the ALREADY-PERMITTED `exec -i` pattern with zero test changes, and gains audit-log durability across pod restarts as a side benefit (aligned with D-02's "real persistent storage" philosophy). If `file_path=stdout` is preferred instead (simpler chart config, no `auditStorage` volume), extend `_PERMITTED_READ_ONLY_SUBCOMMANDS` to include `"logs"` as a small, well-justified, single-line, documented widening — exactly the same shape 04-02 already used twice for `apply -f -` and `exec -i`.

**Warning signs:** `test_no_script_performs_manual_kubectl_surgery` failing in CI the moment `vault-audit-tail.sh`/`.py` is added, with a message naming `kubectl logs`.

### Pitfall 3: `AIRFLOW__SECRETS__USE_CACHE` interacts with D-03's rotation proof in a way that is easy to get backwards

**What goes wrong:** Someone enables `AIRFLOW__SECRETS__USE_CACHE=true` for performance (a reasonable instinct — every Connection lookup is otherwise a live Vault network call) and D-03's rotation test starts failing intermittently, or passing only because it happened to run after the cache TTL expired.

**Why it happens:** `AIRFLOW__SECRETS__USE_CACHE` defaults to `False` (`conf.getboolean(section="secrets", key="use_cache", fallback=False)`, verified via search of Airflow's own `airflow.secrets.cache` module and confirmed default-off), with `AIRFLOW__SECRETS__CACHE_TTL_SECONDS` defaulting to 900 (15 minutes) when enabled. Since it is OFF by default, Airflow's own Connection resolution is ALREADY a live, uncached read on every lookup — which is actually the simplest possible foundation for D-03's proof, requiring zero new configuration. Turning caching ON for a later performance need reintroduces exactly the rotation-lag PITFALLS D5 already warns about.

**How to avoid:** Leave `use_cache` at its default (`False`) for this phase; document explicitly (SEC-09) that this is a deliberate choice trading a small amount of Vault load for zero rotation lag, and that enabling the cache later is a legitimate but consequential change that must update the rotation-lag documentation to say "up to `cache_ttl_seconds`" instead of "immediate." A residual nuance flagged at MEDIUM confidence (not independently verified against Airflow 3.3.0's Task-SDK code path this session — an open GitHub issue, `apache/airflow#48833` "Port SecretCache to task sdk," suggests this caching layer's wiring into the new Task-SDK execution model may itself be in flux): confirm empirically during the phase's own acceptance test rather than assuming the cache is inert just because it defaults off.

**Warning signs:** The rotation test (`tests/e2e/vault/test_rotation.py`) passing on some runs and failing on others with no code change — a classic caching-TTL symptom, not a Vault bug.

### Pitfall 4: The Vault standalone listener ships with TLS disabled by default

**What goes wrong:** The chart's own default `server.standalone.config` (verified verbatim against the pulled `values.yaml`) is:
```
listener "tcp" {
  tls_disable = 1
  address = "[::]:8200"
  cluster_address = "[::]:8201"
}
```
Every credential this phase moves through Vault — including the Kubernetes ServiceAccount JWT used to authenticate — travels as plaintext HTTP inside the cluster network unless this default is overridden, and neither STACK.md nor ARCHITECTURE.md's Vault sections address this explicitly.

**Why it happens:** `tls_disable = 1` is the chart's own out-of-the-box default, presumably chosen for the same reason most local quickstarts default it off — needing a CA and cert-rotation story is real friction for a first deployment.

**How to avoid:** This is a genuine, previously-undocumented decision point for this phase, not something this research can resolve on the planner's behalf — flagged in Open Questions and the Assumptions Log below. At minimum, SEC-14's end-to-end documentation should state explicitly whether TLS was enabled or deliberately deferred, and why, exactly as D-02 already models for the unseal ceremony ("local-dev-only convenience... a real deployment would..."). This is squarely a security-posture decision, not a research fact — see Security Domain.

**Warning signs:** None observable at runtime — this is a configuration-review finding, not a failure mode with symptoms. That is precisely why it needs documenting deliberately rather than left to be discovered later.

### Pitfall 5 (carried forward from PITFALLS.md, now cluster-confirmed): WSL2 clock drift produces auth failures indistinguishable from a real Vault misconfiguration

**What goes wrong:** After the WSL2 host sleeps and resumes, the guest clock lags wall time; every time-sensitive credential in this phase's path (the bound ServiceAccount token's `iat`/`exp`/`nbf`, Vault's own lease clocks) starts producing `permission denied` on tokens that are, in fact, valid.

**How to avoid:** `date` inside WSL vs. the host, before debugging anything Vault-related, exactly as PITFALLS A7/ROADMAP's own plan guidance already instructs. Not re-verified independently this session (no clock-drift condition was present during this research), but the underlying mechanism (bound-token time-window validation) is a standard, well-documented Kubernetes feature, so the pitfall's mechanics are HIGH confidence even without reproducing the failure.

## Code Examples

### Vault Kubernetes-auth role + policy — `csv-processor` (ETL side)

```hcl
# Source: pattern verified against STACK.md §E's already-locked example (mount_point="etl")
# combined with the exact role-field names confirmed via developer.hashicorp.com/vault/api-docs/auth/kubernetes
# (bound_service_account_names, bound_service_account_namespaces, token_policies, token_ttl).
vault write auth/kubernetes/role/csv-processor \
    bound_service_account_names=csv-processor \
    bound_service_account_namespaces=etl \
    token_policies=csv-processor \
    token_ttl=20m \
    token_max_ttl=1h

vault policy write csv-processor - <<'EOF'
path "etl/data/analytics-db" { capabilities = ["read"] }
path "etl/data/minio"        { capabilities = ["read"] }
EOF
```

### The negative test — exact mechanics (SEC-12)

```python
# Source: error message verified via hashicorp/vault-plugin-auth-kubernetes GitHub issues
# and HashiCorp support articles (this session's WebSearch); hvac's own exception hierarchy
# (hvac.exceptions.VaultError as the safe common base to catch, since the precise HTTP status
# for this specific failure was not independently confirmed this session — see Assumptions Log).
import hvac
import pytest

def test_default_service_account_is_denied_csv_processor_role(default_sa_jwt: str, vault_addr: str) -> None:
    """SEC-12: an unmatched ServiceAccount's login must fail closed.

    `default_sa_jwt` is the projected token for ServiceAccount `default` in
    namespace `etl` — NOT `csv-processor`. Vault's role `csv-processor` has
    `bound_service_account_names=csv-processor`, so this JWT's identity does
    not match, and the LOGIN ITSELF must fail — no client token is ever
    issued, so there is nothing to even attempt a KV read with.
    """
    client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        client.auth.kubernetes.login(role="csv-processor", jwt=default_sa_jwt)
```

### D-03's rotation proof — the honest framing

```python
# Source: reasoned from cli.py's confirmed "_build_common() called once per pod" behavior
# (read verbatim this session) + AIRFLOW__SECRETS__USE_CACHE's confirmed default-False.
#
# IMPORTANT: for the ETL-pod tier, "no restart required" is trivially true because KPO pods
# are one-shot processes -- there is no "restart" of an existing pod to demonstrate against.
# The MEANINGFUL proof of "an already-running process picks up a rotated value" is on
# Airflow's own long-running side (scheduler/triggerer/api-server), where every Connection
# lookup is already a live, uncached Vault read by default.
def test_rotation_reflected_without_restart(vault_client, airflow_cli) -> None:
    old_value = airflow_cli.get_connection("minio_default")
    vault_client.secrets.kv.v2.create_or_update_secret(
        mount_point="airflow", path="connections/minio_default",
        secret={"conn_uri": "<new-uri-with-rotated-credential>"},
    )
    new_value = airflow_cli.get_connection("minio_default")  # SAME running Airflow process
    assert new_value != old_value  # picked up live, no pod restart in between
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|---------------|--------|
| Vault Kubernetes auth requiring explicit `issuer` config to match the cluster's OIDC discovery document | `disable_iss_validation=true` by default; TokenReview API performs the equivalent check | Vault 1.9.0 | The kind-specific caveat this research was explicitly invoked to verify does not apply to this project's pinned Vault major (2.x) — confirmed against both the live cluster and current official docs |
| `apache-airflow-providers-hashicorp` docs page showing only `aws_iam`/`jwt` auth types | `auth_type: kubernetes` fully supported in code (verified at the exact running version, 4.7.1) | Present in the provider for multiple releases; docs page has simply never been updated to list it | Costs nothing once known — but trusting the docs page over the source, as CLAUDE.md itself already warned, would have cost real time |
| Vault chart `0.34.0` (STACK.md's pin, researched 2026-08-11) | `0.34.1` / server `2.0.4` now latest (checked 2026-08-14, three days later) | Routine patch releases | LOW impact — a patch bump, not a behavior change this research identified; verify no regressions before moving off the locked `0.34.0` pin, but no evidence exists that it is required |

**Deprecated/outdated:** ARCHITECTURE.md's Vault Agent Injector diagram (§9.1) — superseded by STACK.md's two-tier direct-login pattern, itself already locked. This research's corrected diagram (Architecture Patterns, above) should be treated as the current reference for this phase; the original diagram remains useful only as a record of the alternative that was considered and rejected.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|----------------|
| A1 | The exact HTTP status code (400 vs 403) for a Vault Kubernetes-auth login denied due to SA mismatch was not independently confirmed against this project's exact pinned Vault version (2.0.3) — only the error MESSAGE (`"service account name not authorized"`) and the general 403-class behavior were confirmed via community reports and HashiCorp support articles, not a first-party API reference table | Common Pitfalls / Code Examples (negative test) | LOW — the recommended test catches the broad `hvac.exceptions.VaultError`, which is correct regardless of the exact status code; a test asserting a SPECIFIC hvac exception subclass could need adjustment once run against the real cluster |
| A2 | Which Airflow ServiceAccount(s) actually perform the `VaultBackend` login for this project's live DAG (deferred `S3KeySensor`) is unresolved — Pitfall 1 documents the uncertainty and the recommended empirical resolution method, but no test was run this session (no Vault deployment exists yet to test against) | Architecture Patterns (diagram), Common Pitfalls #1 | MEDIUM — if the planner binds the Vault role to the wrong SA and does not follow the "read the audit log, don't guess" prevention, SEC-05's acceptance test will fail in a way that could be misdiagnosed as a Vault config bug rather than an identity-binding gap |
| A3 | Whether Vault's standalone listener should have TLS enabled for this project, or whether the chart's `tls_disable=1` default is deliberately accepted for local dev, was not decided by prior research (STACK.md/ARCHITECTURE.md are both silent on it) and is not decided here — flagged as a genuinely new, previously-undiscovered decision point | Common Pitfalls #4, Security Domain | MEDIUM — accepting the default silently (rather than as a documented, deliberate choice) would be inconsistent with this project's own stated bar for how D-02 treats other local-dev-only conveniences |
| A4 | `AIRFLOW__SECRETS__USE_CACHE`'s exact interaction with Airflow 3.3.0's Task-SDK execution model (as opposed to the classic pre-3.0 execution model the cache class was originally built for) was not independently verified — the config key/default were confirmed via search of official docs and the `airflow.secrets.cache` module, but a live open GitHub issue (`apache/airflow#48833`) suggests the caching layer's Task-SDK wiring may still be evolving | Common Pitfalls #3 | LOW — the RECOMMENDATION (leave `use_cache` at its default `False`) is safe regardless of how this resolves, since it avoids the ambiguity entirely rather than depending on its answer |
| A5 | The recommendation to implement Vault bootstrap in Python via `hvac` rather than the `vault` CLI binary is this research's own judgment, not a verified fact about which approach the project will actually prefer | Standard Stack (Supporting) | LOW — both are legitimate; a planner choosing the CLI approach loses none of this research's other findings, and the CLI-binary pinning pattern (`tools/k8s/install_*.sh`) is itself already fully established in this repo if chosen instead |

## Open Questions (RESOLVED — see inline markers below; all three settled by this phase's own plan set)

1. **Which Airflow ServiceAccount(s) must the `airflow` Vault role bind to?** **(RESOLVED — see 05-03-PLAN.md Task 2's empirical audit-log observation-and-correction step: the binding is determined by reading the live Vault audit log during the SEC-05 acceptance test, never guessed, per this question's own Recommendation below)**
   - What we know: Five candidate ServiceAccounts exist live (`airflow-api-server`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`, `airflow-worker`); Airflow 3's documented architecture makes the API server "the sole metadata-DB access point for tasks and workers," which is suggestive but not confirmed to extend to secrets-backend lookups specifically.
   - What's unclear: Whether a `VaultBackend` lookup triggered by a deferred sensor's trigger-side connection resolution (this project's live `S3KeySensor`) executes inside the triggerer process directly, or is itself proxied through the API server.
   - Recommendation: Bind to `airflow-api-server` first (best single guess); use the audit log this phase already builds (D-04) to observe the actual identity attempting login during the SEC-05 acceptance test; widen the binding only to what is empirically observed, with each addition individually justified in a comment — never to a wildcard.

2. **Should Vault's listener run with TLS enabled for this project's local-dev deployment?** **(RESOLVED — see 05-01-PLAN.md Task 1's T-05-05 accept-disposition header comment [`tls_disable = 1` accepted, argued not silent] and 05-05-PLAN.md Task 2's SEC-14 documentation of the production-substitution requirement)**
   - What we know: The chart defaults to `tls_disable = 1`; neither prior research document nor this session's CONTEXT.md addresses it; SEC-14 requires documenting the production-substitution story regardless.
   - What's unclear: Whether accepting plaintext-internal-HTTP is an acceptable, deliberate local-dev trade-off (matching the spirit of D-02's unseal-ceremony reasoning) or whether this project's stated security bar (ASVS Level 1, `security_block_on: high`) treats it as a finding to remediate even locally, given that Vault is explicitly the trust boundary this whole phase exists to establish.
   - Recommendation: Treat this as a discussion-worthy decision point for the phase's own planning/discuss step (not silently defaulted), with the two options and their trade-offs stated exactly as D-02 already models.

3. **`vault` CLI binary vs. pure-`hvac`-Python bootstrap — which does the planner prefer?** **(RESOLVED — 05-01-PLAN.md Task 2 implements the hvac-Python bootstrap/unseal approach throughout this phase's plan set; no `vault` CLI binary is introduced)**
   - What we know: Both are viable; this research recommends the Python approach for the reasons in Standard Stack (Supporting), but the CLI-binary pattern is equally well-precedented in this repo.
   - What's unclear: Nothing technical — this is a style/maintainability preference, not a research gap.
   - Recommendation: Default to the `hvac`-Python approach unless the planner has a specific reason to prefer the CLI (e.g., wanting `vault` available interactively for manual debugging, which is a legitimate reason on its own).

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| kind cluster (live) | The entire phase — Vault deploys into it | Yes | `kind-airflow-platform`, server v1.35.5, control-plane + verified namespaces (`airflow`, `data`, `etl`, `cnpg-system`, `ingress-nginx`) all Active | — |
| `helm` (pinned binary) | Vault chart install | Yes | `v4.2.3+g43e8b7f` at `tools/bin/helm`, confirmed working (`helm list -A` succeeds against the live cluster) | — |
| `kubectl` | All bootstrap/verification scripts | Yes | Client v1.36.1 / Server v1.35.5 (within supported ±1 minor skew) | — |
| Local container registry | Not directly needed by Vault itself (chart pulls from Docker Hub/`hashicorp/vault`), but confirms the general image-pull path this phase inherits | Yes | `localhost:5001`, reachable, `csv-processor` repository present | — |
| `vault` CLI binary | Only if the CLI-based bootstrap approach is chosen over the `hvac`-Python recommendation | No | — | Use the recommended `hvac`-based Python bootstrap instead (no new binary needed), or install via the established `tools/k8s/install_*.sh` pinned-binary pattern if the CLI is preferred |
| `hvac` (Python package) | `dataplat`'s new `vault://` scheme; already present in the Airflow image | Partially — present in the live Airflow image, NOT YET present in `packages/dataplat/pyproject.toml`'s own dependency list | `2.4.0` (confirmed both live and on PyPI) | `uv lock && uv sync` after adding it to `pyproject.toml` — no fallback needed, this is a one-line addition |
| `apache-airflow-providers-hashicorp` | Airflow's own `VaultBackend` | Yes — already installed | `4.7.1` (running), matches the constraints-file pin | — |

**Missing dependencies with no fallback:** none.

**Missing dependencies with fallback:** `vault` CLI binary (fallback: `hvac`-based Python bootstrap, recommended anyway).

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest `9.1.1` (`[tool.pytest.ini_options]`, `pyproject.toml`) |
| Config file | `pyproject.toml` — `testpaths = ["tests"]`, markers `slow`/`regression`/`cluster`/`manifests` already defined; no new marker strictly required, but `cluster` already exists and fits a live-Vault-cluster test perfectly |
| Quick run command | `make check` (offline: unit + regression + lint + typecheck; does NOT touch a live cluster) |
| Full suite command | `make check && make test-integration && make cluster-verify` (existing) `+ make vault-verify` (NEW target this phase adds, following `cluster-verify`'s exact `RUN_CLUSTER`/live-cluster pattern) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|---------------------|-------------|
| INFRA-06 | Vault survives cluster restart, unseal restores service | e2e (cluster) | `pytest tests/e2e/vault/test_unseal_survives_restart.py -x` | ❌ Wave 0 |
| SEC-01 | Old Secrets removed after Vault path confirmed | policy + manual verification | `pytest tests/policy/test_no_stale_secrets.py -x` (extends `test_workflow_secrets.py`'s D-14 pattern) | ❌ Wave 0 |
| SEC-03 | No credential in Python source | policy | Existing `gitleaks`/`test_workflow_secrets.py` machinery — no new test needed, `vault://` references are opaque strings by design | ✅ (existing gate covers new code automatically) |
| SEC-04 | No secret baked into image | policy/CI | Existing `trivy image --scanners secret` (CLAUDE.md I. CI/CD) — no new test needed | ✅ |
| SEC-05 | Airflow resolves connections via Vault, proven by deletion | e2e (cluster) | `pytest tests/e2e/vault/test_airflow_backend.py -x` | ❌ Wave 0 |
| SEC-06 / SEC-07 | Least-privilege identity match | e2e (cluster) | `pytest tests/e2e/vault/test_positive_auth.py -x` | ❌ Wave 0 |
| SEC-08 | Auditable, no secret values logged | e2e (cluster) | `pytest tests/e2e/vault/test_audit_log.py -x` | ❌ Wave 0 |
| SEC-09 | Rotation documented + proven | e2e (cluster) | `pytest tests/e2e/vault/test_rotation.py -x` | ❌ Wave 0 |
| SEC-12 | Negative test: `default` SA denied | e2e (cluster) | `pytest tests/e2e/vault/test_negative_auth.py -x` | ❌ Wave 0 |
| SEC-13 | Dev secrets marked/isolated/reproducible | e2e (cluster) + manual (`cluster-rebuild` re-run) | `pytest tests/e2e/vault/test_dev_secrets_reproducible.py -x` | ❌ Wave 0 |
| SEC-14 | Documented end-to-end | manual-only (documentation review) | N/A — `docs/` review, not automatable | N/A |

### Sampling Rate

- **Per task commit:** `make check` (fast, offline; catches resolver.py unit-test regressions immediately)
- **Per wave merge:** `make check && make test-integration && make vault-verify` (full live-cluster proof)
- **Phase gate:** Full suite green before `/gsd:verify-work`, exactly as established for Phases 2–4

### Wave 0 Gaps

- [ ] `tests/e2e/vault/__init__.py` + `tests/e2e/vault/conftest.py` — mirror `tests/e2e/cluster/conftest.py`'s `_require_cluster` pattern exactly
- [ ] `tests/e2e/vault/test_positive_auth.py`, `test_negative_auth.py`, `test_airflow_backend.py`, `test_audit_log.py`, `test_rotation.py`, `test_unseal_survives_restart.py`, `test_dev_secrets_reproducible.py` — all new
- [ ] `tests/unit/test_secrets_resolver.py` extended with `vault://` success/failure cases using a mocked `hvac.Client` (the existing `test_vault_scheme_fails_closed_rather_than_passing_through` test asserts the PRE-Phase-5 behavior and must be updated/replaced once `vault://` becomes a real scheme, not merely a rejected one)
- [ ] `Makefile`: `vault-verify` target (mirrors `cluster-verify`'s shape), `vault-bootstrap`, `vault-unseal`, `vault-audit-tail` targets
- [ ] Framework install: none — pytest/hvac/mocking libraries are all already available or being added as the phase's own dependency

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|--------------------|
| V2 Authentication | Yes | Vault's Kubernetes auth method (TokenReview-backed) — never a hand-rolled JWT validator; workload identity, not user identity, but the same "never build your own" principle applies |
| V3 Session Management | Yes (workload-token analogue) | Vault token TTL/lease (`token_ttl=20m`, `token_max_ttl=1h`, per Code Examples) — short-lived, non-renewable-beyond-max tokens for both roles |
| V4 Access Control | Yes — the core of this phase | Vault policies (`path "..." { capabilities = [...] }`) + `bound_service_account_names`/`_namespaces`, enforced server-side, proven by SEC-12's negative test |
| V5 Input Validation | Yes | `vault://` scheme parsing via stdlib `urlsplit()` (already the established pattern for `env://`/`file://`) with explicit malformed-reference rejection, matching the existing fail-closed design |
| V6 Cryptography | Yes | Vault's own Shamir's Secret Sharing unseal mechanism (never hand-rolled); audit-log field hashing is Vault's own HMAC-SHA256, never a custom redaction scheme |
| V9 Communications (adjacent) | Open question — see Open Questions #2 | The chart's `tls_disable=1` default for the standalone listener means internal cluster traffic (including the ServiceAccount JWT presented at login) is plaintext HTTP unless explicitly overridden — flagged, not silently accepted |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|------------------------|
| A workload widening its own Vault role to `bound_service_account_names: ["*"]` to "fix" a permission-denied | Elevation of Privilege | Never widen to fix an auth failure — diagnose via the audit log (this phase's own D-04 tooling) and bind to the actual observed identity, one name at a time, each justified in a comment (PITFALLS B5/D3, reinforced by Common Pitfalls #1 above) |
| Dev-mode Vault (`server.dev.enabled: true`) used outside CI, with its literal `root` token | Elevation of Privilege / Information Disclosure | `server.dev.enabled: false` locally (already locked in STACK.md); dev mode reserved for CI only, where it never persists and never holds a real credential |
| An unauthorized ServiceAccount attempting to read another workload's KV path | Elevation of Privilege | Vault policy `capabilities` scoped per mount/path; proven false by SEC-12's negative test, not merely asserted |
| A secret value leaking into the audit log | Information Disclosure | Vault's default HMAC-SHA256 hashing of string values (verified via official docs this session) — never set `log_raw = true` |
| Plaintext credential interception on the cluster-internal network | Information Disclosure | Open question (V9 above) — TLS-vs-plaintext for the Vault listener needs an explicit, documented decision, not a silent default |
| A stale Kubernetes Secret left readable after its Vault-backed replacement goes live | Information Disclosure | D-01's explicit "delete or empty, one at a time, only after confirmed working" sequencing (Runtime State Inventory, above) |

## Sources

### Primary (HIGH confidence)

- Live cluster, this session — `kubectl get --raw /.well-known/openid-configuration`, `kubectl -n kube-system get pod -l component=kube-apiserver`, `kubectl -n airflow exec deploy/airflow-scheduler -- pip list`, `kubectl get ns`, `helm list -A`, `kubectl -n airflow get pods -o jsonpath=...` — the actual `kind-airflow-platform` cluster, Airflow image contents, and namespace/RBAC state
- `raw.githubusercontent.com/apache/airflow/providers-hashicorp/4.7.1/.../vault_client.py` — exact source at the ACTUAL running provider version (corrected from 4.8.0)
- `developer.hashicorp.com/vault/docs/auth/kubernetes` — `disable_iss_validation` default-since-1.9 confirmation, `issuer` parameter semantics
- `developer.hashicorp.com/vault/api-docs/auth/kubernetes` — role field definitions (`bound_service_account_names`, `bound_service_account_namespaces`, `token_policies`, `token_ttl`)
- `raw.githubusercontent.com/hashicorp/vault-helm/v0.34.0/values.yaml` and `templates/server-clusterrolebinding.yaml` — exact chart defaults (`dev.enabled: false`, `standalone.config` with `tls_disable=1`, `dataStorage.enabled: true, size: 10Gi`, `auditStorage.enabled: false`, `authDelegator.enabled: true`, `csi.enabled: false`, `injector.enabled: "-"`)
- `developer.hashicorp.com/vault/docs/audit` (fetched via search) — audit log JSON format, HMAC-SHA256 default hashing of string values
- `pip index versions hvac`, `slopcheck install hvac` — direct registry + legitimacy verification
- Existing repository source, read directly this session: `packages/dataplat/src/dataplat/secrets/resolver.py`, `packages/dataplat/src/dataplat/errors.py`, `packages/csv-processor/src/csv_processor/cli.py`, `airflow/dags/_common/kpo.py`, `airflow/dags/csv_ingest_customers.py`, `scripts/etl-secrets.sh`, `kubernetes/rbac-etl.yaml`, `tests/policy/test_no_manual_kubectl_surgery.py`, `tests/unit/test_secrets_resolver.py`, `docker/csv-processor/Dockerfile`, `.planning/phases/04-.../04-02-PLAN.md`, `helm/versions.env`, `pyproject.toml`, `.planning/config.json`

### Secondary (MEDIUM confidence)

- WebSearch: `AIRFLOW__SECRETS__USE_CACHE`/`CACHE_TTL_SECONDS` defaults, cross-referenced against `airflow.secrets.cache` module documentation and a live open GitHub issue (`apache/airflow#48833`) about Task-SDK porting status
- WebSearch: Vault Kubernetes-auth SA-mismatch error message (`"service account name not authorized"`), corroborated across multiple `hashicorp/vault-plugin-auth-kubernetes` GitHub issues and HashiCorp support articles — message text confirmed, exact HTTP status code not first-party-confirmed (see Assumptions Log A1)
- WebSearch: `file_path=stdout` for Vault's file audit device in Kubernetes, corroborated across a HashiCorp support article and independent blog posts

### Tertiary (LOW confidence)

- None — every claim in this document traces to either a live-cluster empirical check, a directly-fetched official source, or a corroborated multi-source WebSearch result. Items that could not clear that bar are listed in the Assumptions Log instead of stated as fact.

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — every version claim independently re-verified this session (live cluster, PyPI, GitHub releases API), not merely carried forward from prior research
- Architecture: HIGH for the corrected secrets-flow diagram and `vault://` scheme design (grounded in actually-read source code); MEDIUM for the "which Airflow SA" question, honestly flagged as unresolved pending empirical testing
- Pitfalls: HIGH for the two explicitly-requested verification questions (kind issuer, provider source) and the TLS-default finding (all directly confirmed against primary sources); MEDIUM for the SecretCache/Task-SDK interaction (config confirmed, exact execution-model wiring not independently verified)

**Research date:** 2026-08-14
**Valid until:** ~14 days for the Vault-specific version pins (chart/server patch cadence observed at 3 days between STACK.md's research and this session — treat as fast-moving); ~30 days for the architectural/design findings (provider source, chart structure, audit format are all stable APIs unlikely to change on that timescale)
