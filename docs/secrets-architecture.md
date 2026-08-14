# Secrets architecture

**Status: live, on the `kind-airflow-platform` cluster.** Every claim below traces to a
specific PLAN.md, SUMMARY.md, or automated test that proved it — Phase 5's own STRIDE
threat register (T-05-12) treats an overstated claim in this document as a threat to
this platform's Core Value (traceable, verifiable, trusted), so nothing here is stated
without a citation to what actually proved it.

This document covers, in order: the injection mechanism, trust boundaries, what
credential lives where, rotation, audit, and how a production secrets manager would
substitute without an application code change.

## 1. Injection mechanism

**Every credential in this codebase is an opaque reference string, never a raw
value, from the point it is created to the point it is resolved.**
`packages/dataplat/src/dataplat/secrets/resolver.py`'s `resolve_secret(ref: str) ->
str` is the single interpretation point (SEC-15, ADR-0002/0008's own composition-seam
discipline). A call site holds `"vault://etl/analytics-db#dsn"` or
`"env://DATAPLAT_S3_ENDPOINT_URL"`; it never learns which backend served the value.

Two workload identities authenticate to Vault, and both use the **same mechanism**:
**direct ServiceAccount-token login** (`hvac` + Vault's Kubernetes auth method) — never
the Vault Agent Injector, the Secrets Store CSI driver, the Vault Secrets Operator
(VSO), or External Secrets Operator (ESO). This is `.planning/research/STACK.md` §E's
locked, HIGH-confidence "two-tier pattern," chosen specifically because a sidecar on
every short-lived ETL pod doubles pod count for zero benefit, and because the secret
never lands in a Kubernetes Secret or an Injector-rendered file at all — the strongest
available answer to "no credential in a Kubernetes manifest" (§81).

### The `vault://` reference shape

```
vault://<mount>/<path>#<field>
```

Parsed with stdlib `urlsplit()` — the same minimal parser `env://`/`file://` already
use (`packages/dataplat/src/dataplat/secrets/resolver.py`, plan 05-02): `.netloc` is
the KV v2 mount, `.path.lstrip("/")` is the secret's path under that mount, `.fragment`
is the field name. `vault://etl/analytics-db#dsn` therefore reads the `dsn` field of
the secret at path `analytics-db` under mount `etl`. A malformed reference (missing
mount, path, or field) raises `SecretResolutionError` **before any network call** —
fail-closed by construction, proven by `tests/unit/test_secrets_resolver.py` (plan
05-02) against a mocked `hvac.Client`.

### `resolve_secret()`'s scheme dispatch

```python
if parsed.scheme == "vault":
    mount_point = parsed.netloc
    path = parsed.path.lstrip("/")
    field = parsed.fragment
    secret = _vault_client().secrets.kv.v2.read_secret_version(
        mount_point=mount_point, path=path,
    )
    return str(secret["data"]["data"][field])
```

`_vault_client()` is a **module-level cached, lazily-authenticated `hvac.Client`** —
authenticated once per process (one `auth/kubernetes/login` round trip), not once per
`resolve_secret()` call, since `csv_processor.cli._build_common()` resolves three
references in a row at pod start. It reads the pod's own projected ServiceAccount
token from its default, always-mounted path
(`/var/run/secrets/kubernetes.io/serviceaccount/token` — no extra volume needed) and
logs in with `role=os.environ["VAULT_K8S_ROLE"]`. `VAULT_ADDR`/`VAULT_K8S_ROLE` are
plain, non-secret configuration — set per-pod in
`airflow/dags/_common/kpo.py`'s `common_kpo_kwargs()`, the one call site plan 05-02
touched outside `resolver.py` itself (the env var's *source* mechanism is inherently
pod-spec-level, not resolver-level).

**A double-indirection worth naming explicitly:** `common_kpo_kwargs()` sets
`DATAPLAT_DB_DSN`/`DATAPLAT_S3_ACCESS_KEY`/`DATAPLAT_S3_SECRET_KEY` to `vault://...`
*literal string values* on the pod's env, and `csv_processor.cli._build_common()`
resolves them via `env://DATAPLAT_DB_DSN` first (reading the env var), then
`resolve_secret()` a **second** time on the value that read returns (resolving the
`vault://` reference it holds) — `resolve_secret()` is deliberately non-recursive, one
level per call, so composing two opaque layers is the caller's responsibility. This
was a real, previously-latent bug (plan 05-03: the first KPO pod to actually exercise
this path failed with `missing "=" after "vault://etl/analytics-db#dsn"` because only
one `resolve_secret()` call was made) — fixed and regression-tested
(`tests/unit/test_csv_processor_cli.py::test_build_common_resolves_vault_literals_held_inside_env_vars`).

### Airflow's side: the native `VaultBackend`, not a second bespoke integration

Airflow resolves its `minio_default` Connection through
`apache-airflow-providers-hashicorp`'s own `VaultBackend` — already installed in the
stock `apache/airflow:3.3.0-python3.12` image (`providers-hashicorp==4.7.1`,
`hvac==2.4.0`, confirmed live via `pip list` inside the running scheduler pod,
05-RESEARCH.md), so this platform adds zero new Python dependencies to the Airflow
image. Configured via `helm/values/{local,ci}/airflow.yaml`'s `config.secrets` block:

```yaml
config:
  secrets:
    backend: airflow.providers.hashicorp.secrets.vault.VaultBackend
    backend_kwargs: '{"url": "http://vault.vault.svc.cluster.local:8200", "auth_type": "kubernetes", "kubernetes_role": "airflow", "mount_point": "airflow", "connections_path": "connections", "variables_path": null, "config_path": null, "kv_engine_version": 2}'
```

Every key here was verified against the **actually installed** provider source at
`providers-hashicorp/4.7.1` (not the docs page, which omits `auth_type: kubernetes`
entirely — `.claude/CLAUDE.md` itself flags this doc/source mismatch). `backend_kwargs`
is deliberately a single-line JSON scalar, not a multi-line YAML block: the chart
embeds this string verbatim into the `airflow.cfg` ConfigMap's INI-rendered
`[secrets]` section with no re-indentation, and a multi-line `>-` block's
over-indented continuation lines broke that ConfigMap's own YAML structure (`helm
template` failed with `did not find expected key`) — found and fixed live in plan
05-03. `variables_path: null` is load-bearing, not decorative: `common_kpo_kwargs()`
calls `Variable.get("csv_processor_image")` at **DAG-parse time**, and routing
Variable lookups through Vault would put a network call on the DAG processor's
parse-time critical path for a value this phase never intended to move.

## 2. Trust boundaries

Consolidated from this phase's five PLAN.md `<threat_model>` blocks
(`.planning/phases/05-vault-secrets-workload-identity/05-0{1,2,3,4,5}-PLAN.md`):

| Boundary | Description | Source |
|---|---|---|
| Host developer kubeconfig → Vault admin API | `scripts/vault-unseal.py`/`scripts/vault-bootstrap.py` run host-side, reaching Vault only through a `kubectl port-forward` torn down on exit — the same trust tier `scripts/minio-credentials.sh`/`scripts/airflow-metadata-secret.sh` already use, never a new in-cluster identity | 05-01 |
| Vault root token (`.secrets/vault-init.json`, local disk) → Vault admin API | The one-time `sys.initialize()` output; `chmod 0o600`, gitignored, read only by `vault-bootstrap.py`, never logged or printed | 05-01 |
| ServiceAccount JWT (`csv-processor`, namespace `etl`) → `auth/kubernetes/login` | Validated by Vault via the Kubernetes TokenReview API, not by this codebase; role `csv-processor` is an exact, non-wildcard, single-identity bind | 05-02 |
| ServiceAccount JWT (four Airflow identities, namespace `airflow`) → `auth/kubernetes/login` | Same TokenReview validation; role `airflow` is bound to `airflow-api-server`, `airflow-triggerer`, `airflow-worker`, `airflow-scheduler` — each one individually, empirically justified (audit-log observation, live reproduction, or documented architectural necessity), never widened to a wildcard | 05-03 |
| `csv-processor` Vault policy → `etl/data/*` | Scoped to exactly `analytics-db` and `minio` under mount `etl`; cannot read `airflow/data/connections/*` | 05-02 |
| `airflow` Vault policy → `airflow/data/connections/*` | Scoped to the `connections/` sub-path of the `airflow` mount only; cannot read `etl/data/*` | 05-03 |
| **Vault's internal TCP listener (cluster-network) → all callers** | **`tls_disable = 1` (chart default, left unoverridden) — an explicit, argued acceptance for this LOCAL-DEV, ClusterIP-only, no-ingress Vault instance (STRIDE threat T-05-05, disposition: accept).** Every credential value AND every ServiceAccount JWT this phase moves through Vault travels as plaintext HTTP inside the cluster network. See §6 below — TLS is a hard requirement, not an accepted default, in any non-local deployment. | 05-01, **T-05-05** |
| `scripts/vault-audit-tail.py` → the persistent audit log file inside `vault-0` | Read-only, via the already-permitted `kubectl exec -i` pattern (`tests/policy/test_no_manual_kubectl_surgery.py`); never mutates cluster state | 05-04 |
| Test assertions → Vault's own HMAC redaction | Tests assert the *absence* of a plaintext substring in the audit log; they never attempt to validate or reverse the HMAC hash itself | 05-04 |

## 3. What is where

Every credential this phase migrated off a Kubernetes Secret, and which identity
reads it:

| Vault reference | Field | Consumer | Namespace / ServiceAccount(s) | Migrated from (D-01) |
|---|---|---|---|---|
| `vault://etl/analytics-db#dsn` | `dsn` | `csv_processor` CLI (KPO pod) | `etl` / `csv-processor` | Secret `csv-processor-db` (plan 05-02) |
| `vault://etl/minio#access_key` | `access_key` | `csv_processor` CLI (KPO pod) | `etl` / `csv-processor` | Secret `csv-processor-s3` (plan 05-02) |
| `vault://etl/minio#secret_key` | `secret_key` | `csv_processor` CLI (KPO pod) | `etl` / `csv-processor` | Secret `csv-processor-s3` (plan 05-02) |
| `airflow/connections/minio_default` (via `VaultBackend`) | `conn_uri` | Airflow's `minio_default` Connection | `airflow` / `airflow-api-server`, `airflow-triggerer`, `airflow-worker`, `airflow-scheduler` | Secret `airflow-minio-connection` (plan 05-03) |

All three of D-01's migration targets (`csv-processor-db`, `csv-processor-s3`,
`airflow-minio-connection`) are deleted from the live cluster, and
`scripts/etl-secrets.sh` — the only script that ever created them — is deleted
outright (plan 05-03). `tests/policy/test_no_stale_secrets.py` (plan 05-05) is the
permanent regression guard: none of the three may ever again appear as a Secret-
creation target under `scripts/**/*.sh` or `helm/values/{local,ci}/**/*.yaml`.

## 4. Rotation (SEC-09)

Two distinct read patterns, by design, backed by `tests/e2e/vault/test_rotation.py`
(plan 05-04, D-03):

- **KPO/ETL pods (`csv-processor` tier): resolved once per process, at pod start.**
  `csv_processor.cli._build_common()` calls `resolve_secret()` exactly once per
  credential when the pod starts. A short-lived KPO pod has no "restart" to
  demonstrate against — for this tier, **"rotation" means the next pod**: change the
  value in Vault, and the *next* `discover`/`ingest` task run reads it. There is no
  code change or redeploy required for a new pod to pick up a rotated value; there is
  also no way for an *already-running* pod to see one.
- **Airflow's long-running components (`airflow` tier): read live, per lookup, with
  no restart.** `AIRFLOW__SECRETS__USE_CACHE` defaults to `False` (confirmed against
  `airflow.secrets.cache` and the official docs), so `VaultBackend` is consulted on
  **every** `BaseHook.get_connection("minio_default")` call. `test_rotation.py` proves
  this live: a value rotated in Vault is observed by the **same already-running**
  `airflow-api-server` pod's very next CLI-driven connection lookup, with no pod
  deleted, restarted, or redeployed. (The rotation payload is a harmless appended
  query parameter rather than a functional field, verified live against the installed
  `apache-airflow-providers-amazon` provider to keep the connection fully usable by
  concurrent pipeline traffic throughout the test.)

Enabling `AIRFLOW__SECRETS__USE_CACHE=true` later (a reasonable performance instinct —
every Connection lookup is otherwise a live Vault round trip) is a legitimate future
change, but it reintroduces rotation lag up to `AIRFLOW__SECRETS__CACHE_TTL_SECONDS`
(default 900s) and must update this section accordingly; it is deliberately left off
for this milestone so "no restart required" stays unconditionally true rather than
"true within the cache TTL."

## 5. Audit (SEC-08)

Vault's `file` audit device is enabled at bootstrap (`scripts/vault-bootstrap.py`,
step (g)) against a **persistent, PVC-backed** log
(`/vault/audit/audit.log`) — durable across a `vault-0` pod restart, unlike the
alternative `file_path=stdout` pattern HashiCorp's own Kubernetes docs recommend
(which would require widening `tests/policy/test_no_manual_kubectl_surgery.py`'s
permitted `kubectl` subcommand set to include `logs`; the persistent-file approach
instead reuses the *already-permitted* `kubectl exec -i` pattern with zero policy
changes, 05-RESEARCH.md Pitfall 2).

**`make vault-audit-tail`** (`scripts/vault-audit-tail.py`, D-04) renders it for
humans — one compact line per entry (timestamp, request path, calling identity,
outcome) — matching the developer-experience bar `make ingest-demo` already set in
Phase 4. It **never reads a request/response body field at all**, only
`time`/`request.path`/`auth.metadata`/top-level `error` — the safest possible
mitigation against ever mis-rendering a hashed value is to never look at the field
that could carry one (plan 05-04's own STRIDE disposition, T-05-11).

Vault's default behavior — confirmed live, not merely assumed from docs — HMAC-SHA256
hashes every sensitive string value inside a logged request/response body.
`tests/e2e/vault/test_audit_log.py` (plan 05-04, SEC-08) proves both halves live
against this same log:

1. **Positive:** a successful `csv-processor` login and a denied `default`-
   ServiceAccount login both appear as distinct, attributable entries — "which
   workload, when, whether it succeeded" is observed in the log, not merely
   configured to be theoretically loggable.
2. **Negative (T-05-04):** the raw log text, searched as a whole, never contains the
   *current* plaintext value of the analytics DSN, the MinIO access key, the MinIO
   secret key, or the Airflow `minio_default` connection URI — all four read fresh
   immediately before the comparison, so this is not a check against a stale or
   assumed value. A non-vacuity control (a known-present string asserted present)
   proves the containment check itself is not silently always-true.

## 6. Production substitution (SEC-14)

Everything below is a **documented, deliberate local-dev convenience**, following the
same "name the shortcut, name the production alternative" discipline D-02 already
established for the unseal ceremony. None of these require an application code
change to substitute — every one is a Helm-values, chart-configuration, or
infrastructure change, because the credential-reference layer (`vault://mount/path
#field`, `resolve_secret()`) stays identical regardless of what sits behind it.

| This milestone's local-dev shortcut | Production substitute | Why the shortcut is safe here, and not there |
|---|---|---|
| **Unseal**: `scripts/vault-unseal.py`, a scripted single-share/threshold-1 init-or-unseal reading `.secrets/vault-init.json` (D-02) | **Auto-unseal** via a cloud KMS/transit backend (AWS KMS, GCP Cloud KMS, Azure Key Vault, or Vault's own `transit` auto-unseal), OR a genuine **multi-key-holder Shamir ceremony** (`n` shares, `t`-of-`n` threshold, each share held by a different operator) | This project runs on WSL2, where a cluster restart is a realistic every-morning event (`.planning/research/PITFALLS.md`); a real multi-operator ceremony is disproportionate daily friction for a single-developer local cluster, but is exactly the control a shared/production Vault needs — a single local key file is a single point of both convenience and compromise |
| **Listener**: `tls_disable = 1` (chart default, unoverridden — T-05-05, accepted) | **TLS required**, using a real CA (cert-manager + an internal or public CA) for both the listener and the ServiceAccount-JWT-bearing login requests | Plaintext HTTP inside a ClusterIP-only, no-ingress, single-developer kind cluster has no untrusted network segment between it and an attacker; any deployment with a shared network, multiple tenants, or any ingress path must not inherit this default silently |
| **Licence**: HashiCorp Vault `2.0.3`, BUSL-1.1 (source-available, not OSI-approved) | **[OpenBao](https://openbao.org)** — a Linux Foundation fork of Vault's last MPL-2.0 release, API-compatible with the Kubernetes auth method, KV v2 engine, and audit-device model this platform already uses | See [`docs/adr/0009-openbao-licence-escape-hatch.md`](adr/0009-openbao-licence-escape-hatch.md) for the full risk acceptance, the considered alternatives, and the named migration triggers (a licence-term change, an unpatched CVE past a stated window, or deployment outside local dev) |
| **Secret delivery**: direct SA-token login (`hvac` + Kubernetes auth), no sidecar, no CSI volume, no Kubernetes Secret ever created | **Vault Secrets Operator (VSO)**, if a long-lived Deployment (not a short-lived KPO pod) ever needs rotation-without-restart beyond what live per-lookup `VaultBackend` reads already provide | `.planning/research/STACK.md` §E's comparison table: VSO has the lowest Vault load and decouples secret lifecycle from pod lifecycle, at the cost of materializing a Kubernetes Secret (which §81.5 requires justifying in writing) — not needed today because Airflow's own components already read live, and KPO pods are short-lived by construction, but the documented next step if that assumption ever changes |
| **Bootstrap tooling**: `hvac`-based Python scripts (`scripts/vault-bootstrap.py`, `scripts/vault-unseal.py`), run by a human developer against a `kubectl port-forward` | Same tooling, pointed at a production Vault/OpenBao endpoint with real RBAC-gated operator access, OR a GitOps-managed `Cluster`/policy CRD pipeline if the fleet grows past a single cluster | The `hvac` HTTP client targets any server implementing Vault's API surface (including OpenBao) — no protocol-specific reimplementation exists to migrate |

**SEC-13's other half** (full reproducibility from a *completely fresh* cluster, not
just a re-run against an already-live one) was **partially proven live** by
gap-closure plan 05-06, after `05-VERIFICATION.md` and `05-REVIEW.md` independently
found that `scripts/vault-bootstrap.py`'s `_ensure_etl_secrets`/`_ensure_airflow_secrets`
sourced their values from three Kubernetes Secrets that this same phase's own plans
(05-02, 05-03) had already deleted — meaning a genuinely fresh cluster rebuild would
have left Vault structurally bootstrapped but with **zero credential values ever
written**. Plan 05-06 Task 1 fixed this (credentials now sourced from the live
`data/minio-app` Secret and a fresh `kubectl exec`-driven `ALTER ROLE` against the
CNPG analytics primary, never a deleted Secret) and added an offline regression guard,
`tests/unit/test_vault_bootstrap.py` (7 cases, independently re-run and confirmed
passing).

Task 2 then proved the fix live via a **scoped Vault-release-and-PVC reinstall**
(delete `data-vault-0`/`audit-vault-0` and pod `vault-0` in namespace `vault` only —
not a full `kind delete cluster` + recreate, which remains disproportionately slow for
this cadence). This produces the identical precondition a full rebuild would:
`scripts/vault-bootstrap.py` cannot distinguish "the whole cluster was rebuilt" from
"only Vault's storage was wiped" — both present it with an empty KV store, the only
precondition its logic branches on. Against that genuinely empty, freshly-reinstalled
Vault:

- `make vault-bootstrap` exited 0 and printed **"created"** (not "already present")
  for all three previously-broken paths (`etl/analytics-db`, `etl/minio`,
  `airflow/connections/minio_default`) — direct proof the `InvalidPath`/regeneration
  branch executed, where before this fix the identical sequence exited 1 with a
  `RuntimeError` naming a Secret that no longer exists.
- **15 of 16** `tests/e2e/vault` tests passed against the freshly-generated
  credentials, including every test that exercises credential *function* rather than
  mere presence: `test_positive_auth.py` (the `csv-processor` ServiceAccount reads
  both its real Vault paths), `test_negative_auth.py`, `test_audit_log.py`,
  `test_rotation.py`, and `test_dev_secrets_reproducible.py`'s idempotent-rerun case.
- The one failure, `test_airflow_backend.py::test_dag_still_resolves_its_connection_and_runs`,
  was **not** a credential failure — it timed out waiting for a live DAG run to
  complete. **Root-caused and fixed via a separate `/gsd:debug` session**
  (`.planning/debug/resolved/dagrun-scheduler-stall.md`): a Docker Desktop/WSL2-level
  event restarted every container on all 3 kind nodes simultaneously, breaking the
  DAGs `hostPath` bind mount platform-wide. With an empty mount, the DAG processor
  never re-parsed anything and `DagModel.is_stale` never cleared, so the scheduler's
  own query silently excluded **every** DagRun for **every** DAG, cluster-wide — a
  total scheduling freeze with zero relation to Vault, credentials, or
  `csv_ingest_customers` specifically. Fixed by restarting the affected kind-node
  Docker containers (reattaching the mount); independently re-verified: the
  originally-stuck DagRun reached `SUCCEEDED`, and `meta.ingestion_runs` recorded a
  fresh `SUCCEEDED` row (`run_id=5127`, `2026-08-14 20:09:50`) after this plan's
  Vault reinstall — a real pipeline run completing end-to-end on the freshly-generated
  `etl_app`/MinIO credentials.
- **One honest residual nuance:** re-running `pytest tests/e2e/vault -q -m cluster`
  after that fix still shows the same test failing, but now for a different, benign
  reason — an unrelated, pre-existing backlog of queued DagRuns (from an over-broad
  `airflow tasks clear` during plan 05-03) is still deep, so the DagRun actually
  executing right now has no reason to notice this specific test's freshly-uploaded
  marker file. This is expected, self-resolving queue depth, explicitly distinguished
  from the scheduler-freeze bug above (which is fixed) — not a Vault/credential
  regression, and not something forced faster here (that would mean bulk-clearing the
  backlog, a class of action already declined once this project's session).

The full literal `make cluster-down && make cluster-up && make vault-unseal && make
vault-bootstrap && make vault-verify` sequence from an actual clean checkout was
**still not independently re-run** in this session. The scoped reinstall above is
considered an equivalent proof specifically for `vault-bootstrap.py`'s own credential-
sourcing logic (per the empty-KV-store argument above), not a substitute for
exercising the full teardown/recreate path itself. See
`.planning/phases/05-vault-secrets-workload-identity/05-VALIDATION.md`'s "Manual-Only
Verifications" table for the current, itemized status.

## Sources

- `packages/dataplat/src/dataplat/secrets/resolver.py` — the `vault://` scheme
  dispatch and the module-level cached `hvac.Client`
- `airflow/dags/_common/kpo.py`, `packages/csv-processor/src/csv_processor/cli.py` —
  the KPO-pod side of the double-indirection
- `helm/values/local/airflow.yaml`, `helm/values/ci/airflow.yaml` — `config.secrets`
  (`VaultBackend`)
- `scripts/vault-bootstrap.py`, `scripts/vault-unseal.py`, `scripts/vault-audit-tail.py`
- `tests/e2e/vault/test_positive_auth.py`, `test_negative_auth.py`,
  `test_airflow_backend.py`, `test_rotation.py`, `test_audit_log.py`,
  `test_dev_secrets_reproducible.py`, `test_unseal_survives_restart.py`
- `tests/policy/test_no_stale_secrets.py`
- `.planning/phases/05-vault-secrets-workload-identity/05-01-PLAN.md` through
  `05-05-PLAN.md` and their SUMMARY.md files — every `<threat_model>` block cited in
  §2, and the full accounting of what each plan proved live
- `.planning/phases/05-vault-secrets-workload-identity/05-RESEARCH.md` — the kind
  JWT-issuer verification, the `auth_type: kubernetes` provider-source verification,
  and the Vault-vs-Agent-Injector architecture decision
- `.planning/research/STACK.md` §E — the two-tier secret-delivery pattern comparison
  table (Agent Injector / CSI / VSO / ESO / direct SA-token login)
- `docs/adr/0009-openbao-licence-escape-hatch.md` — the BUSL-1.1 licence risk
  acceptance referenced in §6
