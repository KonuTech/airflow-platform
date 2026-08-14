# Phase 5: Vault Secrets & Workload Identity - Pattern Map

**Mapped:** 2026-08-14
**Files analyzed:** 27 (new + modified, extracted from 05-CONTEXT.md's Existing Code Insights and 05-RESEARCH.md's Recommended Project Structure / Runtime State Inventory)
**Analogs found:** 24 / 27 exact-or-role-match, 3 flagged as genuinely novel (no strong in-repo precedent — see "No Analog Found")

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `packages/dataplat/src/dataplat/secrets/resolver.py` | utility (opaque-ref dispatcher) | transform + request-response (vault branch makes a network call) | itself — existing `env://`/`file://` branches in the same function | exact (self) |
| `packages/dataplat/src/dataplat/errors.py` | error hierarchy | n/a | itself — `SecretResolutionError` already defined | exact (self) |
| `packages/dataplat/pyproject.toml` | config (package manifest) | n/a | itself — `[project.dependencies]` list | exact (self) |
| `tests/unit/test_secrets_resolver.py` | test (unit) | n/a | itself + `tests/unit/test_csv_processor_cli.py` (monkeypatch convention) | exact (self) / role-match |
| `kubernetes/rbac-vault.yaml` (create only if the chart's `authDelegator` doesn't already cover it — verify against pulled chart first) | RBAC manifest | n/a (declarative) | `kubernetes/rbac-etl.yaml` | exact |
| `helm/values/local/vault.yaml` | config (Helm values) | n/a | `helm/values/local/minio.yaml` | role-match (closest single-node stateful third-party chart) |
| `helm/values/ci/vault.yaml` | config (Helm values) | n/a | `helm/values/ci/minio.yaml` | role-match |
| `helm/values/local/airflow.yaml` (EDIT) | config (Helm values) | n/a | itself — already has `data.metadataSecretName`/env `secretKeyRef` shapes to extend and later remove | exact (self) |
| `helm/values/ci/airflow.yaml` (EDIT) | config (Helm values) | n/a | itself + local counterpart | exact (self) |
| `helm/versions.env` (EDIT) | config | n/a | itself — `MINIO_CHART_VERSION`/`MINIO_IMAGE_TAG` pair | exact (self) |
| `.gitignore` (EDIT) | config | n/a | itself — `build/`, `tools/bin/` per-run/local-only entries | role-match |
| `scripts/vault-bootstrap.py` (or `.sh`, planner's choice per 05-RESEARCH.md Open Question 3) | script (idempotent bootstrap) | event-driven / imperative one-shot | `scripts/etl-secrets.sh` (idempotency shape) + `scripts/repair-duplicate-file-lineage.py` (Python + port-forward mutating a live cluster) | role-match |
| `scripts/vault-unseal.sh` | script | imperative one-shot | `scripts/airflow-metadata-secret.sh` (generate-once discipline) | partial — see "No Analog Found" |
| `scripts/vault-audit-tail.sh` | script (dev tooling) | file-I/O / streaming read + transform (human formatting) | `scripts/etl-secrets.sh` (`exec -i` mechanism) + `scripts/ingest-demo.py` (human-readable receipt convention) + `scripts/minio-credentials.sh` (`show` shape) | role-match |
| `scripts/stages/80-vault.sh` | script (stage runner) | imperative orchestration | `scripts/stages/60-minio.sh` + `scripts/stages/75-etl.sh` | exact |
| `Makefile` (EDIT — `vault-bootstrap`/`vault-unseal`/`vault-audit-tail`/`vault-verify` targets) | config / build | n/a | itself — `ingest-demo`, `minio-creds`, `cluster-verify`, `stage-%` recipes | exact (self) |
| `docs/adr/0009-openbao-licence-escape-hatch.md` | docs | n/a | `docs/adr/0006-unmaintained-upstream-artifacts.md` | exact |
| `tests/e2e/vault/__init__.py` | test scaffolding | n/a | `tests/e2e/cluster/__init__.py` | exact |
| `tests/e2e/vault/conftest.py` | test fixture | n/a | `tests/e2e/cluster/conftest.py` | exact |
| `tests/e2e/vault/test_positive_auth.py` | test (e2e) | request-response | `tests/e2e/cluster/test_minio_buckets.py` | role-match |
| `tests/e2e/vault/test_negative_auth.py` | test (e2e) | request-response | `tests/e2e/cluster/test_minio_buckets.py`'s deny case + 05-RESEARCH.md's own concrete code example | role-match |
| `tests/e2e/vault/test_airflow_backend.py` | test (e2e) | request-response | `tests/e2e/cluster/test_airflow_workloads.py` | role-match |
| `tests/e2e/vault/test_audit_log.py` | test (e2e) | file-I/O (read log) + transform (parse JSON) | `tests/e2e/vault/conftest.py`'s own new exec helper + `tests/e2e/cluster/conftest.py`'s `kubectl_json` | role-match |
| `tests/e2e/vault/test_rotation.py` | test (e2e) | request-response | 05-RESEARCH.md's own concrete code example + `test_airflow_workloads.py`'s `kubectl exec` pattern | role-match |
| `tests/e2e/vault/test_unseal_survives_restart.py` | test (e2e) | event-driven (restart) | none strong | no-analog |
| `tests/e2e/vault/test_dev_secrets_reproducible.py` | test (e2e) | n/a | `tests/policy/test_workflow_secrets.py`'s D-14 section (policy, not e2e) | weak/partial |
| `tests/policy/test_no_stale_secrets.py` | test (policy) | n/a | `tests/policy/test_workflow_secrets.py` | exact |

## Pattern Assignments

### `packages/dataplat/src/dataplat/secrets/resolver.py` (utility, transform + request-response)

**Analog:** itself — the file's own `env://`/`file://` branches, extended with a third `vault://` branch inside the same `resolve_secret()` function. D3 (from `.planning/research/SUMMARY.md`) requires this: no call site changes, only this function's scheme dispatch grows.

**Imports pattern** (`packages/dataplat/src/dataplat/secrets/resolver.py` lines 15-21):
```python
from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import urlsplit

from dataplat.errors import SecretResolutionError
```
Add `import hvac` here (and `hvac.exceptions` if caught explicitly) — this file's own existing style imports the exact stdlib pieces it uses and nothing more; `hvac` is the one new third-party import this phase adds anywhere in `dataplat`.

**Core scheme-dispatch pattern to extend** (lines 42-56, exact current text):
```python
    parsed = urlsplit(ref)
    if parsed.scheme == "env":
        value = os.environ.get(parsed.netloc or parsed.path.lstrip("/"))
        if value is None:
            msg = f"environment variable not set for ref {ref!r}"
            raise SecretResolutionError(msg, context={"ref": ref})
        return value
    if parsed.scheme == "file":
        try:
            return Path(parsed.path).read_text(encoding="utf-8").strip()
        except OSError as exc:
            msg = f"cannot read secret file for ref {ref!r}: {exc}"
            raise SecretResolutionError(msg, context={"ref": ref}) from exc
    msg = f"unsupported secret ref scheme {parsed.scheme!r} in {ref!r}"
    raise SecretResolutionError(msg, context={"ref": ref})
```
A new `if parsed.scheme == "vault":` block goes between the `file` branch and the final fail-closed `raise` — same shape: `try`/`except` around the actual I/O, always re-raising as `SecretResolutionError(msg, context={"ref": ref})`, never letting a third-party exception (`hvac.exceptions.VaultError`) escape unwrapped. 05-RESEARCH.md's Pattern 2 (Architecture Patterns section) already works out the exact parse (`urlsplit` `.netloc`/`.path`/`.fragment` → `mount_point`/`path`/`field`) and is the concrete fill-in for this branch — it was derived directly from this same `urlsplit()` convention, so it is a natural extension, not an inference from elsewhere.

**Error handling pattern** — every unsupported/failed path raises `SecretResolutionError(msg, context={"ref": ref})`, never returns the raw reference and never lets a lower-level exception type surface directly (see `tests/unit/test_secrets_resolver.py`'s `test_resolver_never_returns_the_raw_unresolved_reference_string`, below).

**Docstring correction required** (lines 1-13) — the module docstring currently reads:
```
The caller never learns which backend actually served a secret (SEC-15): a
config or call site holds an opaque secret reference string, e.g.
``env://DB_PASSWORD`` or ``file:///vault/secrets/analytical-db``, and
``resolve_secret()`` is the only place that interprets the scheme.
``vault://`` is real — it is Phase 5's, not this phase's — and any
unrecognized scheme (including ``vault://`` before Phase 5 implements it, ...
```
The `file:///vault/secrets/analytical-db` example illustrates the Agent Injector sidecar pattern, which 05-CONTEXT.md's Claude's Discretion section and 05-RESEARCH.md's Anti-Patterns both confirm this project does NOT build (STACK.md's two-tier direct-SA-login pattern is locked instead). This phase's edit to this file must also correct this docstring — replace the `file:///vault/secrets/...` example with a `vault://etl/analytics-db#dsn`-shaped one and update the "vault:// is real — it is Phase 5's, not this phase's" sentence, which becomes false the moment this file is edited.

---

### `packages/dataplat/src/dataplat/errors.py` (error hierarchy)

**Analog:** itself — `SecretResolutionError` already exists; likely only a docstring touch-up is needed (no new exception class — SEC-15/D3 already anticipated `vault://` raising this exact type).

**Existing class** (lines 93-99):
```python
class SecretResolutionError(DataPlatformError):
    """An opaque secret reference could not be resolved to a value.

    Raised when a ``SecretRef`` (``env://``, ``file://``, or an unrecognized
    scheme such as ``vault://`` before Phase 5 implements it) cannot be
    turned into a usable secret value.
    """
```
The parenthetical "an unrecognized scheme such as `vault://` before Phase 5 implements it" becomes stale prose once `vault://` is real — update to describe it as a recognized-but-failed scheme alongside `env://`/`file://`, not an example of an unrecognized one.

**Context-reservation pattern to reuse** (lines 50-72, `DataPlatformError.__init__`) — every raise site in the new `vault://` branch must pass `context={"ref": ref}` (never `error_type`/`error_message`, which are reserved for `cli.py`'s catch-once handler per WR-03) — this is already how `env://`/`file://` raise, so no new pattern is needed, only consistent reuse.

---

### `packages/dataplat/pyproject.toml` (config)

**Analog:** itself.

**Dependency list to extend** (lines 10-17):
```toml
dependencies = [
  "PyYAML>=6",
  "psycopg[binary,pool]>=3.3.4,<4",
  "boto3>=1.43.68,<2",
  "pydantic>=2.13,<3",
  "structlog>=26,<27",
  "click>=8.4,<9",
]
```
Add `"hvac>=2.4,<3"` in the same pinned-range style as every other entry (`>=X,<X+1major`), per 05-RESEARCH.md's Installation section. `hvac` is a runtime dependency (used inside the ETL pod at resolve time), not a dev-only tool, so it belongs in `[project.dependencies]`, not a dev/cluster group.

---

### `tests/unit/test_secrets_resolver.py` (test, unit)

**Analog:** itself (full file already covers `env://`/`file://` success/failure) + `tests/unit/test_csv_processor_cli.py`'s `monkeypatch.setattr(module, "name", replacement)` convention for mocking a module-level callable.

**Test to replace** (lines 49-54 — this assertion becomes FALSE once `vault://` is real, per the file's own comment; it must be replaced, not left in place per 05-RESEARCH.md's Wave 0 Gaps):
```python
def test_vault_scheme_fails_closed_rather_than_passing_through() -> None:
    """SEC-15's central claim: the scheme most likely to be added carelessly
    later (Phase 5's ``vault://``) is rejected today, not silently accepted.
    """
    with pytest.raises(SecretResolutionError):
        resolve_secret("vault://kv/data/etl/db")
```

**Existing fixture-based mocking convention to reuse for the env case** (lines 22-25):
```python
def test_env_scheme_returns_the_set_environment_variable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SOME_TEST_VAR", "s3cr3t-value")

    assert resolve_secret("env://SOME_TEST_VAR") == "s3cr3t-value"
```
For `vault://`, there is no `monkeypatch.setenv` equivalent — the new tests need to mock `resolver._vault_client()` (the module-level cached client 05-RESEARCH.md's Pattern 1 proposes) or the `hvac.Client` it returns. `tests/unit/test_csv_processor_cli.py` establishes this repo's own convention for that shape:
```python
monkeypatch.setattr(csv_processor_cli, "_build_common", _raise_runtime_error)
```
i.e. `monkeypatch.setattr(resolver, "_vault_client", lambda: fake_client)` with `fake_client` a small stub/`MagicMock` whose `.secrets.kv.v2.read_secret_version(...)` returns a canned `{"data": {"data": {field: value}}}` dict for the success case, and raises `hvac.exceptions.VaultError` for the failure case — matching `SecretResolutionError`'s `except hvac.exceptions.VaultError` wrapping in `resolver.py`.

**Non-vacuity control already established** (lines 65-73) — keep this shape for the new scheme too:
```python
def test_resolver_never_returns_the_raw_unresolved_reference_string() -> None:
    """The literal reference string itself must never come back as if it
    were a resolved value -- every unsupported path must raise instead.
    """
    ref = "ftp://not-supported"
    with pytest.raises(SecretResolutionError) as exc_info:
        resolve_secret(ref)

    assert str(exc_info.value) != ref
```

---

### `kubernetes/rbac-vault.yaml` (RBAC manifest — only if needed; verify against the pulled chart first)

**Analog:** `kubernetes/rbac-etl.yaml` (full file, 69 lines) — the exact shape for "the ONE RBAC grant for a named identity seam," including its header-comment discipline of stating the precise invariant the file holds.

**Header-comment convention to copy** (lines 1-13):
```yaml
# The ONLY RBAC grant into namespace `etl`. Invariant this file must always
# hold: exactly `get/list/watch/create/patch/delete` on `pods`, `get` on
# `pods/log`, `create/get` on `pods/exec`, `list/watch` on `events` — in
# namespace `etl` only — bound to exactly two named subjects. Nothing
# broader: no `cluster-admin`, no wildcard verb, no wildcard resource, no
# ClusterRole. ...
```

**ServiceAccount + Role + RoleBinding shape to copy** (lines 28-69):
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: csv-processor
  namespace: etl
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: etl-pod-launcher-role
  namespace: etl
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "create", "patch", "delete"]
  ...
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: etl-pod-launcher-rolebinding
  namespace: etl
subjects:
  - kind: ServiceAccount
    name: airflow-worker
    namespace: airflow
  - kind: ServiceAccount
    name: airflow-scheduler
    namespace: airflow
roleRef:
  kind: Role
  name: etl-pod-launcher-role
  apiGroup: rbac.authorization.k8s.io
```
05-RESEARCH.md's Standard Stack table already found the chart's own `authDelegator.enabled: true` default creates the `system:auth-delegator` ClusterRoleBinding Vault's Kubernetes-auth TokenReview call needs — verify this is genuinely sufficient before writing this file at all; if it is, this file may not be needed and its absence should be a documented finding, not an oversight (matching how `kubernetes/rbac-etl.yaml`'s own header states its invariant explicitly rather than leaving it implicit).

---

### `helm/values/local/vault.yaml` and `helm/values/ci/vault.yaml` (config, Helm values)

**Analog:** `helm/values/local/minio.yaml` / `helm/values/ci/minio.yaml` — the closest existing pair: a single-node stateful third-party chart with an `existingSecret` reference (never a literal), a `nodeSelector`, explicit `resources` overriding an unschedulable stock default, and the mandatory D-06 divergence-axes header comment.

**D-06 header-comment convention to copy verbatim in shape** (`helm/values/local/minio.yaml` lines 1-12):
```yaml
# D-06: the local and CI profiles diverge on EXACTLY three axes — replica
# counts, resource sizing, and monitoring. This file and
# helm/values/ci/minio.yaml must otherwise be identical in shape. MinIO's
# `metrics.serviceMonitor.enabled` chart default is already `false` (no
# kube-prometheus-stack exists until Phase 7), so the monitoring axis needs no
# override in either profile here.
#
# INFRA-05 / D-08 / D-14: five buckets (`raw` versioned, the rest not), a
# single IAM policy ... No credential literal of any kind appears in this file.
```
For Vault, note 05-RESEARCH.md's own finding that a FOURTH divergence axis is arguable here too — CI keeps `server.dev.enabled: true` (per STACK.md's existing CI guidance) while local uses `standalone` + file storage — mirroring exactly how `helm/values/local/airflow.yaml`'s header (below) argues its own fourth axis (`executor`) rather than smuggling it in silently. Any such extra axis must be argued in the header comment, not left implicit — `tests/policy/test_values_profiles.py` (not read this session, but referenced throughout this codebase as the D-06 divergence enforcement test) will need the new axis allowlisted by name.

**`existingSecret`/`image.tag`/`resources` shape to copy** (`helm/values/local/minio.yaml` lines 18-49):
```yaml
mode: standalone
replicas: 1

image:
  repository: pgsty/minio
  tag: "RELEASE.2026-08-04T00-00-00Z"

existingSecret: minio-root

nodeSelector:
  airflow-platform/role: storage

resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    cpu: "1"
    memory: 1Gi
```
CI's counterpart (`helm/values/ci/minio.yaml` lines 33-39) sizes smaller because "this profile shares a 4 CPU / 16 GB runner with every other pod the CI job renders" — the same reasoning applies to Vault's CI resources, and `tests/policy/test_manifest_resources.py` (D-12) will sum whatever is declared here against that budget.

**Pitfall this phase's own research already flags for this exact file** — 05-RESEARCH.md's Common Pitfall 4: the chart's own `server.standalone.config` default is `tls_disable = 1`. Neither `minio.yaml`'s pattern nor any other existing values file in this repo has had to make this specific TLS decision before — flag it explicitly in this file's header comment (Open Question 2), the same way `helm/values/local/minio.yaml`'s own header names ADR-0006 for its own accepted risk.

---

### `helm/values/local/airflow.yaml` and `helm/values/ci/airflow.yaml` (EDIT)

**Analog:** itself — both files already read in full this session.

**Existing `data.metadataSecretName` reference-by-name shape to mirror for the new `config.secrets` block** (`helm/values/local/airflow.yaml` lines 48-53):
```yaml
data:
  # scripts/airflow-metadata-secret.sh (Task 1) derives this Secret's sole
  # key, `connection`, from the CNPG-generated `airflow-db-app` Secret in
  # namespace `data` (D-13's cross-namespace correction) — never a literal
  # connection string here.
  metadataSecretName: airflow-metadata
```
The new block (no existing `config:` key in this file today — confirmed by the full read) is, per 05-RESEARCH.md's Pattern 3 (already verified against the exact running `providers-hashicorp/4.7.1` source):
```yaml
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
`variables_path: null` is load-bearing, not decoration — `airflow/dags/_common/kpo.py`'s `Variable.get("csv_processor_image")` (line 71, see below) runs at DAG-parse time and must keep resolving from the metadata DB, never Vault.

**Exact `env:`/`secretKeyRef` blocks that D-01 requires removing, ONE AT A TIME, only after the Vault-backed path is confirmed** — three separate occurrences in this one file (`triggerer.env` lines 109-114, `scheduler.env` lines 190-195, `workers.kubernetes.env` lines 272-277), all identical in shape:
```yaml
  env:
    - name: AIRFLOW_CONN_MINIO_DEFAULT
      valueFrom:
        secretKeyRef:
          name: airflow-minio-connection
          key: AIRFLOW_CONN_MINIO_DEFAULT
```
Each of these three blocks is removed only once `VaultBackend` is proven to serve `minio_default` directly (SEC-05's own acceptance test) — matching D-01's explicit "additive-then-subtractive, never a batch cleanup" sequencing. `helm/values/ci/airflow.yaml` carries the identical three blocks (its own header comment at lines 18-21 states they are "identical in both profiles because the underlying hostPath and Secret name are identical in both").

**D-06/fourth-axis-argued convention already established for THIS pair of files** (`helm/values/local/airflow.yaml` lines 1-15, `helm/values/ci/airflow.yaml` lines 1-9) — Vault's own `dev.enabled` local-vs-CI divergence (above) should follow this exact same "argued, not smuggled" convention these two files already use for `executor`.

---

### `helm/versions.env` (EDIT)

**Analog:** itself.

**Existing `KEY=value` pin pair to copy the shape of** (lines 20-26):
```
INGRESS_NGINX_CHART_VERSION=4.15.1
CNPG_OPERATOR_CHART_VERSION=0.29.0
CNPG_CLUSTER_CHART_VERSION=0.8.1
MINIO_CHART_VERSION=5.4.0
MINIO_IMAGE_TAG=RELEASE.2026-08-04T00-00-00Z
AIRFLOW_CHART_VERSION=1.22.0
AIRFLOW_IMAGE_TAG=3.3.0
```
Add `VAULT_CHART_VERSION=0.34.0` (05-RESEARCH.md flags `0.34.1` is now current but the phase's locked pin per STACK.md/CLAUDE.md is `0.34.0` — verify no regression before moving off it, per the research's own State of the Art note). No separate `VAULT_IMAGE_TAG` is needed unless the chart's default `server.image.tag` (which tracks the chart's own `appVersion: 2.0.3`) is overridden — `helm/versions.env`'s own header comment (lines 1-12, already read) states this file is "the ONE place a version literal for these artifacts may live," enforced by `tests/policy/test_pinned_tool_versions_agree.py` — any chart-version literal written into `helm/values/*/vault.yaml` or `scripts/stages/80-vault.sh` instead of read from here would violate that same single-source rule this repo already enforces for every other chart.

---

### `.gitignore` (EDIT)

**Analog:** itself.

**Existing per-run/local-only entry shape** (lines 16-22):
```
# D-04: scripts/cluster-rebuild.sh's last-run per-stage timing breakdown —
# a record of one run, not a build artifact anyone commits.
build/

# GSD run-scoped dispatch sentinel (WINDOWS #7) — must be ignored before any
# infrastructure file lands, or it is swept into this phase's first commit.
.gsd/
```
D-02's unseal-key storage location needs a new entry in exactly this style — a short comment explaining WHY the path must never be committed, followed by the path itself (e.g. `.secrets/vault-init.json` per 05-RESEARCH.md's Recommended Project Structure). This is the first time this repository stores any credential-shaped value on the local filesystem at all — every existing script (`scripts/minio-credentials.sh`, `scripts/airflow-metadata-secret.sh`, `scripts/etl-secrets.sh`) explicitly boasts the opposite invariant ("No value generated or read by this script is ever written to the working tree"). This `.gitignore` entry is where that new, deliberate exception becomes visible; see "No Analog Found" below.

---

### `scripts/vault-bootstrap.py` (or `.sh`) (script, idempotent bootstrap)

**Analog (idempotency shape):** `scripts/etl-secrets.sh` and `scripts/minio-credentials.sh` — both structured as a single `ensure` subcommand made of independent, individually-guarded sub-steps.

**Idempotency guard convention to copy** (`scripts/minio-credentials.sh` lines 87-104):
```bash
cmd_ensure() {
  if _secret_exists "${ROOT_SECRET}"; then
    echo "==> Secret ${NAMESPACE}/${ROOT_SECRET} already exists — leaving it unchanged"
  else
    echo "==> creating Secret ${NAMESPACE}/${ROOT_SECRET}"
    _create_secret "${ROOT_SECRET}" \
      "rootUser=$(_random_hex 8)" \
      "rootPassword=$(_random_hex 32)"
  fi
  ...
}
```
Applied to Vault: check whether the `kubernetes` auth method is already enabled (`hvac.Client.sys.list_auth_methods()`), whether each KV mount exists, whether each role/policy already has the expected content — write only what is missing, exactly as `_secret_exists` gates every `_create_secret` call here. `scripts/etl-secrets.sh`'s three independently-guarded `_ensure_*_secret` functions (lines 123-199, already read) are the multi-step version of the same shape — each sub-step is safe to re-run and never rotates something already in place.

**Analog (Python + port-forward + a client library reaching the live cluster, since 05-RESEARCH.md recommends `hvac` over the `vault` CLI):** `scripts/repair-duplicate-file-lineage.py` and `scripts/ingest-demo.py` — both host-side Python scripts that duplicate the SAME `_port_forwarded_analytics`/`_free_local_port` helper rather than sharing a library module (confirmed: `scripts/repair-duplicate-file-lineage.py` line 49's own comment says "duplicated from `scripts/ingest-demo.py`'s helper"). This is the established convention for `scripts/*.py`: no shared `scripts/_lib.py`, each script is a self-contained, independently-readable unit that copies the small pieces it needs, with a comment naming its source.

**Concrete helper shapes to copy** (`scripts/ingest-demo.py` lines 185-233, already read in full):
```python
def _versions_env_variable(name: str) -> str: ...
def _kubectl_context() -> str: ...
def _require_kubectl() -> str: ...
```
and the `_port_forwarded_analytics` context manager (lines 373-435) — Vault has no ingress either (matching Postgres's own no-ingress situation this same pattern already solves), so a `vault-bootstrap.py` reaching the Vault API from the host most likely needs the identical `kubectl port-forward svc/vault 8200:8200` + `hvac.Client(url="http://127.0.0.1:<local_port>")` shape, torn down on exit in a `finally:` block exactly as this context manager does.

**Note on `tests/policy/test_no_manual_kubectl_surgery.py` applicability:** that policy test's `SCAN_DIRS` is `(REPO_ROOT / "scripts", REPO_ROOT / "tools")` and only globs `*.sh` — a Python bootstrap script is NOT scanned by it at all (confirmed by reading `_scan_paths()`, lines 255-259). Writing the bootstrap in Python, as 05-RESEARCH.md recommends, sidesteps this policy test entirely; writing it as a `.sh` invoking `vault write ...` over a port-forwarded `vault` CLI would still need to stay inside the permitted read-only/`apply -f -`/`exec -i` set below.

---

### `scripts/vault-unseal.sh` (script, D-02)

**Analog (partial):** `scripts/airflow-metadata-secret.sh`'s "generate once, guarded, never echoed, never in argv" discipline — but see "No Analog Found" for what does NOT carry over.

**Trust-model header-comment convention to copy in shape** (`scripts/airflow-metadata-secret.sh` lines 10-23, and `scripts/etl-secrets.sh` lines 10-18 which states outright "Phase 5 replaces this whole script with Vault"):
```bash
# TRUST MODEL (read before touching this file): the principal performing the
# cross-namespace read below is the DEVELOPER'S OWN KUBECONFIG CONTEXT. This
# script runs host-side, during `make cluster-up`, as the human who created
# the cluster — using the credentials `kind` wrote to their kubeconfig. It
# therefore needs no ServiceAccount, no Role and no RoleBinding, and this
# script creates none...
```

**Value-never-in-argv discipline to copy** (`scripts/etl-secrets.sh` lines 28-36, `scripts/airflow-metadata-secret.sh` lines 50-54) — every secret payload moves only through a pipe (`kubectl apply -f -` on stdin, `kubectl exec -i ... psql` on stdin), never a CLI argument. `vault operator unseal <key>` takes its key positionally on most Vault CLI invocations; if the planner uses the `vault` CLI (rather than `hvac.Client.sys.submit_unseal_key(...)`, which takes the key as a Python argument, never a subprocess `argv`), verify the CLI supports reading the key from stdin — `hvac`'s own Python call avoids the question entirely and is consistent with 05-RESEARCH.md's Python-bootstrap recommendation.

---

### `scripts/vault-audit-tail.sh` (script, D-04)

**Analog (mechanism):** `scripts/etl-secrets.sh`'s already-permitted `kubectl exec -i` pattern.

**Exact invocation shape to copy** (`scripts/etl-secrets.sh` lines 145-148):
```bash
  echo "==> setting etl_app's password via kubectl exec (peer/local trust, not a network connection)"
  printf "ALTER ROLE etl_app WITH PASSWORD '%s';\n" "${password}" \
    | _kubectl exec -i -n "${DATA_NAMESPACE}" "${primary_pod}" -- \
        psql -v ON_ERROR_STOP=1 -U postgres -d analytics >/dev/null
```
For audit-tail this becomes a READ through the same `exec -i` mechanism (stdin isn't used for input here, but the literal `-i` flag is still what makes `tests/policy/test_no_manual_kubectl_surgery.py`'s `kubectl_invocation()` classify it as permitted — see that file's `has_dash_i` check, lines 207 and 238-246, which does not distinguish a read from a write, only checks the flag is present):
```bash
_kubectl exec -i -n vault vault-0 -- tail -n 200 /vault/audit/audit.log | jq ...
```
This requires 05-RESEARCH.md's recommended `server.auditStorage.enabled: true` + `vault audit enable file file_path=/vault/audit/audit.log` chart configuration (in the new `vault.yaml` values files) — the `stdout`-only alternative would need `kubectl logs`, which IS NOT in `_PERMITTED_READ_ONLY_SUBCOMMANDS = frozenset({"get", "wait"})` (line 144) and would fail this same policy test.

**Analog (human-readable output convention, explicitly named in 05-CONTEXT.md):** `scripts/ingest-demo.py`'s `_print_receipt()` (lines 599-614):
```python
def _print_receipt(outcome: _PollOutcome) -> None:
    """Print a terminal `_PollOutcome` as human-readable key-value lines.

    Deliberately not raw JSON: this is human-facing demo output (this
    plan's Task 1 action), read by a developer's eyes, not parsed by
    another program.
    ...
    """
    print("--- Ingestion receipt ---")
    print(f"run_id:       {outcome.run_id}")
    ...
```
`make vault-audit-tail`'s output should follow this same "deliberately not raw JSON, human-facing" framing — parse each Vault audit JSON line (per 05-RESEARCH.md's confirmed schema: `type`, `time`, `auth`, `request`, `response` top-level keys) and print a compact, readable line per entry (timestamp, path, principal, success/deny), never re-deriving or displaying the HMAC-hashed sensitive fields Vault's own audit device already redacts.

**Analog (read + shell-sourceable/formatted output convention):** `scripts/minio-credentials.sh`'s `cmd_show` (lines 106-118) — the "read live state back and format it, distinct subcommand from `ensure`" split is the same two-subcommand shape a combined `vault-bootstrap.sh {ensure|...}` might reuse, though `vault-audit-tail` is presentation-only and does not need an `ensure`-equivalent guard.

---

### `scripts/stages/80-vault.sh` (script, stage runner)

**Analog:** `scripts/stages/60-minio.sh` (credentials-before-chart-install ordering + `helm_install` sourcing) and `scripts/stages/75-etl.sh` (numbering-after-dependency rationale + delegating to a companion script).

**Full shape to copy** (`scripts/stages/60-minio.sh`, all 34 lines):
```bash
#!/usr/bin/env bash
#
# The MinIO component stage (D-08, D-14). Credentials MUST exist before the
# chart installs, because the chart references them by Secret name
# (`existingSecret`, `users[].existingSecret`) ...

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# shellcheck source=/dev/null
source "${repo_root}/helm/versions.env"
# shellcheck source=/dev/null
source "${repo_root}/scripts/helm-install.sh"
# shellcheck source=/dev/null
source "${repo_root}/scripts/wait-for.sh"

helm_bin="${repo_root}/tools/bin/helm"

echo "==> ensuring MinIO credentials (Secrets minio-root, minio-app in namespace data)"
"${repo_root}/scripts/minio-credentials.sh" ensure

"${helm_bin}" repo add minio https://charts.min.io >/dev/null 2>&1 || true
"${helm_bin}" repo update minio >/dev/null

helm_install minio minio/minio data MINIO_CHART_VERSION minio

wait_for_deploy_available data minio
```
`scripts/stages/75-etl.sh`'s numbering-rationale header comment (lines 1-8, already read) is the convention to copy for explaining WHY `80-vault.sh` is numbered after `75-etl.sh`: `LC_ALL=C` stage order via numeric filename prefixes is this repo's whole ordering mechanism (per `Makefile`'s own comment at lines 245-256), so state explicitly what this stage depends on existing already (namespaces? the `etl` RBAC/ServiceAccounts, if the Vault Kubernetes-auth role binds to them at bootstrap time) and why nothing later depends on it.

`helm_install`'s call signature (`scripts/helm-install.sh`, full file already read) is `helm_install <release> <chart-ref> <namespace> <version-var-name> <values-basename> [<wait-strategy>]` — e.g. `helm_install vault hashicorp/vault vault VAULT_CHART_VERSION vault`. Note the `wait_strategy` pitfall this same helper's own header comment documents (lines 21-32): Helm 4.2.3's `--wait` defaults to `hookOnly` unless overridden, and a chart with post-install hooks that block on the chart's own resources (Airflow's own migration Job is the worked example) needs the non-default `watcher`/`hookOnly` choice reasoned about explicitly, not assumed.

---

### `Makefile` (EDIT)

**Analog:** itself.

**`ingest-demo` recipe shape to copy for `vault-bootstrap`/`vault-unseal`** (lines 161-163):
```makefile
ingest-demo:                    ## D-14: upload FILE and wait for the real sensor-driven pipeline [plan 04-09]
	@if [ -z "$(FILE)" ]; then echo "ERROR: FILE is required, e.g. make ingest-demo FILE=tests/fixtures/csv/01_simple.csv" >&2; exit 1; fi
	$(RUN_CLUSTER) python scripts/ingest-demo.py --file $(FILE)
```
**`minio-creds` recipe shape to copy for `vault-audit-tail`** (lines 157-159):
```makefile
minio-creds:                   ## D-14: print live MinIO credentials, shell-sourceable [plan 02-04]
	@set -a; . helm/versions.env; set +a; \
	KUBECTL_CONTEXT="kind-$$CLUSTER_NAME" scripts/minio-credentials.sh show
```
**`cluster-verify` shape to copy for a new `vault-verify` target** (lines 165-178, `$(RUN_CLUSTER)` reasoning already documented there): a new `vault-verify` target following this exact shape (`$(RUN_CLUSTER) pytest tests/e2e/vault -q`) is what 05-RESEARCH.md's Validation Architecture section names as this phase's addition to the "Full suite command."

**`.PHONY` list to extend** (lines 51-55):
```makefile
.PHONY: help uv-guard install lock-check lint format typecheck imports test policy \
        fixtures fixtures-verify gitleaks gitleaks-selftest check ci clean \
        install-cluster doctor cluster-up cluster-down cluster-rebuild cluster-verify \
        minio-creds helm-lint manifests manifest-policy test-integration image-csv-processor \
        ingest-demo
```
Add `vault-bootstrap vault-unseal vault-audit-tail vault-verify` here.

---

### `docs/adr/0009-openbao-licence-escape-hatch.md` (docs)

**Analog:** `docs/adr/0006-unmaintained-upstream-artifacts.md` — the "named migration target + dated trigger" shape is exactly what SEC-14's production-substitution documentation and this ADR both need. `docs/adr/README.md` (already read, lines 91-93) has pre-announced this exact record:

```
| Vault is BUSL-1.1 and IBM-owned; OpenBao is the API-compatible escape hatch | **Phase 5** | Nothing is deployed against Vault until Phase 5. The licence assessment is real but has no consequence to record yet. |
```

**`Migration trigger` section shape to copy** (`docs/adr/0006-...md` lines 108-127) — a bulleted list of concrete, observable events, never "none" left unstated:
```markdown
## Migration trigger

Not "none" — each of the following ... is a mid-phase or mid-milestone reason to open the
migration this record already named:

* **A CVE with a public exploit** against `pgsty/minio`, ... published after this record's date.
* **The `pgsty/minio` fork goes more than six months without a release.** ...
...

On any of these: MinIO's migration target is **SeaweedFS**; ...
```
For Vault→OpenBao, analogous triggers: a Vault BUSL licence-term change adverse to this project's use, a security-relevant CVE unpatched for the local Vault pin past a stated window, or (per D-02's own "this is local-dev-only" framing) a decision to actually deploy this pattern outside local dev. `docs/adr/0000-template.md`'s structure (`Context and Problem Statement` / `Considered Options` / `Decision Outcome` / `Consequences` / `Migration trigger` / `References`) is the section skeleton; `docs/adr/README.md`'s numbering rules (lines 51-58, already read: zero-padded four digits, monotonic, never renumbered) confirm `0009` is the next free number after `0008-pipeline-composition-seam.md`.

---

### `tests/e2e/vault/__init__.py` and `tests/e2e/vault/conftest.py` (test fixtures)

**Analog:** `tests/e2e/cluster/conftest.py` — the file's own docstring already states the intended reuse: "The repository root is resolved once ... so a test never depends on the working directory."

**`_require_cluster` skip-cleanly pattern to copy exactly** (lines 79-102):
```python
@pytest.fixture(scope="session", autouse=True)
def _require_cluster(kubectl_context: str) -> None:
    """Skip the whole suite, with a named reason, when no live cluster answers. ..."""
    kubectl_bin = shutil.which("kubectl")
    if kubectl_bin is None:
        pytest.skip("kubectl not found on PATH — tests/e2e/cluster/ needs a live cluster")
    proc = subprocess.run(
        [kubectl_bin, "--context", kubectl_context, "get", "nodes", "-o", "name"],
        capture_output=True, text=True, check=False, timeout=10,
    )
    if proc.returncode != 0:
        pytest.skip(
            f"no live cluster reachable at context '{kubectl_context}' "
            f"(kubectl exited {proc.returncode}) — run `make cluster-up` first:\n{proc.stderr}",
        )
```
`tests/e2e/vault/conftest.py` should either import this fixture set from `tests/e2e/cluster/conftest.py` if pytest's conftest resolution allows it cleanly, or (matching this repo's `scripts/*.py` convention of duplicating small helpers with a comment naming the source, per `repair-duplicate-file-lineage.py` above) duplicate `cluster_name`/`kubectl_context`/`_require_cluster`/`kubectl`/`kubectl_json` verbatim — planner's call, but the shape must match exactly, including always naming `--context kubectl_context` explicitly (line 109's comment: "never the ambient current-context, which a developer's shell could have pointed anywhere").

**`kubectl_json` helper to add a Vault-specific counterpart of, or reuse directly** (lines 130-149) — a `vault_client` fixture (an authenticated `hvac.Client`, built via the same port-forward-then-authenticate shape `_vault_client()` uses) is this directory's own new addition, following the `s3_client` factory fixture's shape (lines 182-229) — a callable fixture parameterized by which identity to authenticate as (`"csv-processor"` / `"default"` / `"airflow"`), mirroring `s3_client("admin")`/`s3_client("app")`.

---

### `tests/e2e/vault/test_positive_auth.py` (test, e2e — SEC-06/SEC-07)

**Analog:** `tests/e2e/cluster/test_minio_buckets.py` — the `pytestmark = pytest.mark.cluster` + fixture-factory-parameterized-by-identity shape.

**Shape to copy** (lines 1-50, already read in full):
```python
pytestmark = pytest.mark.cluster

ALL_BUCKETS = frozenset({"raw", "validated", "processed", "quarantine", "metadata"})
APP_READABLE_BUCKETS = frozenset({"raw", "validated"})


def test_all_five_buckets_exist(s3_client: Callable[[str], Any]) -> None:
    """Every declared bucket exists, and no accidental sixth one does."""
    admin = s3_client("admin")
    app = s3_client("app")
    ...
```
For `csv-processor`'s own-path read: authenticate as the `csv-processor` identity (via a projected token for ServiceAccount `csv-processor` in namespace `etl` — obtained the same way this suite already runs, from the HOST via `kubectl create token csv-processor -n etl` or by reading a bound token, since this suite runs off-cluster like all of `tests/e2e/cluster/`), call `client.auth.kubernetes.login(role="csv-processor", jwt=...)`, then read `etl/data/analytics-db` and `etl/data/minio` and assert the values are non-empty strings — proving Spike U2's positive half.

---

### `tests/e2e/vault/test_negative_auth.py` (test, e2e — SEC-12)

**Analog:** `tests/e2e/cluster/test_minio_buckets.py`'s `test_raw_delete_is_denied_for_app_credential` (the "prove a denial AND prove nothing else changed" shape) plus 05-RESEARCH.md's own already-vault-specific worked example.

**Deny-and-verify-no-side-effect shape to copy** (`test_minio_buckets.py` lines 84-110):
```python
def test_raw_delete_is_denied_for_app_credential(s3_client: Callable[[str], Any]) -> None:
    """The negative case that carries §63: the pipeline's own credential cannot delete from raw."""
    app = s3_client("app")
    admin = s3_client("admin")
    ...
    with pytest.raises(ClientError) as exc_info:
        app.delete_object(Bucket=bucket, Key=key)
    error_code = exc_info.value.response.get("Error", {}).get("Code")
    assert error_code == "AccessDenied", (...)
    ...
```

**05-RESEARCH.md's own concrete Vault-specific fill-in** (Code Examples section, already vault-shaped, use directly):
```python
def test_default_service_account_is_denied_csv_processor_role(default_sa_jwt: str, vault_addr: str) -> None:
    """SEC-12: an unmatched ServiceAccount's login must fail closed."""
    client = hvac.Client(url=vault_addr)
    with pytest.raises(hvac.exceptions.VaultError):
        client.auth.kubernetes.login(role="csv-processor", jwt=default_sa_jwt)
```
`default_sa_jwt` needs a fixture producing the `default` ServiceAccount's own token in namespace `etl` (e.g. `kubectl create token default -n etl`), matching `s3_client`'s own credential-fixture-factory pattern in `tests/e2e/cluster/conftest.py`.

---

### `tests/e2e/vault/test_airflow_backend.py` (test, e2e — SEC-05)

**Analog:** `tests/e2e/cluster/test_airflow_workloads.py`.

**`kubectl exec deploy/airflow-api-server -- airflow ...` shape to copy** (lines 117-136):
```python
def test_running_airflow_version_is_3_3_0(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
) -> None:
    """`airflow version` inside the running api-server proves the image override beat 3.2.2."""
    proc = kubectl(
        "-n", NAMESPACE, "exec", "deploy/airflow-api-server", "--", "airflow", "version",
    )
    assert proc.returncode == 0, (...)
    assert "3.3.0" in proc.stdout, (...)
```
SEC-05's own acceptance test needs the same `kubectl exec deploy/airflow-api-server -- airflow connections get minio_default` (or `airflow dags test`/a live sensor poke check) shape — the exact same invocation form the `Makefile`'s own `image-csv-processor` target already uses for a *different* Airflow CLI call: `$(KUBECTL) --context "$$ctx" exec -n airflow deploy/airflow-api-server -- airflow variables set csv_processor_image "..."` (Makefile lines 239-240) — confirming this repo already treats `kubectl exec ... -- airflow <subcommand>` as the standard way to drive the live Airflow CLI from outside the cluster.

**Metadata-connection port-forward fixture shape available if needed** (`test_airflow_workloads.py` lines 152-218, `_port_forwarded_postgres` + `metadata_connection` fixture) — reusable if SEC-05's test needs to query `alembic_version`/connection tables directly rather than only through `airflow connections get`.

**Precondition this test depends on, not itself performs:** per the Runtime State Inventory (05-RESEARCH.md), the `env: AIRFLOW_CONN_MINIO_DEFAULT` `secretKeyRef` removal from `helm/values/*/airflow.yaml` (documented above) must already be deployed before this test's assertion is meaningful — the test proves resolution still works AFTER that edit, it does not perform the deletion itself.

---

### `tests/e2e/vault/test_audit_log.py` (test, e2e — SEC-08)

**Analog:** the same `kubectl exec -i ... tail ...` mechanism as `scripts/vault-audit-tail.sh` (internal reuse — the test proves the log format, the script formats it for a human) plus `tests/e2e/cluster/conftest.py`'s `kubectl_json`/JSON-parsing convention (lines 130-149, and `ingest-demo.py`'s `json.loads(proc.stdout)` at line 358).

**Shape:** exec the persistent audit log file, split into lines, `json.loads` each, assert at least one entry matches the login/read this test's own setup performed, and assert no top-level or nested value under `request`/`response` contains the plaintext secret value read earlier in the test (only the Vault-computed HMAC-SHA256 hash) — matching 05-RESEARCH.md's Security Domain table's "never set `log_raw = true`" mitigation and the Don't-Hand-Roll table's explicit warning not to re-derive or defeat Vault's own HMAC redaction.

---

### `tests/e2e/vault/test_rotation.py` (test, e2e — D-03)

**Analog:** 05-RESEARCH.md's own concrete worked example (Code Examples section) — use directly, it is already correctly scoped to this project's actual `_build_common()`-resolves-once-per-pod finding:
```python
def test_rotation_reflected_without_restart(vault_client, airflow_cli) -> None:
    old_value = airflow_cli.get_connection("minio_default")
    vault_client.secrets.kv.v2.create_or_update_secret(
        mount_point="airflow", path="connections/minio_default",
        secret={"conn_uri": "<new-uri-with-rotated-credential>"},
    )
    new_value = airflow_cli.get_connection("minio_default")  # SAME running Airflow process
    assert new_value != old_value  # picked up live, no pod restart in between
```
`airflow_cli.get_connection(...)` is not yet a fixture anywhere in this repo — it is most naturally the `kubectl exec deploy/airflow-api-server -- airflow connections get minio_default` invocation shape from `test_airflow_backend.py` above, wrapped as a small helper. This is D-03's ONE demonstrated credential path — no other credential needs an equivalent test this phase, per CONTEXT.md's own scope note.

---

### `tests/e2e/vault/test_unseal_survives_restart.py` (test, e2e — INFRA-06/SC3)

No strong analog — see "No Analog Found" below. Structurally it will need `tests/e2e/cluster/conftest.py`'s `kubectl`/`kubectl_context` fixtures plus a way to trigger a real restart (`kubectl delete pod vault-0` or a full `scripts/cluster-rebuild.sh`-style teardown) and then assert `hvac.Client.sys.is_sealed()` is initially `True` and becomes `False` only after `scripts/vault-unseal.sh` runs — proving persistence survived (data intact) while sealed-state did not (correctly requiring the scripted unseal).

---

### `tests/e2e/vault/test_dev_secrets_reproducible.py` (test, e2e — SEC-13)

**Analog (weak/partial):** `tests/policy/test_workflow_secrets.py`'s D-14 section is how this repo has previously proven "dev secrets are marked/isolated/reproducible" for MinIO/Airflow-metadata credentials — but that is a STATIC policy test (scans committed YAML/scripts for literals), not a live e2e test. See "No Analog Found."

---

### `tests/policy/test_no_stale_secrets.py` (test, policy — SEC-01)

**Analog:** `tests/policy/test_workflow_secrets.py`'s D-14 section — extremely close structural match, extend rather than reinvent.

**`FORBIDDEN_LITERAL_KEYS`/scanning-scope shape to copy** (lines 277-289):
```python
INFRA_YAML_DIRS = (REPO_ROOT / "helm", REPO_ROOT / "kubernetes", REPO_ROOT / "kind")
INFRA_SCRIPT_DIR = REPO_ROOT / "scripts"

FORBIDDEN_LITERAL_KEYS: frozenset[str] = frozenset(
    {"rootPassword", "fernetKey", "webserverSecretKey"},
)

SECRET_DATA_FIELDS: tuple[str, ...] = ("data", "stringData")
```
For SEC-01, the analogous claim is not "no literal appears" but "the three named legacy Secret NAMES no longer appear as a creation target anywhere" — a different predicate over the same scanned surface. A parallel structure:
```python
STALE_SECRET_NAMES: frozenset[str] = frozenset(
    {"csv-processor-db", "csv-processor-s3", "airflow-minio-connection"},
)
```
checked against `scripts/etl-secrets.sh` (should no longer create them, or the whole script is deleted per its own header comment's promise: "Phase 5 replaces this whole script with Vault") and against every `helm/values/*/airflow.yaml`'s `secretKeyRef.name` fields (should no longer reference `airflow-minio-connection` once the Vault-backed path is live).

**Full-tree-scan + report-problems function shape to copy** (lines 362-389):
```python
def infrastructure_credential_problems() -> list[str]:
    problems: list[str] = []
    for path in _infra_yaml_paths():
        label = str(path.relative_to(REPO_ROOT))
        try:
            docs = [d for d in yaml.safe_load_all(path.read_text(encoding="utf-8")) if d]
        except yaml.YAMLError:
            continue
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            problems += forbidden_literal_key_problems(doc, label)
            problems += inline_secret_data_problems(doc, label)
    if INFRA_SCRIPT_DIR.is_dir():
        for path in sorted(INFRA_SCRIPT_DIR.rglob("*.sh")):
            problems += script_literal_key_problems(
                path.read_text(encoding="utf-8"),
                str(path.relative_to(REPO_ROOT)),
            )
    return problems


def test_no_infrastructure_file_holds_a_credential_literal() -> None:
    problems = infrastructure_credential_problems()
    assert not problems, (
        "D-14: every credential must be generated at cluster-up and "
        "referenced by Secret name only:\n" + "\n".join(problems)
    )
```
This test's own sequencing note is important: it can only be written to assert the FINAL post-migration state (three names gone) — during the phase itself, per D-01's explicit "never a batch cleanup" sequencing, this test will legitimately fail until the last credential's old Secret is removed. Whether the planner phases this test's own introduction/tightening across the plan's tasks (matching D-01's per-credential sequencing) or introduces it once at the end is a planning decision, not a pattern-mapping one — flagged here so the planner sees the ordering tension explicitly.

---

## Shared Patterns

### Idempotent `ensure`, guarded by an existence check
**Source:** `scripts/etl-secrets.sh` (`_secret_exists` before every `_apply_secret`), `scripts/minio-credentials.sh` (`cmd_ensure`), `scripts/airflow-metadata-secret.sh` (`_secret_exists` before Fernet/API-key creation)
**Apply to:** `scripts/vault-bootstrap.py`/`.sh` — every mount/auth-method/role/policy write must check-then-skip, never blindly overwrite, so re-running `make vault-bootstrap` against an already-configured Vault is a safe no-op.

### No credential ever in argv, stdout log, or (until D-02) the working tree
**Source:** header comments in `scripts/etl-secrets.sh` (lines 28-36), `scripts/minio-credentials.sh` (lines 25-28), `scripts/airflow-metadata-secret.sh` (lines 50-54) — all state the same invariant nearly verbatim
**Apply to:** `scripts/vault-bootstrap.py`, `scripts/vault-unseal.sh`, `scripts/vault-audit-tail.sh` — every value moves through a pipe/stdin/direct Python variable, never a subprocess argument list, never `print()`ed unguarded (`scripts/minio-credentials.sh`'s `cmd_show` prints a stderr warning first: `"# ... output contains LIVE credentials — do not log or commit it"`).

### `kubectl` mutation discipline: `get`/`wait` (read-only), `apply -f <kubernetes/... | ->`, `exec -i` — nothing else
**Source:** `tests/policy/test_no_manual_kubectl_surgery.py`, `_PERMITTED_READ_ONLY_SUBCOMMANDS = frozenset({"get", "wait"})` (line 144) + `_is_permitted_apply()` (lines 212-217) + the `has_dash_i` check (line 207)
**Apply to:** every new `scripts/*.sh` this phase adds (`vault-unseal.sh`, `vault-audit-tail.sh`, `stages/80-vault.sh`) — this test scans `scripts/` and `tools/` automatically; a bare `kubectl exec` (no `-i`), `kubectl logs`, or `kubectl apply -f <uncommitted-path>` will fail CI. Python scripts (`vault-bootstrap.py`) are outside this scan's `SCAN_DIRS`/glob entirely.

### `SecretResolutionError(msg, context={"ref": ref})` — never let a third-party exception escape unwrapped
**Source:** `packages/dataplat/src/dataplat/secrets/resolver.py` (both existing branches), `packages/dataplat/src/dataplat/errors.py`'s `DataPlatformError.__init__` context-reservation
**Apply to:** the new `vault://` branch in `resolver.py` — catch `hvac.exceptions.VaultError` (and `KeyError` for a missing field), always re-raise as `SecretResolutionError` with `context={"ref": ref}`.

### Live-cluster test suite skip, never fail-with-noise, when no cluster is reachable
**Source:** `tests/e2e/cluster/conftest.py`'s `_require_cluster` (autouse, session-scoped, lines 79-102)
**Apply to:** `tests/e2e/vault/conftest.py` — identical shape, so a developer without a cluster running sees one clear skip, not a wall of connection-refused errors across seven new test files.

### `helm/versions.env` as the single source of every chart/tool version literal
**Source:** `helm/versions.env`'s own header comment (lines 1-12) + `tests/policy/test_pinned_tool_versions_agree.py` (confirmed to enforce agreement, not fully read this session) + `scripts/helm-install.sh`'s `version_var` parameter
**Apply to:** the new `VAULT_CHART_VERSION` entry — every other file (`helm/values/*/vault.yaml`, `scripts/stages/80-vault.sh`) must reference it by variable name, never repeat the literal.

### D-06 profile-divergence: exactly three axes (replicas, resource sizing, monitoring), any 4th argued explicitly in the header comment
**Source:** `helm/values/local/minio.yaml` / `helm/values/ci/minio.yaml` header comments, and `helm/values/local/airflow.yaml` / `helm/values/ci/airflow.yaml`'s own "FOURTH DIVERGENCE AXIS, ARGUED (not smuggled)" precedent for `executor`
**Apply to:** `helm/values/local/vault.yaml` / `helm/values/ci/vault.yaml` — if CI keeps `server.dev.enabled: true` while local does not (a real, defensible 4th axis), it must be argued in the header exactly as `executor` is, not left as an unexplained diff `tests/policy/test_values_profiles.py` would need to allowlist silently.

---

## No Analog Found

Files/patterns with no close match in this codebase — the planner should treat these as genuinely new design surface, using `.planning/phases/05-vault-secrets-workload-identity/05-RESEARCH.md`'s Architecture Patterns / Code Examples sections as the primary source instead of an in-repo precedent:

| File / Pattern | Role | Data Flow | Reason |
|---|---|---|---|
| `scripts/vault-unseal.sh`'s local, gitignored, on-disk credential file (D-02) | script | file I/O (write) | Every existing credential-handling script in this repo (`etl-secrets.sh`, `minio-credentials.sh`, `airflow-metadata-secret.sh`) explicitly states, as an invariant, that no value it generates or reads is EVER written to the working tree — D-02 deliberately, narrowly overrides that invariant for Vault's own init output (unseal keys/root token), which Vault only ever reveals once. This is a first-of-its-kind exception in this repo, not an extension of an existing pattern — write it with the same trust-model rigor these scripts already model (explicit header comment naming exactly what is stored, where, and why it cannot be avoided), but do not expect an existing file to copy from. |
| `tests/e2e/vault/test_unseal_survives_restart.py` | test (e2e) | event-driven (component restart) | No existing test in `tests/e2e/cluster/` or `tests/e2e/slice/` restarts a live component and re-asserts state; the closest cousin is `scripts/cluster-rebuild.sh`'s timed teardown/recreate, which is an operator script, not a test, and rebuilds the WHOLE cluster rather than one pod. |
| `tests/e2e/vault/test_dev_secrets_reproducible.py` (SEC-13) | test (e2e) | n/a | This repo's existing answer to "dev secrets are marked/isolated/reproducible" is a STATIC policy test (`tests/policy/test_workflow_secrets.py`'s D-14 section) plus a manual `cluster-rebuild` re-run — not a dedicated live e2e test. Writing one is legitimate but has no direct precedent to copy structure from; it will likely need to invoke `scripts/vault-bootstrap.py`/`vault-unseal.sh` twice against the same cluster and assert idempotency, closer in spirit to `scripts/etl-secrets.sh`'s own "re-running ensure is a no-op" claim than to any existing pytest file. |

## Metadata

**Analog search scope:** `packages/dataplat/src/dataplat/` (secrets, errors, storage), `kubernetes/`, `helm/values/{local,ci}/`, `helm/versions.env`, `scripts/` (all `.sh` and `.py`), `scripts/stages/`, `Makefile`, `docs/adr/`, `tests/policy/`, `tests/e2e/cluster/`, `tests/unit/test_secrets_resolver.py`, `tests/unit/test_csv_processor_cli.py`, `airflow/dags/_common/kpo.py`
**Files scanned/read directly:** 27 (all listed above), plus `.gitignore`, `pyproject.toml` (root, for pytest markers), `packages/dataplat/pyproject.toml`, `tests/policy/test_pinned_tool_versions_agree.py` (grep-only, existence/purpose confirmed)
**Pattern extraction date:** 2026-08-14
