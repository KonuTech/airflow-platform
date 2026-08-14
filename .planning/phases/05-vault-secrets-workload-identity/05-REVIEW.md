---
phase: 05-vault-secrets-workload-identity
reviewed: 2026-08-14T00:00:00Z
depth: standard
files_reviewed: 34
files_reviewed_list:
  - airflow/dags/_common/kpo.py
  - docs/adr/0009-openbao-licence-escape-hatch.md
  - docs/adr/README.md
  - docs/secrets-architecture.md
  - helm/values/ci/airflow.yaml
  - helm/values/ci/vault.yaml
  - helm/values/local/airflow.yaml
  - helm/values/local/vault.yaml
  - helm/versions.env
  - kubernetes/namespaces.yaml
  - packages/csv-processor/src/csv_processor/cli.py
  - packages/dataplat/pyproject.toml
  - packages/dataplat/src/dataplat/errors.py
  - packages/dataplat/src/dataplat/secrets/resolver.py
  - scripts/stages/75-etl.sh
  - scripts/stages/80-vault.sh
  - scripts/vault-audit-tail.py
  - scripts/vault-bootstrap.py
  - scripts/vault-unseal.py
  - scripts/wait-for.sh
  - tests/e2e/vault/__init__.py
  - tests/e2e/vault/conftest.py
  - tests/e2e/vault/test_airflow_backend.py
  - tests/e2e/vault/test_audit_log.py
  - tests/e2e/vault/test_dev_secrets_reproducible.py
  - tests/e2e/vault/test_negative_auth.py
  - tests/e2e/vault/test_positive_auth.py
  - tests/e2e/vault/test_rotation.py
  - tests/e2e/vault/test_unseal_survives_restart.py
  - tests/policy/test_no_stale_secrets.py
  - tests/policy/test_offline_gate_stays_offline.py
  - tests/policy/test_values_profiles.py
  - tests/unit/test_csv_processor_cli.py
  - tests/unit/test_secrets_resolver.py
findings:
  critical: 2
  warning: 7
  info: 2
  total: 11
status: issues_found
---

# Phase 05: Code Review Report

**Reviewed:** 2026-08-14T00:00:00Z
**Depth:** standard
**Files Reviewed:** 34
**Status:** issues_found

## Summary

This phase's steady-state design is sound: the `vault://` opaque-reference scheme,
the exact non-wildcard ServiceAccount role bindings, the KV v2 least-privilege
policies (`etl/data/analytics-db`, `etl/data/minio` as exact paths, not globs), the
audit-log tooling's deliberate refusal to ever read a `request`/`response` body, and
the e2e test suite's non-vacuity controls are all well-built and match what the
accompanying documentation claims for the **already-bootstrapped, currently-live**
cluster.

The defects found here are concentrated in two places the "already-live" framing
doesn't cover: (1) **`scripts/vault-bootstrap.py`'s idempotent "ensure" functions
converge on presence, not on the target state** — a changed policy body or a changed
namespace/policy-list binding on an existing role silently fails to apply on re-run,
undermining the "least-privilege is maintainable over time" property this phase is
supposed to deliver; and (2) **the bootstrap script's hardcoded dependency on three
Kubernetes Secrets that no longer exist anywhere in this codebase's automation**,
which breaks `make vault-bootstrap` on a genuinely fresh cluster — directly
contradicting SEC-13 and this document's own claimed reproducibility. A smaller set
of warnings covers incomplete exception handling in the new Vault-auth code path
(`resolve_secret()` / `_build_common()`), a crash risk in the audit-tail tool on
structurally-valid-but-null JSON fields, and a version-pinning gap for the Vault
server image relative to this repo's own enforced convention.

Not re-flagged: `tls_disable = 1`, the single-share Shamir unseal, and the BUSL-1.1
licence acceptance. All three are already argued in depth with a named migration
trigger (ADR-0009, `docs/secrets-architecture.md` §2/§6, T-05-02/T-05-05) — re-listing
an already-reasoned, explicitly-accepted risk here would not add information.

## Critical Issues

### CR-01: `vault-bootstrap.py` cannot bootstrap a genuinely fresh cluster — its only credential source was deleted

**File:** `scripts/vault-bootstrap.py:416-463` (`_ensure_etl_secrets`) and `scripts/vault-bootstrap.py:466-504` (`_ensure_airflow_secrets`)
**Issue:**

`_ensure_etl_secrets`/`_ensure_airflow_secrets` populate `etl/analytics-db`,
`etl/minio`, and `airflow/connections/minio_default`'s KV *values* by reading three
Kubernetes Secrets — `csv-processor-db`, `csv-processor-s3` (namespace `etl`) and
`airflow-minio-connection` (namespace `airflow`) — via `_kubectl_get_secret_field`
(lines 367-413), on the branch taken whenever the corresponding Vault KV path does
not yet exist (`except hvac.exceptions.InvalidPath:`, lines 434, 451, 492).

`scripts/etl-secrets.sh` was the **only** script that ever created those three
Secrets, and it was deleted outright in plan 05-03 once the migration completed
(confirmed: `find . -iname etl-secrets.sh` returns nothing; every remaining
reference to it in this repo is a comment/docstring, not a creation site — verified
by grepping every `.sh`/`.py`/`.yaml`/`Makefile` in the repo). `scripts/stages/75-etl.sh`'s
own header comment documents the deletion explicitly. No other script creates these
three specific names — `scripts/minio-credentials.sh` creates `minio-root`/`minio-app`
(different names, different fields: `rootUser`/`rootPassword`/`secretKey`), and
`scripts/airflow-metadata-secret.sh` creates `airflow-metadata`/`airflow-fernet-key`/
`airflow-api-secret-key` (unrelated to MinIO).

Consequence: on a genuinely fresh cluster (`make cluster-down && make cluster-up`,
which destroys the kind nodes and their PVCs, then `make vault-unseal &&
make vault-bootstrap`), Vault has no KV data yet, so `_ensure_etl_secrets` takes the
`InvalidPath` branch, calls `_kubectl_get_secret_field(..., name="csv-processor-db",
...)`, which shells out to `kubectl get secret -n etl csv-processor-db` — a Secret
that is never created by any current stage script — and raises `RuntimeError`. This
propagates out of `bootstrap()` to `main()`'s `except (RuntimeError,
hvac.exceptions.VaultError)` handler, which prints an error and returns exit code 1.
By this point `bootstrap()` has already run every earlier step successfully (KV
mounts, kubernetes auth method, both policies, both roles, the audit device — see
the call order at lines 523-604), so Vault is left **partially bootstrapped: every
structural object exists, but no credential value is ever written**. Every KPO pod's
`resolve_secret("vault://etl/...")` and Airflow's `VaultBackend` lookup of
`minio_default` then fail — the entire platform is unusable after a cluster rebuild,
with no documented recovery path.

This directly contradicts `docs/secrets-architecture.md`'s own SEC-13 claim (see
WR-07 below) and this project's WSL2-realistic assumption that a cluster rebuild is
routine, not a rare edge case (`docs/adr/0009-openbao-licence-escape-hatch.md`'s own
"a cluster restart is a realistic every-morning event" framing, and
`test_dev_secrets_reproducible.py`'s own module docstring, which documents this
exact "clean checkout" path as the intended manual verification for SEC-13's other
half).

**Fix:** Point the two `_ensure_*_secrets` functions at Secrets that are actually
still created by live automation, mirroring the pattern
`scripts/airflow-metadata-secret.sh` already uses for the metadata DB (sourcing from
CNPG's own generated `airflow-db-app` Secret rather than a hand-rolled one):

```python
# etl/minio#secret_key: source from the still-live minio-app Secret
# (scripts/minio-credentials.sh), field "secretKey" -- not the deleted
# csv-processor-s3.
secret_key = _kubectl_get_secret_field(
    kubectl_context, namespace="data", name="minio-app", key="secretKey",
)

# etl/analytics-db#dsn: source from CNPG's own generated analytics-db-app
# Secret (helm/values/local/cnpg-analytics.yaml: "generates the
# analytics-db-app Secret itself, D-14") -- not the deleted csv-processor-db.
```

For `airflow/connections/minio_default#conn_uri`, either construct the URI from the
same `minio-app` Secret at bootstrap time (mirroring how the value was originally
assembled), or accept that this one field has no live source and make bootstrap
prompt for / generate it explicitly rather than silently depending on a Secret nothing
creates. Either way, add an e2e or policy test that actually exercises "Vault has no
KV data yet" (not just "Vault is already fully populated," which is all
`test_dev_secrets_reproducible.py` currently proves) so this class of regression is
caught automatically rather than only on a manual clean-checkout run.

### CR-02: `_ensure_policy()` never re-applies a changed policy body — least-privilege edits silently don't take effect

**File:** `scripts/vault-bootstrap.py:279-286`
**Issue:**

```python
def _ensure_policy(client: hvac.Client, name: str, policy_hcl: str) -> None:
    """(c)/(e) Write a policy, if not already present under `name`."""
    existing = client.sys.list_policies()["data"]["policies"]
    if name in existing:
        print(f"policy {name}: already present")
        return
    client.sys.create_or_update_policy(name=name, policy=policy_hcl)
    print(f"policy {name}: created")
```

This checks only whether a policy **name** exists, never whether its live HCL body
matches the `policy_hcl` argument this run is passing. If `_CSV_PROCESSOR_POLICY` or
`_AIRFLOW_POLICY` (lines 86-93) is edited in a future change — for example, to
**narrow** access after discovering a mistake, which is exactly the kind of
least-privilege correction this platform's own threat model exists to support — a
re-run of this idempotent bootstrap against the already-bootstrapped, live Vault
(the script's own documented, normal way to apply a change) silently prints "already
present" and skips the write. The old, wider policy stays live. Nothing in this
script, and nothing in `tests/e2e/vault/test_dev_secrets_reproducible.py`'s
`_snapshot()` (which captures roles, mounts, audit devices and KV versions, but never
a policy's own body — `tests/e2e/vault/test_dev_secrets_reproducible.py:100-112`),
would ever surface that the intended tightening did not take effect.

This is a real regression relative to this same file's own precedent:
`_ensure_kubernetes_role` (lines 289-354) was specifically extended in plan 05-03 to
detect and correct drift in `bound_service_account_names` for exactly this reason
("plan 05-03's whole point is correcting the `airflow` role's plan-05-01-era best
guess... so this now reads the EXISTING role's live binding and re-writes it... only
when it differs"). The identical problem exists one level down, in the policy body
itself — the actual permission grant — and was not given the same treatment.

**Fix:** Compare the live policy body against `policy_hcl` (e.g.
`client.sys.read_policy(name=name)["data"]["rules"]` or the equivalent `hvac` call)
and re-write when it differs, using the same "already present" / "drifted -- correcting"
print-branch shape `_ensure_kubernetes_role` already establishes:

```python
def _ensure_policy(client: hvac.Client, name: str, policy_hcl: str) -> None:
    existing = client.sys.list_policies()["data"]["policies"]
    if name in existing:
        current_hcl = client.sys.read_policy(name=name)["data"]["policy"]
        if current_hcl.strip() == policy_hcl.strip():
            print(f"policy {name}: already present")
            return
        print(f"policy {name}: body drifted -- correcting")
    client.sys.create_or_update_policy(name=name, policy=policy_hcl)
    print(f"policy {name}: {'updated' if name in existing else 'created'}")
```

## Warnings

### WR-01: `resolve_secret()`'s vault branch doesn't catch connectivity/filesystem errors, breaking its own fail-closed contract

**File:** `packages/dataplat/src/dataplat/secrets/resolver.py:33-55, 91-109`
**Issue:** `_vault_client()` (lines 33-55) does three things that can each raise an
exception type the caller (`resolve_secret`'s vault branch, lines 91-109) does not
handle: `os.environ["VAULT_ADDR"]` / `os.environ["VAULT_K8S_ROLE"]` raise `KeyError`
if unset; `token_path.read_text(...)` raises `OSError` (e.g. `FileNotFoundError`) if
the pod's ServiceAccount token isn't mounted; and `client.auth.kubernetes.login(...)`
can raise `requests.exceptions.ConnectionError`/`Timeout` (not a subclass of
`hvac.exceptions.VaultError`) if Vault is transiently unreachable — a realistic
scenario immediately after a `vault-0` restart or a DNS blip. `_vault_client()` is
called *inside* the vault branch's `try:` (line 99), but that branch only catches
`hvac.exceptions.VaultError` and `KeyError` (lines 104, 107). A `ConnectionError` or
`OSError` from `_vault_client()` propagates as a raw, undocumented exception type,
contradicting the module's own docstring: "A raw, unresolved reference string is
never returned from any code path; every unsupported case raises instead" (implying a
single, well-typed `SecretResolutionError` surface) and the module-level docstring's
"fails closed on everything else" framing.
**Fix:** Wrap the whole vault branch's body (including the `_vault_client()` call) in
a broader `except Exception as exc:` that still re-raises as `SecretResolutionError`,
or give `_vault_client()` its own try/except that converts `KeyError`/`OSError`/
`requests.exceptions.RequestException` into a `SecretResolutionError` at the source,
so every failure mode funnels through the one documented exception type.

### WR-02: `except KeyError` in the same block conflates two unrelated failure causes

**File:** `packages/dataplat/src/dataplat/secrets/resolver.py:107-109`
**Issue:** The `except KeyError as exc:` clause is written to catch
`secret["data"]["data"][field]` (an unknown field in a *successfully-read* secret),
but because `_vault_client()` is called in the same `try:`, a `KeyError` from
`os.environ["VAULT_ADDR"]`/`os.environ["VAULT_K8S_ROLE"]` (see WR-01) lands in the
same clause and is mis-reported as:
`"vault secret at {mount_point}/{path} has no field {field!r}"` — actively
misleading when the real cause is a missing environment variable on the pod, not a
missing field in an otherwise-successful Vault read.
**Fix:** Once WR-01 separates `_vault_client()`'s own failure modes from the KV-read
failure modes, this clause will only ever see genuine missing-field cases and the
message will be accurate again.

### WR-03: `discover()` lacks the WR-01 (04-REVIEW.md) catch-all-exception fix that `ingest()` already received

**File:** `packages/csv-processor/src/csv_processor/cli.py:154-208` (except clause at 197-205)
**Issue:** `ingest()` (lines 240-323) was fixed, per its own docstring, so that
`_write_xcom(_failure_receipt(doc))` runs for **any** exception, not only
`DataPlatformError` — the docstring cites 04-REVIEW.md's WR-01 finding by name.
`discover()`'s `_build_common()` call (line 171) is the exact same function, now
doing Vault authentication as its first action (this phase's own addition), and per
WR-01/WR-02 above that authentication path can raise a raw `KeyError`/`OSError`/
`ConnectionError` that is not a `DataPlatformError`. `discover()`'s except clause
(line 197) only catches `DataPlatformError`:

```python
    except DataPlatformError as exc:
        _write_xcom({"status": "FAILED", ...})
        raise
    finally:
        if pool is not None:
            pool.close()
```

There is no `except Exception:` fallback. A non-`DataPlatformError` failure inside
`_build_common()` (newly more likely because of the Vault-auth code path this phase
introduced) propagates with **no** `{"status": "FAILED", ...}` XCom payload ever
written — the pod still fails (Airflow still sees the non-zero exit code), but the
forensic detail this command's own docstring promises ("writes a
`{"status": "FAILED", ...}` payload for forensic `kubectl logs`/`cat` inspection...
before re-raising") is silently skipped for exactly the failure class this phase
added.
**Fix:** Add the same `except Exception:` clause `ingest()` has, writing the
`{"status": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)}`
payload before re-raising.

### WR-04: `_ensure_kubernetes_role()`'s drift correction only checks `bound_service_account_names`, not namespaces, policies, or TTLs

**File:** `scripts/vault-bootstrap.py:289-354` (comparison at 335-344)
**Issue:** The function accepts `bound_service_account_namespaces`, `policies`,
`ttl` and `max_ttl` as part of the role definition, but the drift check that decides
whether to skip the `create_role` call only compares `bound_service_account_names`:

```python
    current_sas = sorted(current.get("bound_service_account_names") or [])
    target_sas = sorted(bound_service_account_names)
    if current_sas == target_sas:
        print(f"role {name}: already present")
        return
```

If a future change widens/narrows `bound_service_account_namespaces` (e.g. adding a
second namespace) or the `policies` list attached to a role, while the SA-name set
stays the same, a re-run silently returns early and never applies the change —
the same class of silent-drift problem as CR-02, one level up (the role's own
authorization surface, not just the policy body it points at).
**Fix:** Extend the comparison to cover every field this function accepts:
`bound_service_account_namespaces`, `policies`, and (if Vault's role-read response
exposes them consistently) `ttl`/`max_ttl`, not just `bound_service_account_names`.

### WR-05: `vault-audit-tail.py` can crash on a structurally-valid entry with an explicit JSON `null` field

**File:** `scripts/vault-audit-tail.py:161-190`
**Issue:**

```python
def _format_entry(entry: dict[str, Any]) -> str:
    timestamp = entry.get("time", "?")
    path = entry.get("request", {}).get("path", "?")
    metadata = entry.get("auth", {}).get("metadata") or {}
```

`dict.get(key, default)` only substitutes `default` when `key` is **absent**, not
when it is present with value `null`. If a Vault audit-log entry ever carries
`"request": null` or `"auth": null` — plausible for pre-authentication or
error-path entries, i.e. exactly the denied-login rows this tool exists to render
clearly per its own module docstring's SEC-08 framing — `entry.get("request", {})`
returns `None`, and `.get("path", "?")` on `None` raises `AttributeError`.
`render()` (lines 193-217) calls `_format_entry(entry)` with no protection around
this call — only the JSON-decode step has a try/except (lines 211-215) — so a single
such entry crashes rendering of the **entire** requested tail window, directly
contradicting the module docstring's explicit promise: "this tool never crashes over
one bad line."
**Fix:** Guard against an explicit `null`, not just an absent key:

```python
path = (entry.get("request") or {}).get("path", "?")
metadata = (entry.get("auth") or {}).get("metadata") or {}
```

### WR-06: Vault's server/image version is not independently pinned, unlike every other component in this repo

**File:** `helm/versions.env:20-27`
**Issue:** Every other chart-based component in this file pins its chart version
**and** its deployed image/app version as two separate keys —
`MINIO_CHART_VERSION` / `MINIO_IMAGE_TAG`, `AIRFLOW_CHART_VERSION` /
`AIRFLOW_IMAGE_TAG` — specifically because (per this project's own MinIO trap table)
letting a chart's default image tag silently determine the deployed binary version is
the exact anti-pattern this project's version-pinning discipline exists to prevent.
`VAULT_CHART_VERSION=0.34.0` (line 27) has no companion `VAULT_IMAGE_TAG`/
`VAULT_VERSION` key, and neither `helm/values/local/vault.yaml` nor
`helm/values/ci/vault.yaml` sets `server.image.tag` (or equivalent) to override it.
The deployed Vault **server** binary version (`2.0.3` today, per chart `0.34.0`'s
default `appVersion`) therefore floats with whatever the chart's own default happens
to be, and a future `VAULT_CHART_VERSION` bump could silently change the running
Vault major/minor version with no corresponding, reviewable literal change anywhere
this project's own `test_pinned_tool_versions_agree.py`-style discipline would catch.
This is also a determinism gap relative to `.claude/CLAUDE.md`'s own constraint:
"Same source data + configuration + processor version yields the same logical
result... unavoidable non-determinism must be documented."
**Fix:** Add an explicit `VAULT_IMAGE_TAG=2.0.3` (or `VAULT_VERSION=2.0.3`) key to
`helm/versions.env`, and set `server.image.tag` in both values files from it (or via
whatever override mechanism the other pinned components use), matching the
MinIO/Airflow pattern.

### WR-07: `docs/secrets-architecture.md`'s SEC-13 reproducibility claim is very likely false, given CR-01

**File:** `docs/secrets-architecture.md:230-239`
**Issue:** This section states the manual verification procedure "run
`make cluster-down && make cluster-up && make vault-unseal && make vault-bootstrap &&
make vault-verify` from a clean checkout and confirm every Vault e2e test passes with
no manual intervention beyond those four commands" as the documented, expected
behavior for SEC-13's fresh-cluster half. Given CR-01, `make vault-bootstrap` will
fail on exactly that sequence, on the current codebase, because its two credential-
sourcing steps depend on Kubernetes Secrets that plan 05-03 deleted with no
replacement. This document's own header states a self-imposed rigor bar — "Phase 5's
own STRIDE threat register (T-05-12) treats an overstated claim in this document as a
threat to this platform's Core Value... nothing here is stated without a citation to
what actually proved it" — and nothing in the phase's SUMMARY/VALIDATION artifacts
indicates this exact four-command sequence was re-run *after* plan 05-03 deleted
`scripts/etl-secrets.sh` (05-04's own `test_dev_secrets_reproducible.py` only proves
re-run idempotency against an *already-bootstrapped* Vault, never a fresh one — see
that file's own docstring, "deliberately NOT run here").
**Fix:** Once CR-01 is fixed, re-run the documented four-command sequence from an
actual clean checkout and confirm it before leaving this claim as stated; until then,
mark this section as a known gap rather than a proven guarantee.

## Info

### IN-01: `.secrets/vault-init.json` has a brief default-permission window before `chmod`

**File:** `scripts/vault-unseal.py:189-201`
**Issue:**

```python
    _INIT_FILE.write_text(json.dumps(payload), encoding="utf-8")
    os.chmod(_INIT_FILE, 0o600)
```

Between `write_text()` and `chmod()`, the file briefly exists at the process's
default `umask`-derived permissions (commonly `0o644`) before being tightened to
`0o600`. Low practical risk given this is explicitly a single-user, local-only
WSL2 tool with no other untrusted local user in scope, but it's a cheap fix.
**Fix:** Create the file pre-restricted, e.g.
`fd = os.open(_INIT_FILE, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)` then
`os.fdopen(fd, "w")`, so the permissive window never exists.

### IN-02: Stale comments in `scripts/vault-bootstrap.py` reference the now-deleted `scripts/etl-secrets.sh` as if it still exists

**File:** `scripts/vault-bootstrap.py:26, 36, 375, 422, 473`
**Issue:** Several docstrings describe the source Secrets as "already created (Phase
4)" by `scripts/etl-secrets.sh` in the present tense, e.g. line 375: "the live
`csv-processor-db`/`csv-processor-s3` Kubernetes Secrets `scripts/etl-secrets.sh`
already created (Phase 4)." That script no longer exists (see CR-01) — these
comments are the same stale assumption baked into prose, and likely contributed to
CR-01 going unnoticed, since the code's own documentation asserts a source of truth
that is no longer there to check against.
**Fix:** Once CR-01 is fixed, update these comments to describe the new (live)
credential source rather than the deleted script.

---

_Reviewed: 2026-08-14T00:00:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
