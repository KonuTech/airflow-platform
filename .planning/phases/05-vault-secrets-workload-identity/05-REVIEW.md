---
phase: 05-vault-secrets-workload-identity
reviewed: 2026-08-14T21:01:07Z
depth: standard
files_reviewed: 35
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
  - tests/unit/test_vault_bootstrap.py
findings:
  critical: 0
  warning: 7
  info: 2
  total: 9
status: issues_found
---

# Phase 05: Code Review Report

**Reviewed:** 2026-08-14T21:01:07Z
**Depth:** standard
**Files Reviewed:** 35
**Status:** issues_found

## Summary

This is a re-review of the same file set an earlier `05-REVIEW.md` pass covered before
gap-closure plan 05-06 landed. Both of that earlier review's Critical findings were
independently re-verified against the current code and are now **confirmed fixed**:
`_ensure_policy()` (`scripts/vault-bootstrap.py:288-324`) now reads the live policy body
back via `read_policy()` and re-applies on drift (the former CR-02), and
`_ensure_etl_secrets`/`_ensure_airflow_secrets` (`scripts/vault-bootstrap.py:565-700`) no
longer depend on the three deleted Kubernetes Secrets — they source `etl/analytics-db`
from a fresh `kubectl exec`-driven `ALTER ROLE` against the live CNPG primary and
`etl/minio`/`airflow/connections/minio_default` from the live `data/minio-app` Secret
(the former CR-01), with a dedicated offline regression suite
(`tests/unit/test_vault_bootstrap.py`) now guarding both fixes. `docs/secrets-architecture.md`'s
SEC-13 section was also rewritten to honestly scope what was and wasn't re-proven (a
scoped Vault-only reinstall, not a full `kind delete cluster` rebuild), so the earlier
review's WR-07 (overstated reproducibility claim) is resolved too. No new Critical-tier
defect was found in this pass.

The steady-state design remains sound: the `vault://` opaque-reference scheme, the
non-wildcard ServiceAccount role bindings, the least-privilege KV v2 policies (exact
paths, never globs), the audit-tail tool's deliberate refusal to read `request`/`response`
bodies, and the e2e suite's non-vacuity controls are all well-built.

What remains open is a set of exception-handling and drift-detection gaps that were
already present before 05-06 and were out of that plan's stated scope (it targeted only
SEC-13/CR-01/CR-02): `resolve_secret()`'s `vault://` branch still does not catch every
exception type its own `_vault_client()` helper can raise, `discover()` still lacks the
catch-all-exception fix `ingest()` received in an earlier phase, `_ensure_kubernetes_role()`
still only drift-corrects one of the four fields it accepts, `vault-audit-tail.py` still
has an unguarded `None`-vs-absent-key crash risk, and Vault's own server/image version is
still the only component in `helm/versions.env` without a companion pinned version key.
Two additional, previously unflagged issues were found in this pass: a URI-parsing gap in
`resolve_secret()`'s `file://` branch that silently drops a path segment instead of
failing closed (currently unreachable — no call site uses `file://` yet), and a
concrete illustration of copy-paste drift between the four duplicated
`_port_forwarded_vault` helpers.

Not re-flagged: `tls_disable = 1`, the single-share Shamir unseal, and the BUSL-1.1
licence acceptance. All three are already argued in depth with a named migration trigger
(ADR-0009, `docs/secrets-architecture.md` §2/§6, T-05-02/T-05-05) — re-listing an
already-reasoned, explicitly-accepted risk here would not add information.

## Warnings

### WR-01: `resolve_secret()`'s vault branch doesn't catch every exception `_vault_client()` can raise

**File:** `packages/dataplat/src/dataplat/secrets/resolver.py:33-55` (`_vault_client`) and `:91-109` (the `vault://` branch of `resolve_secret`)
**Issue:** `_vault_client()` does three things, each of which can raise an exception
type the caller does not handle:

- `os.environ["VAULT_ADDR"]` (line 48) and `os.environ["VAULT_K8S_ROLE"]` (line 51) raise
  `KeyError` if either is unset on the pod.
- `token_path.read_text(encoding="utf-8")` (line 52) raises `OSError`
  (`FileNotFoundError` in the common case) if the projected ServiceAccount token isn't
  present at the expected path.
- `client.auth.kubernetes.login(...)` (lines 50-53) can raise
  `requests.exceptions.ConnectionError`/`Timeout` if Vault is transiently unreachable —
  a realistic scenario immediately after a `vault-0` restart. These are not subclasses
  of `hvac.exceptions.VaultError`; hvac only wraps Vault API *response* errors in that
  hierarchy, not transport-level failures from the underlying `requests` call.

`_vault_client()` is invoked *inside* the `try:` at line 99, but that block only catches
`hvac.exceptions.VaultError` (line 104) and `KeyError` (line 107). An `OSError` or a
`requests.exceptions.ConnectionError` from `_vault_client()` propagates as a raw,
undocumented exception type, contradicting the module's own docstring promise: "A raw,
unresolved reference string is never returned from any code path; every unsupported case
raises instead" (lines 68-76), and the module-level docstring's "fails closed on
everything else" framing (line 1).
**Fix:** Wrap the client-setup step separately so every failure mode funnels through the
one documented exception type:

```python
try:
    client = _vault_client()
except (KeyError, OSError) as exc:
    msg = f"vault client setup failed for ref {ref!r}: {exc}"
    raise SecretResolutionError(msg, context={"ref": ref}) from exc
try:
    secret = client.secrets.kv.v2.read_secret_version(mount_point=mount_point, path=path)
except hvac.exceptions.VaultError as exc:
    msg = f"vault read failed for ref {ref!r}: {exc}"
    raise SecretResolutionError(msg, context={"ref": ref}) from exc
```

and either add `requests.exceptions.RequestException` to both `except` clauses, or give
`_vault_client()` its own internal `try/except Exception` that re-raises a single,
already-typed error before this function ever sees it.

### WR-02: The `except KeyError` clause conflates two unrelated failure causes

**File:** `packages/dataplat/src/dataplat/secrets/resolver.py:107-109`
**Issue:** Because `_vault_client()` is called inside the same `try:` that the
`except KeyError` clause guards (see WR-01), a `KeyError` from
`os.environ["VAULT_ADDR"]`/`os.environ["VAULT_K8S_ROLE"]` — a missing environment
variable on the pod — lands in the clause written for `secret["data"]["data"][field]`
(an unknown field in an *already-successfully-read* secret) and is reported as:

```
vault secret at etl/analytics-db has no field 'dsn'
```

even when the real cause is a missing `VAULT_ADDR`/`VAULT_K8S_ROLE` env var and no
Vault call was ever made. This directly conflicts with this project's own stated Core
Value ("can be traced, explained... trusted") — a developer debugging a misconfigured
pod is pointed at the wrong root cause. `tests/unit/test_secrets_resolver.py` does not
cover this path either (`test_vault_scheme_wraps_a_missing_field_as_secret_resolution_error`
only exercises a genuinely-missing field on a successful read), so nothing currently
catches this regression.
**Fix:** Once WR-01 separates `_vault_client()`'s own failure modes from the KV-read
failure modes (moving the `_vault_client()` call out of the block guarded by
`except KeyError`), this clause will only ever see genuine missing-field cases and the
message becomes accurate again.

### WR-03: `discover()` still lacks the catch-all-exception fix `ingest()` already received

**File:** `packages/csv-processor/src/csv_processor/cli.py:152-208` (except clause at 197-205); compare `ingest()` at `:240-323` (except clauses at 300-319)
**Issue:** `ingest()`'s own docstring documents that it writes a FAILED `Receipt` "on
every exit path... for ANY exception -- not only `DataPlatformError`" (lines 251-254),
and its code has both an `except DataPlatformError:` (line 300) and an
`except Exception:` (line 303) clause to deliver that guarantee. `discover()` calls the
exact same `_build_common()` (line 171) — now doing Vault authentication as its first
action — but its except clause only catches `DataPlatformError`:

```python
except DataPlatformError as exc:
    _write_xcom({"status": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)})
    raise
finally:
    if pool is not None:
        pool.close()
```

There is no `except Exception:` fallback. A non-`DataPlatformError` failure anywhere in
`discover()`'s try body — `_build_common()` (see WR-01's uncaught exception types),
`load_config()`, `ConfigRegistry(pool).sync(...)`, `metadata.get_or_create_dataset(...)`,
or `discover_files(...)` — propagates with **no** `{"status": "FAILED", ...}` XCom
payload ever written. The pod still fails (Airflow still observes the non-zero exit
code), but the forensic detail `discover()`'s own docstring promises ("writes a
`{"status": "FAILED", ...}` payload for forensic `kubectl logs`/`cat` inspection...
before re-raising") is silently skipped for exactly this failure class.
**Fix:** Add the same `except Exception:` clause `ingest()` has:

```python
except DataPlatformError as exc:
    _write_xcom({"status": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)})
    raise
except Exception as exc:
    _write_xcom({"status": "FAILED", "error_type": type(exc).__name__, "error_message": str(exc)})
    raise
finally:
    if pool is not None:
        pool.close()
```

### WR-04: `_ensure_kubernetes_role()`'s drift correction only checks `bound_service_account_names`

**File:** `scripts/vault-bootstrap.py:327-392` (comparison at 365-376)
**Issue:** The function accepts `bound_service_account_namespaces`, `policies`, `ttl`
and `max_ttl` as part of a role's definition, but the drift check that decides whether
to skip the `create_role()` call only compares `bound_service_account_names`:

```python
current_sas = sorted(current.get("bound_service_account_names") or [])
target_sas = sorted(bound_service_account_names)
if current_sas == target_sas:
    print(f"role {name}: already present")
    return
```

If a future change widens/narrows `bound_service_account_namespaces` or the `policies`
list attached to a role while the SA-name set stays identical, a re-run of this
idempotent bootstrap against an already-bootstrapped Vault silently reports "already
present" and never applies the change — the same defect class `_ensure_policy()`
(lines 288-324) was specifically rewritten to fix (that function's own docstring, lines
298-300, claims it reuses "the same... convergence shape `_ensure_kubernetes_role`
already established for role bindings" — an overstatement, since only one of that
function's four accepted fields is actually drift-corrected). `tests/e2e/vault/test_dev_secrets_reproducible.py`'s
`_snapshot()` captures each role's *entire* live definition before/after a same-Vault
re-run, so it would catch this only if the target values themselves changed between two
runs — which a same-binary idempotency test structurally cannot exercise.
**Fix:** Extend the comparison to every field the caller can vary:

```python
if already_exists:
    current = client.auth.kubernetes.read_role(name=name)
    current_binding = (
        sorted(current.get("bound_service_account_names") or []),
        sorted(current.get("bound_service_account_namespaces") or []),
        sorted(current.get("policies") or []),
    )
    target_binding = (
        sorted(bound_service_account_names),
        sorted(bound_service_account_namespaces),
        sorted(policies),
    )
    if current_binding == target_binding:
        print(f"role {name}: already present")
        return
    print(f"role {name}: binding drifted -- correcting")
```

### WR-05: `vault-audit-tail.py` can crash on a structurally-valid entry with an explicit JSON `null` field

**File:** `scripts/vault-audit-tail.py:174-177` (`_format_entry`), unprotected call site at `:216` (`render`)
**Issue:**

```python
timestamp = entry.get("time", "?")
path = entry.get("request", {}).get("path", "?")

metadata = entry.get("auth", {}).get("metadata") or {}
```

`dict.get(key, default)` only substitutes `default` when `key` is **absent**, not when
it is present with value `null`. If a Vault audit-log entry ever carries
`"request": null` or `"auth": null` — plausible for pre-authentication or error-path
entries, exactly the denied-login rows this tool exists to render clearly per its own
SEC-08 framing — `entry.get("request", {})` returns `None`, and `.get("path", "?")` on
`None` raises `AttributeError`. The same applies to `entry.get("auth", {})`: the
trailing `or {}` on the `metadata` line only guards against `.get("metadata")` itself
returning `None`; it never runs if `entry.get("auth", {})` already raised. `render()`
(lines 193-217) calls `_format_entry(entry)` at line 216 with no protection — only the
JSON-decode step (lines 211-215) has a try/except — so a single such entry crashes
rendering of the **entire** requested tail window, contradicting the module docstring's
explicit promise: "this tool never crashes over one bad line."
**Fix:** Guard against an explicit `null`, not just an absent key:

```python
path = (entry.get("request") or {}).get("path", "?")
metadata = (entry.get("auth") or {}).get("metadata") or {}
```

### WR-06: Vault's server/image version has no companion pinned key, unlike every other component

**File:** `helm/versions.env:27` (compare `:23-26`)
**Issue:** Every other chart-based component in this file pins its chart version **and**
its deployed image/app version as two separate keys — `MINIO_CHART_VERSION` /
`MINIO_IMAGE_TAG`, `AIRFLOW_CHART_VERSION` / `AIRFLOW_IMAGE_TAG` — specifically because
(per this project's own documented MinIO trap) letting a chart's default image tag
silently determine the deployed binary version is the exact anti-pattern this project's
version-pinning discipline exists to prevent. `VAULT_CHART_VERSION=0.34.0` has no
companion `VAULT_IMAGE_TAG`/`VAULT_VERSION` key, and neither `helm/values/local/vault.yaml`
nor `helm/values/ci/vault.yaml` sets `server.image.tag` (both fully read for this
review — no `image:` block appears in either). The deployed Vault **server** binary
version (`2.0.3` today, per chart `0.34.0`'s default `appVersion`) therefore floats with
whatever the chart's own default happens to be; a future `VAULT_CHART_VERSION` bump could
silently change the running Vault major/minor version with no corresponding, reviewable
literal anywhere in this repo. This is also a determinism gap relative to
`.claude/CLAUDE.md`'s own constraint on documented, controlled non-determinism.
**Fix:** Add an explicit `VAULT_IMAGE_TAG=2.0.3` (or `VAULT_VERSION=2.0.3`) key to
`helm/versions.env`, and set `server.image.tag` (or the chart's equivalent override) in
both values files from it, matching the MinIO/Airflow pattern.

### WR-07: `resolve_secret()`'s `file://` branch silently drops a path segment instead of failing closed

**File:** `packages/dataplat/src/dataplat/secrets/resolver.py:85-90`
**Issue:**

```python
if parsed.scheme == "file":
    try:
        return Path(parsed.path).read_text(encoding="utf-8").strip()
    except OSError as exc:
        ...
```

`urlsplit()` treats everything between `scheme://` and the next `/` as the URI's
`netloc` (host), not part of `.path`. A `file://` reference with exactly two slashes
before a relative-looking segment — e.g. a plausible typo `file://vault/audit/foo`
instead of the correct `file:///vault/audit/foo` — parses to `netloc="vault"`,
`path="/audit/foo"`; `Path(parsed.path)` silently reads from `/audit/foo`, **dropping
the `vault` segment entirely**, rather than raising. This directly contradicts the
module's own stated design guarantee: "Any unrecognized scheme, and any malformed or
schemeless reference, raises rather than silently passing the raw reference through.
That fail-closed behavior is SEC-15's entire point" (module docstring, lines 6-11) — the
`file://` branch is the one scheme where a malformed reference is not rejected but
silently reinterpreted as a different path. `tests/unit/test_secrets_resolver.py`'s own
`test_file_scheme_returns_the_stripped_file_contents` only exercises the well-formed
three-slash form (`f"file://{secret_file}"` where `secret_file` is always an absolute
`tmp_path`), so this edge case is untested. **Currently unreachable in production:** a
repo-wide search confirms no call site anywhere in this codebase actually constructs a
`file://` reference today (only `env://` and `vault://` are used by `kpo.py`/`cli.py`),
so the practical blast radius is nil until a future caller adopts this scheme.
**Fix:** Reject any `file://` reference carrying a non-empty `netloc`, since a
well-formed local-file reference should never have one:

```python
if parsed.scheme == "file":
    if parsed.netloc:
        msg = f"malformed file:// ref (unexpected host component {parsed.netloc!r}): {ref!r}"
        raise SecretResolutionError(msg, context={"ref": ref})
    try:
        return Path(parsed.path).read_text(encoding="utf-8").strip()
    except OSError as exc:
        ...
```

## Info

### IN-01: `.secrets/vault-init.json` has a brief default-permission window before `chmod`

**File:** `scripts/vault-unseal.py:198-201`
**Issue:**

```python
_INIT_FILE.write_text(json.dumps(payload), encoding="utf-8")
os.chmod(_INIT_FILE, 0o600)  # noqa: PTH101
```

Between `write_text()` and `chmod()`, the file briefly exists at the process's default
`umask`-derived permissions (commonly `0o644`) before being tightened to `0o600`. Low
practical risk given this is explicitly a single-user, local-only WSL2 tool with no
other untrusted local user in scope, but the fix is cheap and this file holds the
cluster's Vault root token.
**Fix:** Create the file pre-restricted:

```python
fd = os.open(_INIT_FILE, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w", encoding="utf-8") as f:
    f.write(json.dumps(payload))
```

### IN-02: The duplicated `_port_forwarded_vault` helper has already drifted between its four copies

**File:** `scripts/vault-bootstrap.py:240-243`, `scripts/vault-unseal.py:183-186` vs. `tests/e2e/vault/conftest.py:193-198`, `tests/e2e/vault/test_unseal_survives_restart.py:125-130`
**Issue:** This repository deliberately duplicates small helpers rather than sharing
them through a library module (stated explicitly in several of these functions' own
docstrings). The two `scripts/*.py` copies' cleanup is:

```python
finally:
    proc.terminate()
    with contextlib.suppress(subprocess.TimeoutExpired):
        proc.wait(timeout=10)
```

while both `tests/e2e/vault/*.py` copies are:

```python
finally:
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
```

The script-side copies never escalate to `SIGKILL` if the `kubectl port-forward`
process ignores `SIGTERM` within 10s — the timeout is silently swallowed and the
process can be left running, orphaned, still holding its local port. This is a small,
concrete illustration of the exact copy-paste drift risk the "duplicate, don't share"
convention invites: two of the four copies already disagree on a real behavior, not
just formatting. Low impact (`kubectl port-forward` reliably honors `SIGTERM`), but
worth fixing given how many places this exact snippet is now pasted.
**Fix:** Add the same `except subprocess.TimeoutExpired: proc.kill()` escalation to
both `scripts/vault-bootstrap.py` and `scripts/vault-unseal.py`'s copies.

---

_Reviewed: 2026-08-14T21:01:07Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
