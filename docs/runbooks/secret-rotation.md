# Secret rotation — two read patterns, one restart-free by design

Documents an already-built feature (SEC-09). This runbook deliberately cross-references
[`docs/secrets-architecture.md`](../secrets-architecture.md) §4 rather than re-describing the
mechanism — a second, driftable copy of the same explanation is exactly the risk this platform's own
threat register (T-11-36) flags for this pair of runbooks. See that document for the full mechanism
and citations.

## Symptoms

A credential in Vault needs to change — a scheduled rotation, a suspected exposure, routine
hygiene — and an operator needs to know which already-running workloads pick up the new value
automatically versus which need a new pod before they will.

## Diagnosis

Two distinct read patterns exist by design (`docs/secrets-architecture.md` §4):

- **Short-lived KPO/ETL pods** (`csv-processor` tier) resolve every `vault://` reference **once**,
  at process start. For this tier, "rotation" means the **next** pod — there is no running process
  to demonstrate live pickup against, and no way for an already-running pod to see a new value.
- **Airflow's long-running components** (`airflow` tier) read `VaultBackend` live, on every
  `BaseHook.get_connection()` call (`AIRFLOW__SECRETS__USE_CACHE` defaults to `False`) — a rotated
  value is observed by the SAME already-running pod's very next lookup, no restart needed.

## Recovery

Write the new value to the relevant Vault KV v2 path:

```bash
vault kv put <mount>/<path> <field>=<value>
```

For the `airflow` tier, nothing else is required. For the `csv-processor` tier, no currently-running
pod will observe the change — only the next task's freshly-launched pod will. That is expected
behavior, not a gap to work around with a forced pod restart.

## Reprocessing

Not applicable in the data sense — rotating a credential is an identity/configuration change, not a
data correction, and implies no re-drive of already-loaded rows.

## Verification

`tests/e2e/vault/test_rotation.py::test_rotating_minio_default_is_observed_with_no_restart` is this
platform's own live, standing proof of the `airflow`-tier claim above — it rotates
`airflow/connections/minio_default`'s `conn_uri`, reads it via the CLI against the SAME running
`airflow-api-server` pod before and after, and asserts the change is observed with zero pod
restart, then restores the original value in a `finally` block. Run it after any real rotation to
confirm the mechanism still holds:

```bash
uv run --frozen --group cluster pytest tests/e2e/vault/test_rotation.py -m cluster -q
```
