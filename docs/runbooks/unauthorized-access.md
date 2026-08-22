# Unauthorized access — denied at login, before any secret read is attempted

Documents an already-built feature (SEC-12). This runbook deliberately cross-references
[`docs/secrets-architecture.md`](../secrets-architecture.md) §2 rather than re-describing the
mechanism — a second, driftable copy of the same explanation is exactly the risk this platform's own
threat register (T-11-36) flags for this pair of runbooks. See that document for the full trust-
boundary table and citations.

## Symptoms

A concern that one workload might be able to read another workload's secrets — this needs a
definitive, provable answer, not a configuration re-read.

## Diagnosis

Vault's Kubernetes-auth role binding (`bound_service_account_names`) is the enforcement point: a
ServiceAccount not explicitly bound to a role is denied **at login**, before any KV read is even
attempted — there is no token to attempt a read with afterward.
`docs/secrets-architecture.md` §2 documents the exact per-identity policy scoping this platform
runs: `csv-processor`'s policy reaches only `etl/data/*`; `airflow`'s policy reaches only
`airflow/data/connections/*`; `dbt`'s policy is scoped identically to its own mount. None can read
another's.

## Recovery

If an audit finds a role bound too broadly (a wildcard, or an unintended extra ServiceAccount),
narrow `bound_service_account_names` to the exact identity list it should serve, then re-run the
negative test below to confirm the boundary holds again. Never "fix" an access problem by loosening
a policy path as a workaround — the entire model depends on exact, non-wildcard binds.

## Reprocessing

Not applicable in the data sense — this is an identity/access correction, not a data correction.

## Verification

`tests/e2e/vault/test_negative_auth.py` is this platform's own live, standing proof — run it after
any Vault role/policy change:

```bash
uv run --frozen --group cluster pytest tests/e2e/vault/test_negative_auth.py -m cluster -q
```

It asserts, mutually and in both directions: `default` (namespace `etl`) is denied both the
`csv-processor` and `dbt` roles; `csv-processor` is denied the `dbt` role and `dbt` is denied the
`csv-processor` role; a ServiceAccount from an entirely different namespace (`airflow-scheduler`) is
denied the same way. Every case asserts `client.token is None` — no token is ever issued, not merely
"no useful read succeeded" — via `pytest.raises(hvac.exceptions.VaultError)` on the login call
itself. All cases must raise.
