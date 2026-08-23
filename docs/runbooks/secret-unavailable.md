# Secret unavailable — a specific Vault path is missing, distinct from Vault itself being down

No dedicated chaos test exists for this exact scenario (§89 item 16 is not one of QUAL-15's 11
named scenarios). Per the plan's own interfaces instruction, this runbook is written from two
real, live-verified sources: (a) `tests/e2e/chaos/test_vault_unavailable.py`'s own fault/recovery
mechanism for the **shared symptom class** (`resolve_secret()` failing at the Vault layer), and
(b) a live reproduction, performed on this platform's real cluster on 2026-08-23, of the
**distinguishing** case — a missing/never-created secret path against an otherwise healthy,
unsealed Vault. `docs/secrets-architecture.md` §5 (SEC-08) is the diagnostic tool that tells the
two apart in practice; this runbook cites its live-verified behavior directly.

## Symptoms

`resolve_secret()`'s `vault://<mount>/<path>#<field>` scheme (`packages/dataplat/src/dataplat/
secrets/resolver.py`) fails when the referenced path was never written — either because a
credential rotation never completed, a new workload's secret was never bootstrapped, or a typo in
the reference string names a path that does not exist. The symptom is a **404-shaped, not-found**
error, live-captured directly against this cluster's real Vault instance:

```
$ vault kv get etl/nonexistent-workload-secret
No value found at etl/data/nonexistent-workload-secret
(exit code 2)
```

Critically, this is a fast, clean failure — Vault itself answers immediately and correctly; it
just has nothing at that path. Contrast with `vault-unavailable.md`'s own symptom (Airflow's
`VaultBackend` surfacing a plain `Connection not found.` 404 for an EXISTING connection, because
the whole backend cannot resolve anything while sealed) — the two can look identical to an
operator glancing at a single failed task, which is exactly why the Diagnosis step below matters.

## Diagnosis

The distinguishing check, live-verified this session against the real cluster's Vault instance:

```bash
kubectl -n vault exec vault-0 -- vault status
```

- **`Sealed: true`** → this is `vault-unavailable.md`'s scenario (the whole backend is down),
  **not** this one. Follow that runbook instead.
- **`Sealed: false`** (confirmed live: `Initialized: true`, `Sealed: false` throughout this
  scenario's own reproduction) → Vault itself is healthy. Proceed to the path-specific check:

```bash
kubectl -n vault exec vault-0 -- env VAULT_TOKEN=<root-or-operator-token> \
  vault kv get <mount>/<path>
```

against the SAME path the failing workload's `vault://` reference names. A `No value found at
<mount>/data/<path>` (exit code 2) with Vault otherwise healthy confirms this exact scenario —
the path genuinely does not exist, distinct from an authorization denial (which would be a
`permission denied` on the request itself, not a not-found on a successful, authorized read) and
distinct from Vault being sealed/unreachable (which fails before ever reaching a specific path).

**SEC-08's audit log is the tool that makes this distinction unambiguous, live-confirmed this
session.** A successful read of an *existing* path produces a `response`-type audit entry whose
`response.data.data` field carries the (HMAC-redacted) secret payload; a read of a *missing* path
produces a `response`-type entry for the same request with **no `data` key present at all** —
directly observable via:

```bash
make vault-audit-tail
# or: kubectl -n vault exec vault-0 -- tail -n 20 /vault/audit/audit.log
```

This is the same audit device SEC-08 already proves live in `tests/e2e/vault/test_audit_log.py` —
"which workload read which path, when, and whether it succeeded" (`docs/secrets-architecture.md`
§5) answers this scenario's own diagnostic question directly: a distinct, attributable log entry
exists either way, so this is never a silent, unattributable failure.

## Recovery

Recovery depends on *why* the path is missing — this scenario has no single fix, unlike Vault
being sealed (`make vault-unseal` always applies there):

1. **A credential was never bootstrapped for this workload.** Run the platform's own idempotent
   bootstrap: `make vault-bootstrap` (`scripts/vault-bootstrap.py`) — it is safe to re-run against
   an already-initialized Vault; it only writes paths that are missing or explicitly out of date
   (see `docs/secrets-architecture.md` §6's "SEC-13's other half" for the exact idempotency
   guarantee this relies on).
2. **A rotation completed the delete-half but not the write-half of an update.** Re-run the
   specific rotation procedure documented in `docs/secrets-architecture.md` §4/`secret-rotation.md`
   for that credential, rather than a full bootstrap re-run.
3. **A typo in the `vault://` reference itself.** Fix the reference string in the calling code or
   Helm values — no Vault-side change is needed; the path the reference names was never supposed
   to exist under that spelling.

## Reprocessing

Once the path is populated (or the reference corrected), the calling workload's next attempt
resolves it live — `resolve_secret()` is a synchronous, per-call Vault read with no negative
caching (`AIRFLOW__SECRETS__USE_CACHE` is deliberately left off, `docs/secrets-architecture.md`
§4), so no restart, no cache invalidation, and no special re-drive mechanism is needed. A task
already `up_for_retry`/`failed` for this reason simply succeeds on its own next attempt (Airflow's
normal retry) or via a fresh manual trigger, exactly like any other resolved transient failure —
there is no partial state to clean up, since a failed `resolve_secret()` call never proceeds far
enough to write anything.

## Verification

1. `vault kv get <mount>/<path>` now returns the expected `Data` block instead of `No value found`.
2. The audit log's newest `response`-type entry for that exact path now carries a `response.data.
   data` field (live-confirmed shape: `{"data": {"data": {"<field>": "hmac-sha256:..."}}}`), not an
   absent `data` key.
3. The previously-failing workload's next attempt (Airflow retry or a fresh manual trigger)
   resolves the secret and proceeds past the point it was failing at.
4. `tests/e2e/vault/test_audit_log.py -m cluster` continues to pass — confirms the audit device
   this diagnosis depends on is itself still healthy and redacting correctly.
