# Vault unavailable — sealed after a restart, connections 404

Sourced from [`.planning/debug/resolved/wait-for-files-stuck-task.md`](../../.planning/debug/resolved/wait-for-files-stuck-task.md)
(2026-08-16). Every claim below is that incident's own `resolution.root_cause`/`fix`/`verification`,
restated for an operator audience.

## Symptoms

`wait_for_files` (or another Vault-`Connection`-dependent task, most often a deferrable sensor)
intermittently sits in `up_for_retry` with no live pod backing it. Because this platform runs
`max_active_runs=1` per ingestion DAG, a single stuck task blocks all new file discovery for that
DAG until it resolves. `airflow connections get minio_default` returns a plain `Connection not
found.` — indistinguishable, at the log level, from a genuinely-missing connection.

## Diagnosis

This platform's Vault deployment is deliberately single-key Shamir + file storage with **no
auto-unseal** (D-02, `docs/secrets-architecture.md` §6) — a restart of the `vault-0` pod, or of the
host it runs on, always reseals it, and nothing reopens it automatically. While sealed, Airflow's
`VaultBackend` cannot resolve any `Connection` it serves (including `minio_default`), and Airflow's
secrets-backend chain surfaces that failure as a plain 404 rather than a distinct "backend
unavailable" error.

Confirm directly:

```bash
kubectl -n vault exec vault-0 -- vault status
```

`Sealed: true` (with `Initialized: true`) confirms this exact scenario. `Initialized: false` is a
different, more serious problem — see the note in Recovery below.

The task itself is not infinitely stuck: it is honoring its own configured exponential-backoff
`retry_delay` (`TaskInstance.next_retry_datetime()`), and will keep retrying — and keep failing,
for the same reason — until `max_tries` exhausts. Every retry cycle Vault stays sealed costs one
`DagRun`'s entire retry-exhaustion window of blocked discovery.

## Recovery

Run this platform's own documented, idempotent recovery procedure — no code change:

```bash
make vault-unseal
# equivalently: python scripts/vault-unseal.py
```

This reads the existing key material from `.secrets/vault-init.json` and unseals the SAME Vault
instance — it does not create a new one. Confirm the script reports **"unsealed"**, not
**"initialized"**: "initialized" would mean Vault's storage was actually lost (a materially
different, worse situation requiring a full `make vault-bootstrap` re-run afterward — see
`docs/secrets-architecture.md` §6's "SEC-13's other half" for that recovery path).

## Reprocessing

No manual re-drive is needed. The stuck task retries automatically at its own already-computed
`next_retry_datetime` once Vault answers again — confirmed live within ~40 seconds of running
`make vault-unseal` — and the `DagRun` then progresses through its remaining tasks to `success`
with zero manual task-state intervention.

## Verification

1. `vault status` reports `Sealed: false`, with the **same `Cluster ID`** as before the incident —
   this proves genuine storage recovery, not a fresh re-initialization that lost prior secrets.
2. `airflow connections get minio_default` succeeds (previously "Connection not found." on every
   attempt).
3. The previously-stuck task instance transitions out of `up_for_retry` without manual state
   clearing, and its `DagRun` reaches `state=success`.
4. Confirm it is not a one-off: watch 2-3 subsequent scheduled ticks of the same DAG complete
   cleanly, not just the one that was stuck.
5. `tests/e2e/vault/test_rotation.py` and the rest of `tests/e2e/vault/` (`-m cluster`) should pass
   cleanly against the now-unsealed Vault — a useful broader confirmation beyond the one DAG.
