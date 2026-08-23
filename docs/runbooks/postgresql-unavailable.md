# PostgreSQL (analytical database) unavailable — connection refused, retries then recovers

Sourced from `tests/e2e/chaos/test_database_unavailable.py` (D-25/D-41) — no matching real incident
exists in `.planning/debug/resolved/*.md`, so this is written from the chaos test's own designed
fault-injection mechanism (CNPG `Cluster` hibernation, not a NetworkPolicy — see the test's own
module docstring for why: this cluster's CNI does not enforce NetworkPolicy at all, live-verified
twice), then run live and captured before writing. On 2026-08-23, the exact `cnpg.io/hibernation`
annotation cycle the test performs was reproduced manually against this platform's real analytical
CNPG cluster, capturing the actual observed connection-failure signature from a live pod's own
log — matching the failure class `test_database_unavailable.py`'s own `_CONNECTION_FAILURE_
SIGNATURES` list checks for ("connection refused", "could not connect", "operationalerror").

**Live-capture note:** the reproduction captured this exact signature from a `stage` task pod (a
mapped `KubernetesPodOperator` task processing a different, already-in-flight file on this shared
cluster at the time), not literally the `discover` task the automated test targets — both tasks
call the identical `csv_processor.cli._build_common()` → `psycopg_pool.ConnectionPool.open(wait=
True)` connection-establishment path, so the failure signature is the same either way. `discover`
is the FIRST task in the DAG's own graph to touch the analytical database (per the test's own
design) and is the correct one to point to for "which task fails first" — this note exists only so
a reader comparing this runbook against the automated test's own assertions understands the exact
provenance of the captured log text below.

## Symptoms

A DB-touching task (`discover`, or any other stage of the pipeline touching PostgreSQL via
`csv_processor.cli._build_common()`) fails with a **connection pool timeout**, live-captured
verbatim from a real pod's log:

```
psycopg_pool.PoolTimeout: pool initialization incomplete after 30.0 sec
```

with the underlying repeated cause, also live-captured:

```
[base] error connecting in 'pool-1': connection failed: connection to server at "10.96.100.222",
port 5432 failed: Connection refused
	Is the server running on that host and accepting TCP/IP connections?
```

The pod's `base` container exits with `exit_code: 1` (Kubernetes `reason: Error`); the task
reaches **`up_for_retry`**, not a bare crash or silent hang — `discover`/`stage` both carry
`retries` (2 and 3 respectively, `retry_exponential_backoff=True`), unlike `list_matched_keys` in
the MinIO-unavailable scenario, which has none. The whole failure-to-`up_for_retry` cycle,
live-measured: well under a minute from the pod appearing to the container exiting.

## Diagnosis

Confirm the analytical CNPG cluster's actual state directly:

```bash
kubectl -n data get pods -l cnpg.io/cluster=analytics-db
kubectl -n data get cluster/analytics-db -o jsonpath='{.metadata.annotations.cnpg\.io/hibernation}'
```

Zero backing pods for the cluster's own label selector, or the `cnpg.io/hibernation: "on"`
annotation present, both confirm this exact scenario. Live-verified this session: with hibernation
active, `kubectl get endpoints analytics-db-rw` returns an empty `subsets` list — kube-proxy's own
empty-endpoints handling produces the immediate `Connection refused` above (not a slow TCP
SYN-retry timeout), so the failure is clean and attributable, the same design property MinIO's own
`deployment/minio` scale-to-zero exhibits.

Distinguish this from a credential/auth problem: this failure is a **transport-level**
`ConnectionRefusedError`, not a PostgreSQL-level authentication rejection (which would instead
surface as `password authentication failed` or similar from a server that DID accept the TCP
connection). The database process itself is unreachable, not merely rejecting the login.

## Recovery

Restore the CNPG `Cluster` by removing the hibernation annotation entirely (not merely setting it
to `"off"` — the test's own live-verified reasoning: this keeps the `Cluster` CR's annotations
byte-identical to their pre-fault state):

```bash
kubectl -n data annotate cluster analytics-db cnpg.io/hibernation-
kubectl -n data wait --for=condition=Ready pod -l cnpg.io/cluster=analytics-db --timeout=180s
```

Live-measured this session: the instance pod reaches `Ready` again within seconds of annotation
removal — well inside the 180s budget. Because hibernation retains the cluster's PVC untouched
(CNPG terminates only the instance pod, never the storage), this is a genuine zero-data-loss
restoration, not a fresh reinitialization — confirmed live via `SELECT count(*) FROM
meta.ingestion_runs` returning the same pre-outage row count immediately after recovery, not a
reset-to-zero count.

## Reprocessing

No manual re-drive is needed. `discover`/`stage` both carry Airflow's own automatic retry with
exponential backoff — the SAME task instance that hit `up_for_retry` resumes on its own
already-computed `next_retry_datetime` once the database answers again, no second file upload and
no manual DAG intervention required. If `discover` itself is the task that failed (rather than a
downstream stage), `meta.files` will have no row for the affected file for the whole outage window
— exactly mirroring `minio-unavailable.md`'s own "nothing partial was ever recorded" property,
since `discover` is the first task to write to `meta.files` at all.

## Verification

1. `kubectl -n data get pods -l cnpg.io/cluster=analytics-db` shows the instance pod `Running`/
   `Ready`, with no `cnpg.io/hibernation` annotation present.
2. `SELECT count(*) FROM meta.ingestion_runs` (or any other simple query) succeeds again — the
   database answers queries, not merely accepts TCP connections.
3. The previously-`up_for_retry` task instance transitions to `success` on its own scheduled
   retry, with no manual state clearing.
4. For a `discover`-level failure specifically: `SELECT file_id, duplicate_of_file_id FROM
   meta.files ...` for the affected `object_uri` now returns exactly one row with
   `duplicate_of_file_id IS NULL`.
5. `tests/e2e/chaos/test_database_unavailable.py -m cluster` is this scenario's own permanent,
   automated regression proof, asserting the full `discover`-specific failure/recovery/row-count
   chain this runbook's own live manual reproduction corroborates from an adjacent task.
