# MinIO unavailable — object storage unreachable, discovery fails clearly, retriggers cleanly

Sourced from `tests/e2e/chaos/test_minio_unavailable.py` (D-25/D-41) — this scenario has no
matching real incident in `.planning/debug/resolved/*.md`, so D-41's own rule applies: write it
from the chaos test's designed fault-injection mechanism, then run it live and capture the actual
observed output before writing. Every symptom below (error type, exception chain, task file/line)
was captured live against this platform's real cluster on 2026-08-23 by reproducing the test's
own fault sequence manually against `csv_ingest_orders` (`list_matched_keys` failing while
`wait_for_files` had already succeeded, `deployment/minio` scaled to zero replicas, then restored)
— not read only from the test's source code.

## Symptoms

The `list_matched_keys` task (`_common/integrity_gate.py`, a plain `@task`, `retries=0` — no
backoff to wait out) fails on its **first and only attempt**, well within a minute. Downstream
tasks the DagRun's own `gate`/`discover`/`stage` chain depends on go `upstream_failed`, and the
whole `DagRun` reaches `failed` quickly and cleanly — not a silent hang. Live-captured error text
(exact, from the failing pod's own log):

```
EndpointConnectionError: Could not connect to the endpoint URL:
"http://minio.data.svc.cluster.local:9000/raw?list-type=2&prefix=orders%2F&delimiter=&start-after=&encoding-type=url"
  ...caused by...
NewConnectionError: Failed to establish a new connection: [Errno 111] Connection refused
  ...caused by...
ConnectionRefusedError: [Errno 111] Connection refused
```

The failure surfaces inside `list_matched_keys` (`dags/_common/integrity_gate.py:111`) via
`airflow.providers.amazon.aws.hooks.s3.S3Hook.list_keys` — `boto3`/`botocore`'s pagination call
against the `minio_default` connection's endpoint. Because `list_matched_keys` has no `retries`
override (Airflow's default `retries=0`), there is no multi-minute retry-backoff window to wait
out before the `DagRun` fails — live-observed total time from MinIO going to zero replicas to the
task itself failing: well under 30 seconds.

## Diagnosis

Confirm MinIO's own Deployment state directly:

```bash
kubectl -n data get deployment/minio
kubectl -n data get pods -l app.kubernetes.io/name=minio  # empty when at 0 replicas
```

`deployment/minio` reporting `0/0` `AVAILABLE`/`READY` (or genuinely zero backing pods) confirms
this exact scenario — kube-proxy's own empty-endpoints handling produces the immediate
`ConnectionRefusedError` above rather than a slow TCP-timeout hang, so the failure signature is
clean and attributable, not ambiguous.

Distinguish this from a genuine credential/auth problem (which would surface as a `botocore`
`ClientError` with an HTTP 403/`AccessDenied`, not a connection-level `EndpointConnectionError`) —
the exception chain above is specifically a *transport*-level failure, meaning MinIO itself is
unreachable, not merely rejecting the request.

Because `list_matched_keys` runs before `discover`, `meta.files` never gets a row for any
in-flight file while MinIO is down — live-confirmed via direct query
(`SELECT file_id FROM meta.files f JOIN meta.datasets d ... WHERE object_uri = ...` returned no
row for the whole outage window). This matters for Reprocessing below: nothing needs to be
"undone," because nothing was ever partially recorded.

## Recovery

Restore MinIO's replica count:

```bash
kubectl -n data scale deployment/minio --replicas=1
kubectl -n data wait --for=condition=Available deployment/minio --timeout=180s
```

`--for=condition=Available` (not a StatefulSet `readyReplicas` check) is the correct wait
condition — the official `minio/minio` chart's `mode: standalone` renders a **Deployment**, not a
StatefulSet, confirmed live (`kubectl -n data get statefulset,deployment` shows
`deployment.apps/minio` and no matching StatefulSet). Live-measured this session: MinIO reports
`Available` again in well under a minute of scaling back to 1 replica.

## Reprocessing

No manual re-drive of any partial state is needed, because — per Diagnosis above — `list_matched_
keys` fails before `discover` ever runs, so no `meta.files`/`meta.batches` row exists yet for the
outage-window file. The **same** already-uploaded object is still sitting in `raw/`, entirely
unconsumed. A **fresh trigger** of the same DAG (not the original DagRun's own retry — it has
none, `retries=0`) picks the file up as if for the first time:

```bash
kubectl -n airflow exec deploy/airflow-api-server -- airflow dags trigger <dag_id>
```

Live-confirmed this session: re-triggering after MinIO's restoration re-ran `list_matched_keys`
successfully, `discover` then registered the file in `meta.files` with `duplicate_of_file_id IS
NULL` — genuinely new, not a duplicate — confirming the recovery path is a clean fresh discovery,
not a stuck retry needing manual intervention.

## Verification

1. `kubectl -n data get deployment/minio` reports `1/1` `AVAILABLE`.
2. A fresh trigger of the affected DAG reaches `list_matched_keys: success` (previously `failed`
   on its one and only attempt).
3. `SELECT file_id, duplicate_of_file_id FROM meta.files f JOIN meta.datasets d ON d.dataset_id =
   f.dataset_id WHERE d.dataset_name = '<dataset>' AND f.object_uri = 's3://raw/<key>'` now
   returns exactly one row with `duplicate_of_file_id IS NULL` — live-confirmed this session
   (`file_id=4`, `duplicate_of_file_id` NULL, for the same object that had zero rows during the
   outage).
4. `tests/e2e/chaos/test_minio_unavailable.py -m cluster` is this scenario's own permanent,
   automated regression proof — its own assertions cover everything above plus the full
   pipeline's eventual `SUCCEEDED` state and exact row count, a stronger bar than this runbook's
   own live manual reproduction needed to establish the fault/recovery signature.
