# U1 — locally-built image pulls and runs via KubernetesPodOperator on kind

**Regenerated automatically by `tests/e2e/slice/test_smoke_and_idempotency.py::test_smoke_dag_xcom_contains_built_sha` — do not hand-edit.**

- Proven at: 2026-08-13T22:30:46.060804+00:00
- DAG: `smoke_kubernetes_pod`, task `print_version_to_xcom`
- DagRun: `e2e-u1-9c7e38c56ceb`
- Git SHA baked into the running image (`ENV GIT_SHA`,
  `docker/csv-processor/Dockerfile`): `180990c`

## Pass criterion (ROADMAP.md, Spikes table)

> The XCom contains the SHA that was built.

## Assertion proved

`xcom.value["git_sha"] == subprocess.run(["git", "rev-parse", "--short", "HEAD"]).stdout.strip()`
evaluated against the checkout this test ran from, read directly from the Airflow
metadata database's `xcom` table (`dag_id`, `run_id`, `task_id='print_version_to_xcom'`,
`key='return_value'`) after the triggered DagRun reached `state='success'`.

This is the permanent platform smoke test (ROADMAP.md's own words): `smoke_kubernetes_pod`
proves a locally-built `csv-processor:<git-sha>` image pulls from the local registry and
runs as the `csv-processor` service account via `KubernetesPodOperator`, and that its
`do_xcom_push=True` sidecar delivers `/airflow/xcom/return.json` back to Airflow correctly.
