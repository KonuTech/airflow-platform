# U3 — streaming CSV throughput and peak-RSS baseline

**Regenerated automatically by
`tests/e2e/slice/test_pod_kill_retry.py::test_u3_throughput_and_peak_rss_baseline`
— do not hand-edit.**

- Measured at: 2026-08-13T22:38:21.981751+00:00
- Fixture: `tests/fixtures/slice-corpus.yaml`'s `customers_large.csv`
  (1,000,000 rows, ~55 MB — see that manifest's own
  `expect.approx_bytes`)
- Ingest pod configured memory limit:
  `4Gi` (`airflow/dags/csv_ingest_customers.py`'s
  `_INGEST_RESOURCES`)
- `rows_loaded`: 1,000,000
- `duration_ms`: 23,840
- **Throughput: 41,946 rows/sec**
- **Peak RSS (sampled): 62.9 MiB** (8 sample(s) of
  `/sys/fs/cgroup/memory.current`, every 3s)

## Measurement method, and its honest limits

Throughput is `rows_loaded / (duration_ms / 1000)` read directly from the
completed run's own `meta.ingestion_runs` row — the same wall-clock the
pipeline itself reports, not a value derived independently in this test.

Peak memory is the running MAXIMUM of `/sys/fs/cgroup/memory.current`,
polled by `kubectl exec`-ing into the ingest pod on a fixed interval while
the run is in flight. This cluster's containers were verified live to NOT
expose the cgroup v2 `memory.peak` file (`cat
/sys/fs/cgroup/memory.peak` → `No such file or directory`), and `kubectl
top pod` was verified live to fail (`error: Metrics API not available` —
no metrics-server is installed on this cluster). The sampled maximum is
therefore a LOWER BOUND on the true peak, not an exact figure: a spike
between two samples (up to
3s apart) would not be observed, and a
spike in the pod's final moments before it is deleted
(`on_finish_action=delete_succeeded_pod`) could be missed entirely if it
happens after the last successful sample.

## Regression policy

A future run of this same test producing a throughput figure more than 5x
worse than the number above should be treated as a bug, not a mystery
(ROADMAP.md's own words) — investigate before assuming the hardware or
cluster is merely "slower today."
