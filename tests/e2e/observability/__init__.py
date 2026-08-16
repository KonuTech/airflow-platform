"""The live-cluster Observability verification harness (plan 07-07).

Every module here carries `pytestmark = pytest.mark.cluster` and requires a
running `kind-airflow-platform` cluster with `make cluster-up` AND
`make vault-bootstrap` both already run -- the `grafana-alert-webhook`
Kubernetes Secret plan 07-06's `_ensure_grafana_secrets` creates is a hard
precondition for Grafana's own pod to start at all (`envFromSecret`), so a
cluster that has only run `cluster-up` will fail every test in this
directory with a clear, named skip/failure, not a silent hang. Plan 07-08
extends this package with the live webhook-delivery proof (D-20).
"""
