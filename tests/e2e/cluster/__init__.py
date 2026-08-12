"""The live-cluster verification harness (D-16).

Every module here carries `pytestmark = pytest.mark.cluster` and requires a
running `kind-airflow-platform` cluster — run `make cluster-up` first, or run
the whole suite through `make cluster-verify`, which does that check for you
and skips cleanly (rather than erroring) when no cluster is reachable.
"""
