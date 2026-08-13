"""The vertical-slice E2E proof suite (04-08-PLAN.md).

Every module here carries `pytestmark = pytest.mark.cluster` and requires a
running `kind-airflow-platform` cluster with the vertical slice deployed
(`csv_ingest_customers`/`smoke_kubernetes_pod` DAGs, the analytical database
migrated, the `csv-processor` image built and pushed) — run
`make cluster-up` and `make image-csv-processor` first, or run the whole
suite through `make cluster-verify`, which checks cluster reachability for
you and skips cleanly (rather than erroring) when no cluster is reachable.

Distinct from `tests/e2e/cluster/`: that directory proves the PLATFORM
exists and is wired correctly (INFRA-*). This directory proves the
PIPELINE's own ROADMAP success criteria for Phase 4 — unattended delivery,
idempotency, crash-recovery and publish atomicity — against the real
deployed DAGs, not the platform's scaffolding.
"""
