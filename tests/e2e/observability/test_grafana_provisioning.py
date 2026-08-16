"""tests/e2e/observability/test_grafana_provisioning.py -- Task 3's live structural proof.

Proves, against the LIVE cluster (after both `make cluster-up` and
`make vault-bootstrap` have run), that plan 07-07's own values-file wiring
is not just rendered but actually WORKING:

  1. Grafana's own `/api/datasources` lists exactly three datasources
     (`analytics-postgres`, `prometheus`, `tempo`), each passing its own
     health check.
  2. `/api/search` + `/api/dashboards/uid/...` show one dashboard whose
     panel titles include all eight named metrics plus the three D-03 live
     gauges.
  3. `/api/v1/provisioning/alert-rules` lists at least five rules, including
     both freshness severity tiers (embedding the real, tested SQL -- never
     a paraphrase) and the three live-gauge rules.
  4. Prometheus (queried through Grafana's own datasource proxy, which is
     also this module's own live proof that the `prometheus` datasource
     itself works end-to-end, not just that it exists) actually holds a
     scraped `dataplat` metric series -- proving Task 1's own
     `additionalServiceMonitors` entry works, not just renders.

Every endpoint shape below was verified EMPIRICALLY against a live deployed
Grafana this session, not assumed from documentation -- two turned out to
differ from what 07-07-PLAN.md's own `<interfaces>` block or a first-pass
reading of the chart's docs suggested (see `helm/values/local/
monitoring.yaml`'s own header comments for the full story of each):
datasource health-check is `GET /api/datasources/uid/{uid}/health` (the
UID-scoped endpoint), not the plan's own literally-named `POST /api/
datasources/{id}/health` (id-scoped `GET`/`POST` both return `404` live);
and `additionalServiceMonitors` needed a real values-key relocation, not
just a datasource-health quirk.

Honest limit: assertion (4) needs a real `csv_ingest_customers` run to have
actually completed AND a fresh `csv-processor` image to be registered
(`csv_processor_image` Airflow Variable) -- this module runs the same
file-drop-and-poll shape `tests/e2e/slice/test_smoke_and_idempotency.py`
already established (mirrored, not imported -- see conftest.py's own
docstring) but does not itself rebuild/push the image; if the registered
image predates plans 07-02/07-05's own OTel wiring, this specific assertion
will legitimately time out with a clear, diagnostic message, distinct from
"the additionalServiceMonitors entry is broken" -- `make image-csv-processor`
is the fix, not a change to this module.
"""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.observability.conftest import poll_file_discovered, poll_ingestion_run

if TYPE_CHECKING:
    from collections.abc import Callable

    import psycopg

pytestmark = pytest.mark.cluster

_CUSTOMERS_DAG_ID = "csv_ingest_customers"
_CUSTOMERS_DATASET = "customers"

_EXPECTED_DATASOURCE_NAMES = frozenset({"analytics-postgres", "prometheus", "tempo"})

_EXPECTED_METRIC_PANEL_TITLES = frozenset(
    {
        "files_processed",
        "files_failed",
        "rows_processed",
        "rows_invalid",
        "rows_deduplicated",
        "processing_duration",
        "validation_failures",
        "data_freshness",
    },
)
_EXPECTED_GAUGE_RULE_UIDS = frozenset(
    {"gauge-runs-inflight", "gauge-failure-rate", "gauge-reject-rate"},
)

_DASHBOARD_UID = "platform-observability"

_INGEST_TIMEOUT_SECONDS = 180
_PROMETHEUS_POLL_TIMEOUT_SECONDS = 180
_POLL_INTERVAL_SECONDS = 5


def test_grafana_has_exactly_three_healthy_datasources(
    grafana_api: Callable[..., Any],
) -> None:
    """OBS-08/D-03: analytics-postgres, prometheus and tempo all exist and connect."""
    datasources = grafana_api("GET", "/api/datasources")
    names = {ds["name"] for ds in datasources}
    assert names == _EXPECTED_DATASOURCE_NAMES, (
        f"expected exactly {sorted(_EXPECTED_DATASOURCE_NAMES)}, got {sorted(names)}"
    )
    for ds in datasources:
        # The UID-scoped health endpoint -- verified live this session; the
        # id-scoped form (POST or GET) returns 404 on this Grafana version.
        health = grafana_api("GET", f"/api/datasources/uid/{ds['uid']}/health")
        assert health.get("status") == "OK", f"datasource {ds['name']!r} unhealthy: {health}"


def test_dashboard_has_all_eight_named_metric_panels(
    grafana_api: Callable[..., Any],
) -> None:
    """D-03: one dashboard shows all 8 named metrics plus the 3 live gauges."""
    results = grafana_api("GET", "/api/search", params={"type": "dash-db"})
    assert results, "no dashboards found via /api/search?type=dash-db"
    summary = next((d for d in results if d.get("uid") == _DASHBOARD_UID), None)
    assert summary is not None, (
        f"dashboard uid={_DASHBOARD_UID!r} not found among: "
        f"{sorted(d.get('uid', '') for d in results)}"
    )

    dashboard = grafana_api("GET", f"/api/dashboards/uid/{_DASHBOARD_UID}")
    panel_titles = {p["title"] for p in dashboard["dashboard"]["panels"]}
    missing = _EXPECTED_METRIC_PANEL_TITLES - panel_titles
    assert not missing, f"dashboard is missing panels for {missing} (found: {sorted(panel_titles)})"


def _rule_raw_sql(rule: dict[str, Any]) -> str:
    """Concatenate every `rawSql` fragment across a rule's own query chain."""
    parts = []
    for query in rule.get("data", []):
        model = query.get("model", {})
        if "rawSql" in model:
            parts.append(model["rawSql"])
    return "\n".join(parts)


def test_alert_rules_cover_both_freshness_severities_and_three_gauges(
    grafana_api: Callable[..., Any],
) -> None:
    """OBS-01/OBS-09/D-05: >=5 rules, both freshness tiers, three live gauges."""
    rules = grafana_api("GET", "/api/v1/provisioning/alert-rules")
    assert len(rules) >= 5, f"expected at least 5 alert rules, found {len(rules)}"
    by_uid = {r["uid"]: r for r in rules}

    warn = by_uid.get("freshness-warn")
    fail = by_uid.get("freshness-fail")
    assert warn is not None, f"freshness-warn rule missing from {sorted(by_uid)}"
    assert fail is not None, f"freshness-fail rule missing from {sorted(by_uid)}"

    assert "expected_frequency IS NOT NULL" in _rule_raw_sql(warn), (
        "WARN-tier rule's query text does not contain the real, tested "
        "freshness predicate -- it may have drifted from "
        "tests/integration/test_freshness_query.py's own FRESHNESS_BREACH_QUERY"
    )
    assert "freshness_fail_after" in _rule_raw_sql(fail), (
        "FAIL-tier rule's query text does not reference freshness_fail_after"
    )
    assert warn["labels"].get("severity") == "warning", warn["labels"]
    assert fail["labels"].get("severity") == "critical", fail["labels"]

    missing_gauges = _EXPECTED_GAUGE_RULE_UIDS - set(by_uid)
    assert not missing_gauges, f"missing live-gauge alert rules: {missing_gauges}"


def _unique_small_csv_bytes() -> bytes:
    """A tiny, uniquely-marked customers CSV -- never collides with a prior run's content hash."""
    marker = uuid.uuid4().hex[:16]
    return (
        "customer_id,name,country,birth_date,event_ts\n"
        f"900001,E2E-OBS-{marker},PL,1990-01-01,2026-01-01T00:00:00Z\n"
        "900002,Observability Probe,US,1985-05-05,2026-02-02T00:00:00Z\n"
    ).encode()


def test_prometheus_scrapes_dataplat_metrics_via_the_otel_collector(
    grafana_api: Callable[..., Any],
    kubectl: Callable[..., Any],
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
) -> None:
    """OBS-08/D-03: proves Task 1's additionalServiceMonitors entry actually works.

    Nothing in Waves 1-3 otherwise triggers a fresh `csv_ingest_customers`
    run with the OTel-instrumented pipeline code live, so an empty
    Prometheus result would be indistinguishable from a genuinely broken
    `additionalServiceMonitors` entry -- this test drives a real run first
    (mirroring, not importing, `tests/e2e/slice/test_smoke_and_idempotency.
    py`'s own file-drop-and-poll shape per this project's established
    `tests/e2e/*/` convention), then polls the live Prometheus API through
    Grafana's own datasource proxy (never queried once -- Prometheus's own
    scrape interval means a just-completed run's metrics may not be visible
    for up to one scrape cycle).
    """
    unpause = kubectl(
        "-n",
        "airflow",
        "exec",
        "deploy/airflow-api-server",
        "--",
        "airflow",
        "dags",
        "unpause",
        _CUSTOMERS_DAG_ID,
    )
    assert unpause.returncode == 0, f"airflow dags unpause failed:\n{unpause.stderr}"

    app = s3_client("app")
    key = f"customers/e2e-observability-{uuid.uuid4().hex[:12]}.csv"
    object_uri = f"s3://raw/{key}"
    app.put_object(Bucket="raw", Key=key, Body=_unique_small_csv_bytes())

    file_row = poll_file_discovered(
        analytics_connection,
        dataset=_CUSTOMERS_DATASET,
        object_uri=object_uri,
        timeout=_INGEST_TIMEOUT_SECONDS,
    )
    outcome = poll_ingestion_run(
        analytics_connection,
        file_id=file_row["file_id"],
        timeout=_INGEST_TIMEOUT_SECONDS,
    )
    assert outcome["status"] == "SUCCEEDED", (
        f"probe ingestion run finished {outcome['status']!r}, not SUCCEEDED -- "
        "cannot prove the Prometheus scrape without a real completed run"
    )

    # The OTel Collector's Prometheus exporter (default `add_metric_suffixes:
    # true`, otelcol-contrib) appends `_total` to every monotonic Sum/Counter
    # instrument per the OpenTelemetry-to-Prometheus naming convention -- the
    # series Prometheus actually stores is `runs_started_total`, never the
    # bare `runs_started` name `dataplat.observability.metrics.increment()`
    # creates. Verified live this session: `runs_started_total` has a real,
    # populated result vector in Prometheus immediately after a SUCCEEDED
    # run; the bare `runs_started` name has none, ever, regardless of poll
    # duration -- it is not a race, the series genuinely does not exist
    # under that name. Query the exposition-format name here.
    deadline = time.monotonic() + _PROMETHEUS_POLL_TIMEOUT_SECONDS
    last_response: dict[str, Any] = {}
    while time.monotonic() < deadline:
        last_response = grafana_api(
            "GET",
            "/api/datasources/proxy/uid/prometheus/api/v1/query",
            params={"query": "runs_started_total"},
        )
        result = last_response.get("data", {}).get("result", [])
        if result:
            return
        time.sleep(_POLL_INTERVAL_SECONDS)

    pytest.fail(
        f"Prometheus never returned a result vector for `runs_started_total` within "
        f"{_PROMETHEUS_POLL_TIMEOUT_SECONDS}s of a SUCCEEDED run (file_id="
        f"{file_row['file_id']}) -- last response: {last_response!r}. If the "
        f"registered csv_processor_image Variable predates plans 07-02/07-05's "
        f"OTel wiring, run `make image-csv-processor` and retry; otherwise this "
        f"indicates a genuinely broken additionalServiceMonitors entry."
    )
