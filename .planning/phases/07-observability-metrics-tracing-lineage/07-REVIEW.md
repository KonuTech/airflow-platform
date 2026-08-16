---
phase: 07-observability-metrics-tracing-lineage
reviewed: 2026-08-16T12:23:00Z
depth: standard
files_reviewed: 55
files_reviewed_list:
  - airflow/dags/_common/kpo.py
  - airflow/dags/_common/tracing_kpo.py
  - airflow/dags/csv_ingest_customers.py
  - configs/datasets/customers.yaml
  - helm/values/ci/airflow.yaml
  - helm/values/ci/monitoring.yaml
  - helm/values/ci/otel-collector.yaml
  - helm/values/ci/tempo.yaml
  - helm/values/local/airflow.yaml
  - helm/values/local/monitoring.yaml
  - helm/values/local/otel-collector.yaml
  - helm/values/local/tempo.yaml
  - helm/versions.env
  - kubernetes/namespaces.yaml
  - migrations/versions/0009_meta_schema_versions.py
  - migrations/versions/0010_meta_datasets_freshness.py
  - migrations/versions/0011_grafana_reader_role.py
  - migrations/versions/0012_meta_v_customers_lineage.py
  - packages/dataplat/pyproject.toml
  - packages/dataplat/src/dataplat/cli.py
  - packages/dataplat/src/dataplat/config/model.py
  - packages/dataplat/src/dataplat/config/registry.py
  - packages/dataplat/src/dataplat/metadata/postgres.py
  - packages/dataplat/src/dataplat/metadata/repository.py
  - packages/dataplat/src/dataplat/observability/metrics.py
  - packages/dataplat/src/dataplat/observability/tracing.py
  - packages/dataplat/src/dataplat/pipeline/engine.py
  - packages/dataplat/src/dataplat/pipeline/run.py
  - scripts/render-manifests.sh
  - scripts/stages/85-monitoring.sh
  - scripts/vault-bootstrap.py
  - tests/e2e/observability/__init__.py
  - tests/e2e/observability/conftest.py
  - tests/e2e/observability/test_alert_webhook_delivery.py
  - tests/e2e/observability/test_grafana_provisioning.py
  - tests/e2e/observability/test_trace_propagation.py
  - tests/e2e/vault/test_grafana_secrets.py
  - tests/integration/test_config_registry.py
  - tests/integration/test_freshness_query.py
  - tests/integration/test_lineage_view.py
  - tests/integration/test_metrics_otlp.py
  - tests/integration/test_migrations.py
  - tests/integration/test_run_ingest.py
  - tests/policy/test_dag_thinness.py
  - tests/policy/test_manifest_resources.py
  - tests/policy/test_values_profiles.py
  - tests/unit/conftest.py
  - tests/unit/observability/__init__.py
  - tests/unit/observability/test_metrics.py
  - tests/unit/observability/test_tracing.py
  - tests/unit/test_cli_error_handling.py
  - tests/unit/test_cli_trace_extraction.py
  - tests/unit/test_pipeline_errors.py
  - tests/unit/test_run_ingest_trace.py
  - tests/unit/test_tracing_kpo.py
findings:
  critical: 0
  warning: 3
  info: 1
  total: 4
status: issues_found
---

# Phase 07: Code Review Report

**Reviewed:** 2026-08-16T12:23:00Z
**Depth:** standard
**Files Reviewed:** 55
**Status:** issues_found

## Summary

This phase wires OTel-based metrics/tracing (`dataplat.observability.{metrics,tracing}`), W3C trace propagation into KubernetesPodOperator pods (`TracingKubernetesPodOperator`), data-freshness tracking (`meta.datasets` freshness columns + `ConfigRegistry.sync()`), schema versioning (migration 0009), a `grafana_reader` read-only Postgres role, a lineage view (`meta.v_customers_lineage`), a Grafana/Prometheus/Tempo observability stack (Helm values + dashboards + alerting-as-code), and a Vault-bootstrap extension for Grafana secrets.

The implementation is unusually well-documented and heavily tested (unit, integration, and live-cluster E2E tiers), and cross-file consistency is generally strong: the WARN-tier freshness SQL embedded in the Grafana alert rule was verified byte-identical to the tested `FRESHNESS_BREACH_QUERY` constant; the local/CI Helm values profiles were diffed directly and confirmed to diverge only on the already-permitted axes (including the large embedded dashboard JSON and alerting-rules blocks, which are byte-identical between profiles); SQL statements are consistently parameterized; secrets are never logged or hardcoded; subprocess calls consistently avoid shell interpolation.

No BLOCKER-severity defects were found. Three WARNING-level issues were found: a `dict.get(key, default)` misuse in the manifest-resource-budget policy test that will crash with an unhelpful `TypeError` (rather than the module's own intended fail-closed message) if a rendered CR ever carries an explicit `replicas: null`/`instances: null`; a docstring in `FreshnessConfig` that claims an ordering invariant (`warn_after <= fail_after`) is "enforced by PostgreSQL at query time" when no such enforcement exists anywhere in the SQL or schema; and an OTel provider-replacement pattern in `observability/metrics.py`/`tracing.py` that leaks the previous provider's background export thread if `configure()` is ever called twice with a real endpoint in one process, contradicting its own "safely re-callable" docstring claim. One INFO-level item notes a schema-level grant that is never exercised.

## Warnings

### WR-01: `spec.get(key, default)` does not default when the rendered CR carries an explicit `null`

**File:** `tests/policy/test_manifest_resources.py:217` and `tests/policy/test_manifest_resources.py:238`
**Issue:** `cluster_requests()` and `custom_resource_requests()` both use the pattern `spec.get(field, 1)` to default a replica-like count to `1`. `dict.get(key, default)` only substitutes `default` when `key` is **absent**; when the rendered YAML document carries the key with an explicit `null` value (`replicas: null` / `instances: null` — a well-known Helm gotcha when a chart template renders an unset `.Values.x` field without a `default` filter), `.get()` returns `None` instead of `1`. This `None` then flows into `parse_quantity(...) * instances` / `parse_quantity(...) * replicas`, raising an unhandled `TypeError: unsupported operand type(s) for *: 'float' and 'NoneType'`. Verified directly:
```
>>> {"replicas": None}.get("replicas", 1)
None
>>> 0.5 * None
TypeError: unsupported operand type(s) for *: 'float' and 'NoneType'
```
This module's own stated design goal (module docstring, "Pitfall 6") is that an unrecognised or malformed input must fail with a **named, actionable message**, not a silent zero — an uncaught `TypeError` deep inside a list comprehension is the opposite of that: it surfaces as a bare stack trace in the CI manifest-policy gate instead of a clear diagnostic. Neither of this repository's two current values profiles overrides `prometheus.prometheusSpec.replicas` / `cluster.instances` in a way that renders literal `null` today, so this is currently latent rather than actively triggered — but it is a real, reproducible defect in code that gates CI, not a hypothetical.
**Fix:**
```python
def cluster_requests(doc: dict[str, Any]) -> tuple[float, float]:
    spec = doc.get("spec") or {}
    instances = spec.get("instances")
    instances = 1 if instances is None else instances
    ...

def custom_resource_requests(doc: dict[str, Any]) -> tuple[float, float]:
    ...
    replicas = spec.get(replica_field)
    replicas = 1 if replicas is None else replicas
    ...
```

### WR-02: `FreshnessConfig`'s documented `warn_after <= fail_after` "enforcement" does not exist anywhere

**File:** `packages/dataplat/src/dataplat/config/model.py:361-365`
**Issue:** `FreshnessConfig.fail_after`'s docstring states: *"Ordering (`warn_after <= fail_after`) is enforced by PostgreSQL at query time in the freshness alert condition, not by this model."* This claim was checked against every place that could plausibly perform such enforcement, and none does:
- `migrations/versions/0010_meta_datasets_freshness.py` adds `expected_frequency`/`freshness_warn_after`/`freshness_fail_after` as three independent, nullable `Interval` columns with **no `CHECK` constraint** relating them.
- The two Grafana alert rules embedded in `helm/values/{local,ci}/monitoring.yaml` (`freshness-warn`, `freshness-fail`) are **two independent queries**, each comparing staleness against only its own threshold — neither compares `freshness_warn_after` to `freshness_fail_after`, and neither query references the other's threshold at all.
- The same is true of `tests/integration/test_freshness_query.py`'s `FRESHNESS_BREACH_QUERY`.

So a dataset misconfigured with `warn_after > fail_after` is accepted silently by `DatasetConfig` validation (no Pydantic model validator checks it — the docstring explicitly disclaims doing so, citing the real and reasonable constraint that these are opaque interval-literal strings this project deliberately never parses in Python) and by `ConfigRegistry.sync()`/`_resolve_dataset_id()` (no server-side check either), and would produce an operationally confusing sequence where the FAIL-tier (critical) alert can fire *before* the WARN-tier (warning) alert. (The dashboard's own `data_freshness` panel — id 8 in the same values files — happens to be robust to this because it checks the FAIL condition first in a single `CASE WHEN`, but the two independent alert *rules* have no equivalent protection.) The docstring's claim of DB-level enforcement is therefore inaccurate, and the actual validation gap it describes as "not by this model" is also not covered anywhere else.
**Fix:** Either add the enforcement the docstring claims (a `CHECK` constraint is straightforward here, since Postgres — unlike Python — parses `interval` server-side without ambiguity):
```python
# migrations/versions/0010_meta_datasets_freshness.py, upgrade()
op.create_check_constraint(
    "ck_datasets_freshness_warn_before_fail",
    "datasets",
    "freshness_warn_after IS NULL OR freshness_fail_after IS NULL "
    "OR freshness_warn_after <= freshness_fail_after",
    schema="meta",
)
```
or, at minimum, correct the docstring to state plainly that no such ordering is currently validated anywhere.

### WR-03: `metrics.configure()`/`tracing.configure()` leak the previous provider's background thread on re-configuration

**File:** `packages/dataplat/src/dataplat/observability/metrics.py:47-67`, `packages/dataplat/src/dataplat/observability/tracing.py:53-73`
**Issue:** Both modules' docstrings claim `configure()` is "safely re-callable: each call replaces the module-owned provider." The implementation does replace the module-level `_provider` reference, but never calls `.shutdown()` (or `.force_flush()`) on the **outgoing** provider first:
```python
# metrics.py
global _provider, _counters
if not otlp_endpoint:
    _provider = metrics.NoOpMeterProvider()
else:
    ...
    _provider = MeterProvider(metric_readers=[reader])   # old _provider silently dropped
_counters = {}
```
```python
# tracing.py
global _provider
if not otlp_endpoint:
    _provider = trace.NoOpTracerProvider()
    return
...
_provider = provider   # old _provider silently dropped
```
`MeterProvider`'s `PeriodicExportingMetricReader` and `TracerProvider`'s `BatchSpanProcessor` both own real background export threads. If `configure()` is ever called a second time with a real `otlp_endpoint` in the same process, the first provider's thread (and any buffered, unflushed spans/metrics) is abandoned rather than drained/stopped — a genuine resource leak, and a silent loss of any telemetry it was still holding. The file's own `flush()` function already establishes the correct defensive pattern (`getattr(_provider, "force_flush", None)`); the same pattern is simply missing from `configure()` for the *outgoing* provider. In the current production call pattern (`dataplat.cli.main()` calls `configure()` exactly once per short-lived pod process) this is dormant, but the module's own docstring advertises safe re-callability as a guarantee, and both files' own test suites (`tests/unit/observability/test_tracing.py::test_configure_is_safely_re_callable_within_one_process`) already exercise repeated `configure()` calls and manually work around the leak by calling `shutdown()` themselves after each configured test — evidence the gap is real, not merely theoretical.
**Fix:**
```python
def configure(*, otlp_endpoint: str | None) -> None:
    global _provider, _counters
    previous = _provider
    if not otlp_endpoint:
        _provider = metrics.NoOpMeterProvider()
    else:
        exporter = OTLPMetricExporter(endpoint=f"{otlp_endpoint}/v1/metrics")
        reader = PeriodicExportingMetricReader(exporter)
        _provider = MeterProvider(metric_readers=[reader])
    _counters = {}
    shutdown = getattr(previous, "shutdown", None)
    if callable(shutdown):
        shutdown()
```
(same shape for `tracing.configure()`, shutting down `previous` before returning).

## Info

### IN-01: `grafana_reader` is granted `USAGE` on a schema it never has a table grant in

**File:** `migrations/versions/0011_grafana_reader_role.py:46`
**Issue:** `upgrade()` runs `GRANT USAGE ON SCHEMA normalized TO grafana_reader` alongside `GRANT USAGE ON SCHEMA meta TO grafana_reader`. The role's own docstring (lines 20-24) is explicit that `grafana_reader` **deliberately never gets a direct table grant on `normalized.customers`** — it reaches that data only through `meta.v_customers_lineage` (migration 0012), which runs under the view owner's privileges, not the querying role's, so `USAGE` on `normalized` contributes nothing the role actually needs. It is inert today (no table-level grant exists in `normalized` for this role, so the `USAGE` grant alone confers no data access), but it is a small deviation from the least-privilege posture this migration otherwise documents carefully, and an unused grant is easy to forget about (and mistakenly rely on) if a future migration ever does add a direct `normalized.*` table grant to this role.
**Fix:** Drop the `GRANT USAGE ON SCHEMA normalized TO grafana_reader` line (and its corresponding `REVOKE` in `downgrade()`) since it is not exercised by anything this role is granted access to, or add a comment explaining why it is kept for forward-compatibility if that is the intent.

---

_Reviewed: 2026-08-16T12:23:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
