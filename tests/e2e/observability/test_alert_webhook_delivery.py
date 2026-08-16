"""tests/e2e/observability/test_alert_webhook_delivery.py -- D-20's live alert-delivery proof.

CONTEXT.md D-20: force a real freshness breach and prove Grafana's own
Alerting engine actually delivers an HTTP POST to a webhook contact point --
not merely that the underlying SQL predicate evaluates true. This is the
single most novel testing mechanism this phase introduces, because Grafana's
Alerting engine runs entirely in-cluster and can never reach a
pytest-process-local `localhost` listener (RESEARCH.md's own Wave 0 Gaps
section): `tests/e2e/observability/conftest.py`'s `webhook_receiver` fixture
(Task 1) stands up a real, cluster-reachable throwaway receiver for this
test's duration.

Follows `tests/e2e/vault/test_rotation.py`'s own force/observe/restore-in-
`finally` shape exactly (07-08-PLAN.md's own `<interfaces>` block): read
every piece of state this test is about to mutate BEFORE touching anything,
force the condition, observe the real effect, then restore everything --
including on a failure -- with the `finally` block's own closing assertions
confirming the restore actually round-tripped.

**Why a live Grafana restart is required, not just a Secret update:**
`helm/values/local/monitoring.yaml`'s `grafana.envFromSecret:
grafana-alert-webhook` injects the Secret's two keys as container env vars,
and its own `alerting.contactpoints.yaml` interpolates
`url: $GRAFANA_ALERT_WEBHOOK_URL` -- Grafana reads and interpolates
provisioning files (including `$ENV_VAR` substitution) ONCE, at process
start, never on a live per-evaluation basis. Updating the Kubernetes Secret
alone therefore never changes an already-running Grafana pod's actual
contact-point URL -- this test explicitly `kubectl rollout restart`s the
`monitoring-grafana` Deployment and blocks on `rollout status` before
proceeding, exactly the branch 07-08-PLAN.md's own Task 3 action text
anticipated as "verify which is actually true against the live chart".

**Why this takes several real minutes:** `helm/values/local/monitoring.yaml`'s
`platform` rule group evaluates every `interval: 5m`, and each individual
rule additionally requires `for: 5m` of sustained breach before Pending
transitions to Firing (only Firing calls the contact point). A forced,
persistent breach therefore needs, worst case, two full evaluation cycles
(~10 minutes) before a webhook POST can possibly arrive. This test polls
with a generous bound comfortably above that theoretical floor rather than
attempting to shrink the rule group's own provisioned interval (07-08-PLAN.md's
own Task 3 action text offers that as an OPTIONAL alternative "if that proves
faster and still genuine" -- rejected here because reconstructing the live
5-rule "platform" group via the provisioning API risks corrupting the other
4 rules `test_grafana_provisioning.py` itself depends on, for a speed gain
this permanent regression test does not need).
"""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable
    from pathlib import Path

    import hvac
    import psycopg

pytestmark = pytest.mark.cluster

_MONITORING_NAMESPACE = "monitoring"
_GRAFANA_DEPLOYMENT = "deployment/monitoring-grafana"
_GRAFANA_MOUNT = "grafana"
_WEBHOOK_PATH = "alert-webhook"
_DB_PASSWORD_PATH = "analytics-db"  # noqa: S105 -- a Vault KV path segment, not a credential value
_SECRET_NAME = "grafana-alert-webhook"  # noqa: S105 -- a K8s Secret's metadata.name, not a credential

# A dataset dedicated to this test -- NEVER `customers` (07-08-PLAN.md's own
# explicit instruction, to avoid interfering with other suites' assumptions
# about its state). Fixed name (not a fresh uuid per run) so this test is
# idempotent-safe against a live cluster that already carries a prior,
# interrupted run's leftover row -- the before/restore logic below handles
# either starting state correctly.
_TEST_DATASET_NAME = "e2e-observability-alert-test"

# See module docstring: group interval (5m) x >=2 cycles + the per-rule
# `for: 5m` pending duration is the real worst-case wall-clock a forced,
# persistent breach needs before Grafana's Alerting engine transitions
# Pending -> Firing and calls the webhook contact point. This bound carries
# a comfortable margin over that ~10-minute theoretical floor.
_ALERT_FIRE_TIMEOUT_SECONDS = 900
_ALERT_POLL_INTERVAL_SECONDS = 10

_GRAFANA_RESTART_TIMEOUT_SECONDS = 180


def _read_dataset_row(conn: psycopg.Connection[Any], dataset_name: str) -> dict[str, Any] | None:
    """Read every mutable column of a `meta.datasets` row, or None if it does not exist."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT source_system, description, is_active, "
            "expected_frequency, freshness_warn_after, freshness_fail_after "
            "FROM meta.datasets WHERE dataset_name = %s",
            (dataset_name,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return {
        "source_system": row[0],
        "description": row[1],
        "is_active": row[2],
        "expected_frequency": row[3],
        "freshness_warn_after": row[4],
        "freshness_fail_after": row[5],
    }


def _force_freshness_breach(conn: psycopg.Connection[Any], dataset_name: str) -> None:
    """UPSERT `dataset_name` with a 1-second expected_frequency AND freshness_fail_after.

    No `meta.files`/`meta.ingestion_runs` row is ever created for this
    dataset, so `COALESCE(MAX(f.discovered_at), d.created_at)` -- the
    freshness rules' own reference point -- resolves to `created_at`,
    already in the past the instant this commits. Plan 07-07's two-tier
    design means setting BOTH thresholds this low breaches BOTH the
    WARN-tier and FAIL-tier rules on the very next evaluation tick, so this
    one forced breach proves the full two-severity chain -- this test
    asserts specifically on the FAIL tier (`severity: critical`), per
    07-08-PLAN.md's own acceptance criteria.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO meta.datasets "
            "(dataset_name, expected_frequency, freshness_warn_after, freshness_fail_after) "
            "VALUES (%s, interval '1 second', NULL, interval '1 second') "
            "ON CONFLICT (dataset_name) DO UPDATE SET "
            "expected_frequency = EXCLUDED.expected_frequency, "
            "freshness_warn_after = EXCLUDED.freshness_warn_after, "
            "freshness_fail_after = EXCLUDED.freshness_fail_after",
            (dataset_name,),
        )
    conn.commit()


def _restore_dataset_row(
    conn: psycopg.Connection[Any],
    dataset_name: str,
    original: dict[str, Any],
) -> None:
    """UPDATE `dataset_name`'s row back to its pre-test column values."""
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE meta.datasets SET "
            "source_system = %(source_system)s, "
            "description = %(description)s, "
            "is_active = %(is_active)s, "
            "expected_frequency = %(expected_frequency)s, "
            "freshness_warn_after = %(freshness_warn_after)s, "
            "freshness_fail_after = %(freshness_fail_after)s "
            "WHERE dataset_name = %(dataset_name)s",
            {**original, "dataset_name": dataset_name},
        )
    conn.commit()


def _delete_dataset_row(conn: psycopg.Connection[Any], dataset_name: str) -> None:
    """DELETE `dataset_name`'s row entirely (this test created it fresh)."""
    with conn.cursor() as cur:
        cur.execute("DELETE FROM meta.datasets WHERE dataset_name = %s", (dataset_name,))
    conn.commit()


def _apply_grafana_webhook_secret(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    manifest_path: Path,
    *,
    webhook_url: str,
    db_password: str,
) -> None:
    """Re-apply the `grafana-alert-webhook` Secret, preserving `GRAFANA_DB_PASSWORD`.

    Mirrors `scripts/vault-bootstrap.py`'s own `_apply_kubernetes_secret`
    Secret-materialization shape -- a small duplicated helper in this test
    file, not an import (that script is not a package module; 07-08-PLAN.md's
    own Task 3 action text names this exact choice explicitly). Both keys are
    always written together: `envFromSecret` injects the WHOLE Secret, so
    omitting `GRAFANA_DB_PASSWORD` here would break the `analytics-postgres`
    datasource's password the moment Grafana restarts to pick this up.
    """
    manifest = {
        "apiVersion": "v1",
        "kind": "Secret",
        "type": "Opaque",
        "metadata": {"name": _SECRET_NAME, "namespace": _MONITORING_NAMESPACE},
        "stringData": {
            "GRAFANA_ALERT_WEBHOOK_URL": webhook_url,
            "GRAFANA_DB_PASSWORD": db_password,
        },
    }
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    proc = kubectl_fn("-n", _MONITORING_NAMESPACE, "apply", "-f", str(manifest_path))
    assert proc.returncode == 0, (
        f"failed to apply {_SECRET_NAME} Secret (exit {proc.returncode}):\n{proc.stderr}"
    )


def _restart_grafana_and_wait_ready(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    *,
    timeout: float = _GRAFANA_RESTART_TIMEOUT_SECONDS,
) -> None:
    """Roll `monitoring-grafana`'s Deployment and block until the new pod is Ready.

    See module docstring: required because env-var-interpolated provisioning
    (`$GRAFANA_ALERT_WEBHOOK_URL`) is only ever read once, at Grafana process
    start.
    """
    restart = kubectl_fn("-n", _MONITORING_NAMESPACE, "rollout", "restart", _GRAFANA_DEPLOYMENT)
    assert restart.returncode == 0, (
        f"kubectl rollout restart {_GRAFANA_DEPLOYMENT} failed "
        f"(exit {restart.returncode}):\n{restart.stderr}"
    )
    status = kubectl_fn(
        "-n",
        _MONITORING_NAMESPACE,
        "rollout",
        "status",
        _GRAFANA_DEPLOYMENT,
        f"--timeout={int(timeout)}s",
        timeout=int(timeout) + 30,
    )
    assert status.returncode == 0, (
        f"kubectl rollout status {_GRAFANA_DEPLOYMENT} did not report Ready within {timeout}s "
        f"(exit {status.returncode}):\n{status.stderr}"
    )


def _point_webhook_at_receiver(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    vault_client: hvac.Client,
    tmp_path: Path,
    *,
    webhook_url: str,
    db_password: str,
) -> None:
    """FORCE (1): point the `platform-webhook` contact point at `webhook_url`, live.

    Writes `grafana/alert-webhook` in Vault, re-applies the
    `grafana-alert-webhook` Kubernetes Secret to match, then restarts
    Grafana and blocks until the new pod is Ready -- see module docstring
    for why the restart is required.
    """
    vault_client.secrets.kv.v2.create_or_update_secret(
        mount_point=_GRAFANA_MOUNT,
        path=_WEBHOOK_PATH,
        secret={"url": webhook_url},
    )
    manifest_path = tmp_path / "grafana-alert-webhook-secret.json"
    _apply_grafana_webhook_secret(
        kubectl_fn,
        manifest_path,
        webhook_url=webhook_url,
        db_password=db_password,
    )
    _restart_grafana_and_wait_ready(kubectl_fn)


def _extract_webhook_bodies(logs: str) -> list[dict[str, Any]]:
    """Parse every `WEBHOOK_RECEIVED:` log line's JSON body, skipping unparseable lines.

    Locates the FIRST `{` in each matching line rather than assuming any
    particular text before it -- robust to the receiver's own
    `{method} {path} {body}` prefix shape (`conftest.py`'s
    `_WEBHOOK_RECEIVER_HANDLER_SOURCE`) without hardcoding an exact split
    point.
    """
    bodies: list[dict[str, Any]] = []
    for line in logs.splitlines():
        if "WEBHOOK_RECEIVED:" not in line:
            continue
        brace_index = line.find("{")
        if brace_index == -1:
            continue
        try:
            bodies.append(json.loads(line[brace_index:]))
        except json.JSONDecodeError:
            continue
    return bodies


def _find_critical_alert_for_dataset(logs: str, dataset_name: str) -> dict[str, Any] | None:
    """Return the first FAIL-tier (`severity: critical`) alert naming `dataset_name`, or None.

    Searches each matched alert's own full JSON text for the dataset name
    substring rather than assuming an exact label/column key -- robust to
    exactly which column Grafana's multi-dimensional SQL alerting attaches
    the per-row `dataset_name` value under, while still scoping the search
    to alerts already confirmed `severity: critical` (Grafana's stable,
    Alertmanager-compatible webhook payload shape: top-level `alerts: [...]`,
    each with its own `labels`).
    """
    for body in _extract_webhook_bodies(logs):
        for alert in body.get("alerts", []):
            if alert.get("labels", {}).get("severity") != "critical":
                continue
            if dataset_name in json.dumps(alert):
                return alert
    return None


def _wait_for_critical_alert(
    read_receiver_logs: Callable[[], str | None],
    dataset_name: str,
) -> dict[str, Any]:
    """OBSERVE: poll (never sleep-and-hope) the receiver's logs for a real, matching delivery."""
    deadline = time.monotonic() + _ALERT_FIRE_TIMEOUT_SECONDS
    last_logs: str | None = None
    while time.monotonic() < deadline:
        last_logs = read_receiver_logs()
        if last_logs:
            matched = _find_critical_alert_for_dataset(last_logs, dataset_name)
            if matched is not None:
                return matched
        time.sleep(_ALERT_POLL_INTERVAL_SECONDS)
    msg = (
        f"no severity=critical webhook naming dataset {dataset_name!r} arrived within "
        f"{_ALERT_FIRE_TIMEOUT_SECONDS}s of forcing the breach -- last observed receiver "
        f"logs: {last_logs!r}"
    )
    raise AssertionError(msg)


def _restore_dataset_state(
    conn: psycopg.Connection[Any],
    dataset_name: str,
    *,
    pre_existing: bool,
    original: dict[str, Any] | None,
) -> list[str]:
    """Restore `meta.datasets`, never letting a raised exception skip the webhook restore too."""
    errors: list[str] = []
    try:
        if pre_existing:
            assert original is not None  # narrows for mypy; caller only sets True when non-None
            _restore_dataset_row(conn, dataset_name, original)
            restored_row = _read_dataset_row(conn, dataset_name)
            if restored_row != original:
                errors.append(
                    f"meta.datasets row restore mismatch: expected {original!r}, "
                    f"got {restored_row!r}"
                )
        else:
            _delete_dataset_row(conn, dataset_name)
            if _read_dataset_row(conn, dataset_name) is not None:
                errors.append(
                    f"meta.datasets row for {dataset_name!r} still exists after "
                    "this test's own DELETE"
                )
    except Exception as exc:  # noqa: BLE001 -- collected, never allowed to skip the webhook restore
        errors.append(f"meta.datasets restore raised: {exc!r}")
    return errors


def _restore_webhook_state(
    kubectl_fn: Callable[..., subprocess.CompletedProcess[str]],
    vault_client: hvac.Client,
    tmp_path: Path,
    *,
    original_webhook_url: str,
    db_password: str,
) -> list[str]:
    """Restore the Vault secret, the K8s Secret and Grafana's own pod, with a closing read-back."""
    errors: list[str] = []
    try:
        vault_client.secrets.kv.v2.create_or_update_secret(
            mount_point=_GRAFANA_MOUNT,
            path=_WEBHOOK_PATH,
            secret={"url": original_webhook_url},
        )
        manifest_path = tmp_path / "grafana-alert-webhook-secret-restore.json"
        _apply_grafana_webhook_secret(
            kubectl_fn,
            manifest_path,
            webhook_url=original_webhook_url,
            db_password=db_password,
        )
        _restart_grafana_and_wait_ready(kubectl_fn)

        restored_secret = vault_client.secrets.kv.v2.read_secret_version(
            mount_point=_GRAFANA_MOUNT,
            path=_WEBHOOK_PATH,
        )
        restored_url = restored_secret["data"]["data"]["url"]
        if restored_url != original_webhook_url:
            errors.append(
                f"grafana/alert-webhook restore mismatch: expected {original_webhook_url!r}, "
                f"got {restored_url!r}"
            )
    except Exception as exc:  # noqa: BLE001 -- collected below, never silently swallowed
        errors.append(f"grafana/alert-webhook restore raised: {exc!r}")
    return errors


def test_forced_freshness_breach_delivers_a_real_webhook_post(
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_root_client: hvac.Client,
    analytics_connection: psycopg.Connection[Any],
    webhook_receiver: tuple[str, Callable[[], str | None]],
    tmp_path: Path,
) -> None:
    """D-20: a forced, persistent freshness breach makes Grafana deliver a real webhook POST.

    Asserts the captured payload's content (the test dataset's own name)
    AND a `severity: critical` marker -- proving the FAIL-tier rule
    specifically fired, not merely that some alert of some severity
    arrived. Restores every piece of live state this test mutates
    (`meta.datasets` row, the `grafana/alert-webhook` Vault secret, the
    `grafana-alert-webhook` Kubernetes Secret, and Grafana's own running
    pod) in a `finally` block, with closing assertions confirming the
    restore round-tripped -- regardless of whether the test itself passed.
    """
    webhook_url, read_receiver_logs = webhook_receiver

    # ---- BEFORE: read every piece of state this test is about to mutate ----
    before_webhook_secret = vault_root_client.secrets.kv.v2.read_secret_version(
        mount_point=_GRAFANA_MOUNT,
        path=_WEBHOOK_PATH,
    )
    original_webhook_url: str = before_webhook_secret["data"]["data"]["url"]

    db_password_secret = vault_root_client.secrets.kv.v2.read_secret_version(
        mount_point=_GRAFANA_MOUNT,
        path=_DB_PASSWORD_PATH,
    )
    db_password: str = db_password_secret["data"]["data"]["password"]

    original_dataset_row = _read_dataset_row(analytics_connection, _TEST_DATASET_NAME)
    dataset_row_pre_existing = original_dataset_row is not None

    try:
        _point_webhook_at_receiver(
            kubectl,
            vault_root_client,
            tmp_path,
            webhook_url=webhook_url,
            db_password=db_password,
        )
        _force_freshness_breach(analytics_connection, _TEST_DATASET_NAME)

        matched_alert = _wait_for_critical_alert(read_receiver_logs, _TEST_DATASET_NAME)
        assert matched_alert["labels"]["severity"] == "critical", matched_alert
        assert _TEST_DATASET_NAME in json.dumps(matched_alert), matched_alert
    finally:
        restore_errors = _restore_dataset_state(
            analytics_connection,
            _TEST_DATASET_NAME,
            pre_existing=dataset_row_pre_existing,
            original=original_dataset_row,
        )
        restore_errors += _restore_webhook_state(
            kubectl,
            vault_root_client,
            tmp_path,
            original_webhook_url=original_webhook_url,
            db_password=db_password,
        )
        if restore_errors:
            pytest.fail(
                "test cleanup FAILED to fully restore live cluster state -- MANUAL "
                "INTERVENTION REQUIRED:\n" + "\n".join(restore_errors)
            )
