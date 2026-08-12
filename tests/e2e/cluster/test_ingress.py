"""tests/e2e/cluster/test_ingress.py — the tracer's manual curl, made permanent.

Honest limit: this proves the ingress path from the host into the cluster is
wired end-to-end (extraPortMappings -> ingress-nginx -> its default backend)
and that the controller itself is Ready. It does not prove any real
application's Ingress object routes correctly — Airflow's and MinIO's own
`test_*.py` modules in this directory, added by later plans, own that.

02-RESEARCH.md Pitfall 4: `<name>.localtest.me` resolves to `::1` **before**
`127.0.0.1` on this host, and kind's `extraPortMappings` publish IPv4 only.
Every connection here therefore pays one failed IPv6 attempt before the
client falls back — `urllib.request` (stdlib, used below) iterates the
address list exactly like curl and boto3 do, so it succeeds, but expect this
exact failure mode to be misdiagnosed as "the ingress is down" if a future
client is ever pinned to `AF_INET6`.
"""

from __future__ import annotations

import urllib.error
import urllib.request
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = pytest.mark.cluster

# Any hostname under *.localtest.me works here: it deliberately names no real
# service, because ingress-nginx's default backend answers 404 for ANY
# unmatched Host header — no Ingress object needs to exist for this
# assertion, which is exactly why it is usable before Airflow/MinIO exist.
PROBE_HOST = "tracer.localtest.me"


def test_ingress_default_backend_answers_over_http() -> None:
    """A GET to an unmatched *.localtest.me host returns 404, not a connection error."""
    url = f"http://{PROBE_HOST}/"
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:  # http, not user input
            status = resp.status
    except urllib.error.HTTPError as exc:
        # ingress-nginx's default backend legitimately answers with a 4xx —
        # urllib raises this as an exception rather than returning it.
        status = exc.code
    except OSError as exc:
        pytest.fail(
            f"connection to {url} failed outright rather than reaching the ingress "
            f"controller's default backend: {exc}",
        )
    assert status == 404, (
        f"expected the ingress-nginx default backend's 404 for an unmatched host, got {status}"
    )


def test_ingress_nginx_controller_is_available(kubectl_json: Callable[..., Any]) -> None:
    """The controller Deployment itself reports condition Available=True."""
    deployment = kubectl_json(
        "-n",
        "ingress-nginx",
        "get",
        "deployment",
        "ingress-nginx-controller",
    )
    conditions = deployment.get("status", {}).get("conditions", [])
    available = next((c for c in conditions if c.get("type") == "Available"), None)
    assert available is not None, (
        f"ingress-nginx-controller reported no Available condition: {conditions}"
    )
    assert available.get("status") == "True", (
        f"ingress-nginx-controller is not Available: {available}"
    )
