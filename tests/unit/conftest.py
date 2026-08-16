"""The one shared ``airflow/dags``-loading mechanism, reused by every DAG-structure test.

``DagBag(dag_folder="airflow/dags")`` (this Airflow version's constructor
has no ``include_examples`` kwarg -- verified directly against the pinned
``apache-airflow==3.3.0``; passing it raises ``TypeError``) needs two things
neither pytest nor a plain interpreter session provides for free, both
handled once here (04-07-PLAN.md Task 2: "reuse the SAME loading mechanism,
do not invent a second one" -- a shared ``conftest.py`` fixture, rather than
importing a test function across modules, is the mechanism: it avoids a
ruff F811 false-positive between the fixture's module-level definition and
each test function's same-named parameter):

1. ``airflow/dags`` is not a normal installed package, so
   ``from _common.kpo import common_kpo_kwargs`` (both DAG files) only
   resolves once ``airflow/dags`` is itself on ``sys.path`` -- true inside a
   real Airflow process (its own startup adds the configured
   ``core.dags_folder`` to ``sys.path``) but not inside a bare test process.
2. ``common_kpo_kwargs`` calls ``Variable.get("csv_processor_image")`` at
   DAG-parse time. ``airflow.sdk.Variable.get`` resolves through Airflow's
   layered secrets-backend chain, which includes the standard
   ``AIRFLOW_VAR_<KEY>``-environment-variable backend *before* any live
   API-server call -- setting that one env var here is the standard,
   documented, offline-safe way to satisfy a parse-time ``Variable.get``
   call in a structural test, with no live metadata DB or API server
   anywhere in the loop.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest
from airflow.models import DagBag

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_FOLDER = REPO_ROOT / "airflow" / "dags"

# Module-level, NOT only inside the `dagbag` fixture below (plan 07-04): pytest
# imports every ancestor `conftest.py` before collecting a directory's test
# modules, so this bootstrap must run unconditionally here for a test module
# that imports `_common.*` directly at ITS OWN module level (test_tracing_kpo.py)
# to resolve that import during COLLECTION -- a fixture body only runs at test
# EXECUTION time, well after collection-time imports have already been
# attempted. The fixture's own identical check below stays untouched and is
# now a redundant (idempotent, harmless) safety net for callers that import
# this module directly rather than through pytest's conftest discovery.
if str(DAGS_FOLDER) not in sys.path:
    sys.path.insert(0, str(DAGS_FOLDER))


@pytest.fixture(scope="session")
def dagbag() -> DagBag:
    """Parse `airflow/dags/` once per test session -- the one shared loading mechanism."""
    dags_folder_str = str(DAGS_FOLDER)
    if dags_folder_str not in sys.path:
        sys.path.insert(0, dags_folder_str)
    previous = os.environ.get("AIRFLOW_VAR_CSV_PROCESSOR_IMAGE")
    os.environ["AIRFLOW_VAR_CSV_PROCESSOR_IMAGE"] = "localhost:5001/csv-processor:test-fixture"
    try:
        return DagBag(dag_folder=dags_folder_str)
    finally:
        if previous is None:
            os.environ.pop("AIRFLOW_VAR_CSV_PROCESSOR_IMAGE", None)
        else:
            os.environ["AIRFLOW_VAR_CSV_PROCESSOR_IMAGE"] = previous
