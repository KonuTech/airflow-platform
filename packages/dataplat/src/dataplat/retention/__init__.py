"""``dataplat.retention`` -- pure, never-raising retention dry-run/enforce evaluation.

Deliberately package-marker only, matching ``dataplat/scd/__init__.py``'s and
``dataplat/validate/__init__.py``'s shallow re-export convention: callers
import from the submodule directly, e.g.
``from dataplat.retention.policy import evaluate_retention``. The function
living in this package (``policy.evaluate_retention``) takes no storage/DB
handle and performs no I/O -- a future DAG task (plan 11-08's
``platform_retention``) is the only place that ever queries a layer's real
candidates or acts on this package's report.
"""

from __future__ import annotations
