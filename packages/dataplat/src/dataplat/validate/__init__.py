"""Config-addressable data quality/validation rules -- VALID-01/02/03's `StreamingStage`s.

Home to the `StreamingStage` validation rules this phase (08) and later plans
register into `VALIDATION_RULE_REGISTRY`. Callers import from the submodule
directly, e.g. ``from dataplat.validate.completeness import CompletenessRule``
-- this package marker re-exports nothing, matching
``dataplat/normalize/__init__.py``'s and ``dataplat/config/__init__.py``'s
shallow re-export convention.
"""

from __future__ import annotations
