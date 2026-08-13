"""Pipeline composition: ``PipelineContext``, the ``Stage`` protocols, and the sequencing engine.

Callers import from the submodule directly, e.g.
``from dataplat.pipeline.protocol import PipelineContext`` — this package
marker re-exports nothing, matching ``dataplat/config/__init__.py``'s and
``dataplat/metadata/__init__.py``'s shallow re-export convention.
"""

from __future__ import annotations
