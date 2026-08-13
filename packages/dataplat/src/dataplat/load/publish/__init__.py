"""``Publisher`` — the protocol a staging table's rows are committed through.

Callers import from the submodule directly, e.g.
``from dataplat.load.publish.protocol import Publisher, PublishResult`` —
this package marker re-exports nothing, matching
``dataplat/config/__init__.py``'s shallow re-export convention. This phase
defines the protocol only; ``merge`` arrives in Phase 4, SCD/CDC publishers
in Phase 10.
"""

from __future__ import annotations
