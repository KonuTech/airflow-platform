"""Publication: how staged records reach their target table.

Callers import from the submodule directly, e.g.
``from dataplat.load.publish.protocol import Publisher``. This phase defines
only the ``load.publish`` protocol; staging and concrete publication
strategies (``merge``, SCD/CDC) are later phases' work.
"""

from __future__ import annotations
