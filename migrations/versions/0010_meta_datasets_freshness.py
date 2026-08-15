"""meta.datasets freshness columns — the data foundation for OBS-01/OBS-09.

Adds three nullable `interval` columns to `meta.datasets`:
`expected_frequency`, `freshness_warn_after`, `freshness_fail_after`. All
three are nullable with no server default — absence is a real, load-bearing
state (07-CONTEXT.md D-08), not "unset for now": `expected_frequency IS
NULL` means freshness is not tracked for this dataset at all, structurally
distinct from "tracked and currently stale" (OBS-09's exact requirement).

Fed by `configs/datasets/*.yaml`'s new opt-in `freshness:` block via
`dataplat.config.registry.ConfigRegistry.sync()` (see this plan's Task 2) —
never written directly by this migration, matching every other `meta.*`
column's config-not-code provenance in this codebase.

`freshness_warn_after`/`freshness_fail_after` are additional grace periods
layered on top of `expected_frequency` before a warn/fail threshold fires
(07-RESEARCH.md Pattern 3) — evaluated entirely by a Grafana Alert rule
querying Postgres directly (D-10); no new evaluation code, no new table.
`07-RESEARCH.md`'s own note supersedes a stale prior-research artifact
(`ARCHITECTURE.md` §2.2's speculative `meta.dataset_sla` table) — columns
directly on `meta.datasets`, not a new table, is the locked decision.

Revision ID: 0010
Revises: 0009
Create Date: 2026-08-16
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0010"
down_revision = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add the three nullable freshness columns to `meta.datasets`."""
    op.add_column(
        "datasets",
        sa.Column("expected_frequency", sa.Interval(), nullable=True),
        schema="meta",
    )
    op.add_column(
        "datasets",
        sa.Column("freshness_warn_after", sa.Interval(), nullable=True),
        schema="meta",
    )
    op.add_column(
        "datasets",
        sa.Column("freshness_fail_after", sa.Interval(), nullable=True),
        schema="meta",
    )


def downgrade() -> None:
    """Drop the three freshness columns, in reverse order of `upgrade()`."""
    op.drop_column("datasets", "freshness_fail_after", schema="meta")
    op.drop_column("datasets", "freshness_warn_after", schema="meta")
    op.drop_column("datasets", "expected_frequency", schema="meta")
