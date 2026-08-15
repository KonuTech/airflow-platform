"""SQL-only integration test for the freshness-breach condition (OBS-01, OBS-09).

No `dataplat` code under test here -- this proves a raw SQL condition
against `meta.datasets`/`meta.files`/`meta.ingestion_runs`, matching its own
classification in `07-RESEARCH.md`'s Test Map. Three datasets are seeded
directly (never through `ConfigRegistry.sync()` -- that write path is
proven separately, in `test_config_registry.py`'s own new
`test_sync_persists_freshness_config_to_meta_datasets`), each isolating one
structural property this query must get right:

    (a) `expected_frequency IS NULL`      -- freshness never configured for
                                              this dataset. Must NEVER appear
                                              in the breach result set,
                                              structurally (the query's own
                                              `WHERE expected_frequency IS
                                              NOT NULL` excludes it before any
                                              staleness math even runs) --
                                              OBS-09's "no file currently
                                              available" (stays quiet).
    (b) `expected_frequency = '1 hour'`, zero `meta.files`/
        `meta.ingestion_runs` rows ever, `created_at` backdated 2 hours --
        the cold-start case (a dataset that has NEVER received a single
        file). Must appear in the breach result set, proving
        `COALESCE(MAX(f.discovered_at), d.created_at)`'s fallback to
        `meta.datasets.created_at` when no file row exists at all.
    (c) `expected_frequency = '1 day'`, one `meta.files` row discovered 1
        hour ago. Must NOT appear -- fresh, not stale.

`FRESHNESS_BREACH_QUERY` below is kept byte-for-byte identical to
`07-RESEARCH.md`'s Code Examples section (Architecture Patterns) -- plan
07-07's Grafana alert rule embeds this exact SQL text, so this constant is
the one place both are proven to agree; do not let the two drift apart.
"""

from __future__ import annotations

import hashlib

import psycopg

# Verbatim from 07-RESEARCH.md's Code Examples section ("Freshness Grafana
# Alert condition (OBS-01/OBS-09) — the SQL a Postgres-datasource rule
# evaluates"). Plan 07-07's Grafana alert rule must embed this exact text.
FRESHNESS_BREACH_QUERY = """
SELECT
    d.dataset_id,
    d.dataset_name,
    d.expected_frequency,
    d.freshness_warn_after,
    d.freshness_fail_after,
    COALESCE(MAX(f.discovered_at), d.created_at)                        AS last_received_at,
    MAX(r.finished_at) FILTER (WHERE r.status = 'SUCCEEDED')            AS last_success_at,
    now() - COALESCE(MAX(f.discovered_at), d.created_at)                AS processing_delay
FROM meta.datasets d
LEFT JOIN meta.files f          ON f.dataset_id = d.dataset_id
LEFT JOIN meta.ingestion_runs r ON r.dataset_id = d.dataset_id
WHERE d.expected_frequency IS NOT NULL   -- OBS-09: NULL here means "stays quiet", structurally
GROUP BY d.dataset_id, d.dataset_name, d.expected_frequency,
         d.freshness_warn_after, d.freshness_fail_after, d.created_at
HAVING now() - COALESCE(MAX(f.discovered_at), d.created_at)
       > d.expected_frequency + COALESCE(d.freshness_warn_after, interval '0');
"""


def _insert_dataset(
    conn: psycopg.Connection,
    *,
    dataset_name: str,
    expected_frequency: str | None,
) -> int:
    row = conn.execute(
        """
        INSERT INTO meta.datasets (dataset_name, expected_frequency)
        VALUES (%s, %s::interval)
        RETURNING dataset_id
        """,
        (dataset_name, expected_frequency),
    ).fetchone()
    assert row is not None
    return int(row[0])


def _backdate_created_at(conn: psycopg.Connection, *, dataset_id: int, hours_ago: float) -> None:
    conn.execute(
        """
        UPDATE meta.datasets
           SET created_at = now() - (%s || ' hours')::interval
         WHERE dataset_id = %s
        """,
        (hours_ago, dataset_id),
    )


def _insert_file(
    conn: psycopg.Connection,
    *,
    dataset_id: int,
    object_uri: str,
    discovered_hours_ago: float,
) -> None:
    conn.execute(
        """
        INSERT INTO meta.files
            (dataset_id, object_uri, content_sha256, size_bytes, filename, status, discovered_at)
        VALUES
            (%s, %s, %s, %s, %s, %s, now() - (%s || ' hours')::interval)
        """,
        (
            dataset_id,
            object_uri,
            hashlib.sha256(object_uri.encode()).digest(),
            100,
            object_uri.rsplit("/", 1)[-1],
            "DISCOVERED",
            discovered_hours_ago,
        ),
    )


def _breach_dataset_ids(dsn: str) -> set[int]:
    with psycopg.connect(dsn) as conn:
        rows = conn.execute(FRESHNESS_BREACH_QUERY).fetchall()
    return {row[0] for row in rows}


def test_freshness_breach_query_distinguishes_never_configured_cold_start_and_fresh(
    migrated_dsn: str,
) -> None:
    with psycopg.connect(migrated_dsn) as conn:
        # (a) Never configured -- expected_frequency IS NULL.
        never_configured_id = _insert_dataset(
            conn,
            dataset_name="freshness_never_configured",
            expected_frequency=None,
        )

        # (b) Cold start -- configured, but zero meta.files/meta.ingestion_runs
        # rows ever inserted for it; created_at backdated past the 1-hour
        # expected_frequency so the COALESCE fallback to created_at is stale.
        cold_start_id = _insert_dataset(
            conn,
            dataset_name="freshness_cold_start",
            expected_frequency="1 hour",
        )
        _backdate_created_at(conn, dataset_id=cold_start_id, hours_ago=2)

        # (c) Fresh -- configured with a generous 1-day expected_frequency,
        # and a real meta.files row discovered only 1 hour ago.
        fresh_id = _insert_dataset(
            conn,
            dataset_name="freshness_fresh",
            expected_frequency="1 day",
        )
        _insert_file(
            conn,
            dataset_id=fresh_id,
            object_uri="s3://raw/freshness_fresh/recent.csv",
            discovered_hours_ago=1,
        )
        conn.commit()

    breached = _breach_dataset_ids(migrated_dsn)

    # (a) never appears -- structurally excluded by `WHERE expected_frequency
    # IS NOT NULL`, before any staleness math runs (OBS-09).
    assert never_configured_id not in breached, (
        "a dataset with expected_frequency IS NULL must never appear in the "
        "freshness-breach result set, regardless of file/run history"
    )
    # (b) DOES appear -- proves the COALESCE(MAX(f.discovered_at),
    # d.created_at) cold-start fallback fires for a dataset with zero prior
    # file history.
    assert cold_start_id in breached, (
        "a configured dataset that has never received a single file must "
        "appear as breached once its created_at exceeds expected_frequency"
    )
    # (c) does NOT appear -- fresh, not stale.
    assert fresh_id not in breached, (
        "a dataset with a recent file discovery well inside its "
        "expected_frequency must not appear as breached"
    )
