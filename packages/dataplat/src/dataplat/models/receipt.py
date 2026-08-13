"""``Receipt`` -- the <=4KB XCom-budget outcome one ``ingest`` pod hands back to Airflow.

Derived this session from ARCHITECTURE.md Sec 6.3 (line 743-745), adapted the
same way ``assignment.py`` adapts Sec 6.2: ``quarantined`` is dropped --
no quarantine concept exists until Phase 8 (04-03-PLAN.md Interfaces).

Written to ``/airflow/xcom/return.json`` by a later plan's ``ingest`` CLI --
never returned as a bare dict, so the shape stays a versioned contract a
DAG's downstream task can ``model_validate_json`` instead of trusting
untyped keys.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class Receipt(BaseModel):
    """The outcome of one ingestion run, sized to fit Airflow's XCom budget.

    ARCHITECTURE.md Sec 6.3 fixes a practical <=4KB ceiling on this
    document -- every field here is a scalar (never a per-row list), so a
    ``model_dump_json()`` of this model stays well under that ceiling
    regardless of how large the source file was.

    Attributes:
        run_id: The ``meta.ingestion_runs.run_id`` this receipt reports on.
        status: The run's terminal status, e.g. ``"SUCCEEDED"``, ``"FAILED"``.
        rows_read: Number of rows read from the source file.
        rows_loaded: Number of rows actually published to the target table.
        rows_invalid: Number of rows rejected by validation.
        rows_deduplicated: Number of rows collapsed by deduplication.
        duration_ms: Wall-clock duration of the run, in milliseconds.
        report_uri: Object-store URI of a fuller validation report, when
            one was written. ``None`` when no such report exists.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: int
    status: str
    rows_read: int
    rows_loaded: int
    rows_invalid: int
    rows_deduplicated: int
    duration_ms: int
    report_uri: str | None = None
