"""tests/e2e/slice/test_dbt_silver_pipeline.py -- 08.1-13 Task 2/3's own live proof.

This suite is the phase's own final gate (08.1-13-PLAN.md's own words): every earlier
plan in phase 08.1 built or tested one piece of the `discover -> stage -> dbt_build ->
publish` chain in isolation (testcontainers, mocked KPO, unit coverage). This file is
the only place a REAL file flows through the WHOLE chain on the REAL live cluster and
lands, deduplicated, in both `silver.customers` (dbt's own domain, migration 0021+) and
`normalized.customers` (the pre-existing gold target `publish` has always owned).

Shares `tests/e2e/slice/conftest.py`'s established fixtures/helpers (`slice_fixtures_dir`,
`analytics_connection`, `poll_file_discovered`/`poll_run_for_file`/`poll_ingestion_run`)
rather than re-deriving them -- this suite is a SIBLING of `test_smoke_and_idempotency.py`
and `test_backfill_reentry.py` in the same directory, not a re-implementation.

`_unique_marked_csv_bytes`'s marker-in-name-field shape is copied (not imported) from
`test_smoke_and_idempotency.py`'s own `_unique_small_csv_bytes`, matching that module's
own documented convention (its docstring: "small helpers are copied per test tier, not
shared through a library module") -- it additionally composes
`conftest.py::large_csv_with_offset_customer_ids` (imported, not copied: it is already
shared infrastructure `test_pod_kill_retry.py`/`test_concurrent_select.py` both import)
to avoid a genuine, live-confirmed business-key collision (see `_OFFSET_LOW`/
`_OFFSET_HIGH`'s own comment below).
"""

from __future__ import annotations

import contextlib
import logging
import random
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import (
    large_csv_with_offset_customer_ids,
    poll_file_discovered,
    poll_ingestion_run,
    poll_run_for_file,
)

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    import psycopg

pytestmark = pytest.mark.cluster

# OBS-03's `print`/`pprint` ban (T20) is repository-wide, `tests/**` included --
# `tests/policy/test_print_ban_scope.py` guards against adding a third carve-out
# beyond `scripts/**`/`tools/corpus/__main__.py`. This suite's own checkpoint
# (08.1-13-PLAN.md Task 2) needs the four verification queries' results visible in
# this session's output for human review; `logging` (not `print`) is the
# established escape hatch -- invoke with `pytest -s --log-cli-level=INFO` to see
# it live.
_logger = logging.getLogger(__name__)

_CUSTOMERS_DATASET = "customers"

# Generous relative to test_smoke_and_idempotency.py's own 180s
# _INGEST_TIMEOUT_SECONDS: this run now waits on discover -> stage -> dbt_build ->
# publish (4 KPO pods, including a real `dbt run` invocation), not the old single
# `ingest` task.
_DAG_RUN_TIMEOUT_SECONDS = 180
_INGEST_TIMEOUT_SECONDS = 300

# tests/fixtures/slice-corpus.yaml's own `customers_small.csv` row_spec:
# `customer_id: {kind: zero_padded_int, width: 6, start: 1}` -- 120 rows,
# business keys 1..120. Found live (08.1-13 Task 2's own first run): this
# literal 1..120 range is ALSO used verbatim by
# `test_smoke_and_idempotency.py`'s own small-fixture test AND by this
# cluster's background per-minute `csv_ingest_customers` schedule replaying
# earlier debugging sessions' own uploads of the same static fixture --
# asserting against the fixture's literal `customer_id=1` business key
# raced against whichever OTHER upload most recently "won" silver's
# business-key dedup competition for that same key, not necessarily THIS
# test's own upload (a genuine, confirmed collision, not a hypothetical:
# `meta.v_customers_lineage`'s dbt-hop columns resolved to a DIFFERENT,
# older run's `meta.dedup_audit` row on a live re-check). Fixed the same way
# `conftest.py::large_csv_with_offset_customer_ids` already fixed this exact
# problem for `customers_large.csv` (`test_pod_kill_retry.py`,
# `test_concurrent_select.py`): shift every row's `customer_id` by a fresh,
# randomly-chosen offset per run, so this test's own business keys never
# collide with the small fixture's own literal range, this suite's sibling
# tests, or background cluster activity replaying the same static fixture.
_OFFSET_LOW = 2_000_000
_OFFSET_HIGH = 1_000_000_000


def _unique_marked_csv_bytes(base_bytes: bytes, *, offset: int) -> bytes:
    """Return `base_bytes` with every `customer_id` shifted by `offset` and a fresh name marker.

    Combines `conftest.py::large_csv_with_offset_customer_ids` (business-key
    collision avoidance, this module's own docstring above) with
    `test_smoke_and_idempotency.py::_unique_small_csv_bytes`'s own marker
    convention (a human-readable breadcrumb in the first data row's `name`
    field, visible in this test's own logged query output). The offset alone
    already guarantees a fresh `content_sha256` (every row's leading field
    changes), so the marker here is for log readability, not deduplication.
    """
    offset_bytes = large_csv_with_offset_customer_ids(base_bytes, offset=offset)
    marker = uuid.uuid4().hex[:12]
    lines = offset_bytes.decode("utf-8").split("\n")
    header, first_data_row, *rest = lines
    fields = first_data_row.split(",")
    fields[1] = f"E2E-DbtSilver-{marker}"
    new_first_row = ",".join(fields)
    return "\n".join([header, new_first_row, *rest]).encode("utf-8")


def test_fresh_customers_file_flows_through_stage_dbt_build_publish(
    s3_client: Callable[[str], Any],
    analytics_connection: psycopg.Connection[Any],
    slice_fixtures_dir: Path,
) -> None:
    """08.1-13 Task 2: a fresh file flows, unattended, through the complete
    `discover -> stage -> dbt_build -> publish` chain and lands correctly in BOTH
    `silver.customers` (dbt's domain) and `normalized.customers` (the pre-existing
    gold target), with a real `meta.dedup_audit` row and a lineage view that resolves
    the whole chain including the new dbt-hop columns 08.1-13's own migrations (0024-
    0026) added.

    This is a live-only proof: testcontainers integration tests already cover the SQL
    the `dbt` models/`publish` CLI run, and `tests/dagtest/` already covers the DAG's
    task-graph mechanics with `KubernetesPodOperator.execute` mocked -- neither proves
    a REAL `dbt run` invocation, authenticated via the REAL `dbt` Vault identity,
    against the REAL live analytical PostgreSQL, actually lands rows. This test does.
    """
    app = s3_client("app")
    admin = s3_client("admin")

    offset = random.SystemRandom().randint(_OFFSET_LOW, _OFFSET_HIGH)
    first_row_customer_id_int = offset + 1
    first_row_customer_id_text = f"{first_row_customer_id_int:06d}"

    base_bytes = (slice_fixtures_dir / "customers_small.csv").read_bytes()
    payload = _unique_marked_csv_bytes(base_bytes, offset=offset)

    marker = uuid.uuid4().hex[:12]
    key = f"customers/e2e-dbt-silver-{marker}.csv"
    object_uri = f"s3://raw/{key}"

    try:
        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM normalized.customers WHERE customer_id = %s::int",
                (first_row_customer_id_int,),
            )
            (pre_upload_count,) = cur.fetchone()
        assert pre_upload_count == 0, (
            f"normalized.customers already has a row for customer_id="
            f"{first_row_customer_id_int!r} before upload -- the random offset "
            f"collided with a prior run's data"
        )

        app.put_object(Bucket="raw", Key=key, Body=payload)

        file_row = poll_file_discovered(
            analytics_connection,
            dataset=_CUSTOMERS_DATASET,
            object_uri=object_uri,
            timeout=_DAG_RUN_TIMEOUT_SECONDS,
        )
        assert file_row["duplicate_of_file_id"] is None, (
            f"the freshly-marked upload was already marked a duplicate (of "
            f"file_id={file_row['duplicate_of_file_id']!r}) -- the uniqueness marker "
            f"did not make this content genuinely new"
        )

        run_row = poll_run_for_file(analytics_connection, file_id=file_row["file_id"], timeout=30)
        outcome = poll_ingestion_run(
            analytics_connection,
            run_row["idempotency_key"],
            timeout=_INGEST_TIMEOUT_SECONDS,
        )
        assert outcome["status"] == "SUCCEEDED", (
            f"ingestion run for {object_uri!r} finished {outcome['status']!r}, not "
            f"SUCCEEDED -- the discover -> stage -> dbt_build -> publish chain did not "
            f"complete cleanly (check the Airflow UI graph view / KPO pod logs for the "
            f"run identified by idempotency_key={run_row['idempotency_key']!r})"
        )

        # Query 1: silver.customers -- dbt's own domain, one row, correctly typed.
        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT customer_id, name, country, birth_date, event_ts "
                "FROM silver.customers WHERE customer_id = %s",
                (first_row_customer_id_text,),
            )
            silver_rows = cur.fetchall()
        _logger.info(
            "[Query 1] silver.customers WHERE customer_id = '%s': %r",
            first_row_customer_id_text,
            silver_rows,
        )
        assert len(silver_rows) == 1, (
            f"expected exactly ONE silver.customers row for customer_id="
            f"{first_row_customer_id_text!r} (business-key deduplicated), found "
            f"{len(silver_rows)}: {silver_rows!r}"
        )

        # Query 2: normalized.customers -- the pre-existing gold target, matching content.
        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT customer_id, name, country, birth_date, event_ts "
                "FROM normalized.customers WHERE customer_id = %s::int",
                (first_row_customer_id_int,),
            )
            normalized_rows = cur.fetchall()
        _logger.info(
            "[Query 2] normalized.customers WHERE customer_id = %s::int: %r",
            first_row_customer_id_int,
            normalized_rows,
        )
        assert len(normalized_rows) == 1, (
            f"expected exactly ONE normalized.customers row for customer_id="
            f"{first_row_customer_id_int!r}, found {len(normalized_rows)}: "
            f"{normalized_rows!r}"
        )

        # Query 3: meta.dedup_audit -- a real row with plausible counts for this run's
        # dbt invocation. `model_name` is the LITERAL `dataset_name` argument
        # `dedup_audit_post_hook(...)` was called with in dbt/models/silver/
        # silver_customers.sql (`dataset_name='customers'`) -- verified live against
        # this cluster's own meta.dedup_audit rows, NOT 'silver_customers' (the value
        # this plan's own checkpoint text assumed; corrected here as a Rule 1 fix,
        # confirmed against dbt/macros/dedup_audit_post_hook.sql's own docstring:
        # "also used as `meta.dedup_audit.model_name`").
        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT dedup_audit_id, dbt_invocation_id, model_name, records_received, "
                "records_accepted, records_rejected, records_deduplicated, run_at "
                "FROM meta.dedup_audit WHERE model_name = 'customers' "
                "ORDER BY dedup_audit_id DESC LIMIT 1",
            )
            dedup_audit_row = cur.fetchone()
        _logger.info(
            "[Query 3] meta.dedup_audit WHERE model_name = 'customers' "
            "ORDER BY dedup_audit_id DESC LIMIT 1: %r",
            dedup_audit_row,
        )
        assert dedup_audit_row is not None, (
            "meta.dedup_audit has NO row for model_name='customers' -- dbt_build "
            "never ran, or the dbt model never wrote its own audit row"
        )
        records_received = dedup_audit_row[3]
        records_accepted = dedup_audit_row[4]
        assert records_received >= 1, (
            f"meta.dedup_audit's most recent model_name='customers' row has "
            f"records_received={records_received!r}, expected >= 1"
        )
        assert records_accepted >= 1, (
            f"meta.dedup_audit's most recent model_name='customers' row has "
            f"records_accepted={records_accepted!r}, expected >= 1"
        )

        # Query 4: meta.v_customers_lineage -- resolves the FULL chain, including the
        # new dbt-hop columns (migration 0026), none NULL for this row.
        with analytics_connection.cursor() as cur:
            cur.execute(
                "SELECT customer_id, file_id, batch_id, run_id, silver_loaded_at, "
                "dbt_invocation_id, dbt_run_at, dbt_invocation_records_deduplicated "
                "FROM meta.v_customers_lineage WHERE customer_id = %s::int",
                (first_row_customer_id_int,),
            )
            lineage_rows = cur.fetchall()
        _logger.info(
            "[Query 4] meta.v_customers_lineage WHERE customer_id = %s::int: %r",
            first_row_customer_id_int,
            lineage_rows,
        )
        assert len(lineage_rows) == 1, (
            f"expected exactly ONE meta.v_customers_lineage row for customer_id="
            f"{first_row_customer_id_int!r}, found {len(lineage_rows)}: {lineage_rows!r}"
        )
        (
            _customer_id,
            _file_id,
            _batch_id,
            _run_id,
            silver_loaded_at,
            dbt_invocation_id,
            dbt_run_at,
            dbt_invocation_records_deduplicated,
        ) = lineage_rows[0]
        for column_name, value in (
            ("silver_loaded_at", silver_loaded_at),
            ("dbt_invocation_id", dbt_invocation_id),
            ("dbt_run_at", dbt_run_at),
            ("dbt_invocation_records_deduplicated", dbt_invocation_records_deduplicated),
        ):
            assert value is not None, (
                f"meta.v_customers_lineage.{column_name} is NULL for customer_id="
                f"{first_row_customer_id_int!r} -- the dbt hop never joined into this "
                f"row's lineage"
            )
    finally:
        # Best-effort cleanup: S3 DELETE is idempotent (a missing key is not an
        # error) -- same discipline as test_smoke_and_idempotency.py's own finally
        # block. Never masks a real assertion failure above with a cleanup exception.
        with contextlib.suppress(Exception):
            admin.delete_object(Bucket="raw", Key=key)
