"""tests/e2e/chaos/test_duplicate_batch.py — QUAL-15 scenario: a genuinely duplicate batch claim.

This module's docstring states, per 11-10-PLAN.md's own interfaces-block decision framework,
which existing tests this deliberately does NOT re-prove, and what angle is genuinely new here.

`tests/e2e/slice/test_smoke_and_idempotency.py::test_idempotent_reupload` already proves live that
re-uploading the SAME bytes under a second key produces zero additional `normalized.customers`
rows -- but that is a SEQUENTIAL proof: the second upload happens strictly after the first
upload's own run has already reached `SUCCEEDED`, so `duplicate_of_file_id` at the `meta.files`
discovery layer is the only mechanism exercised. `tests/e2e/slice/test_backfill_reentry.py` proves
a different sequential angle again (re-entry via a genuinely CONTENT-DIFFERING corrected file).
Neither test ever has two attempts racing to process the identical logical unit of work AT THE
SAME TIME -- the exact "duplicate batch" angle 11-10-PLAN.md's own interfaces block names as
genuinely uncovered: "most likely two CONCURRENT (not sequential) triggers racing to claim the
same batch".

This test proves that angle directly, live, against the real analytical database:
`MetadataRepository.claim_ingestion_run` (`packages/dataplat/src/dataplat/metadata/postgres.py`)
is the ONE gate every real `stage`/`publish` CLI invocation must pass before it does ANY
staging/publish work at all -- `dataplat.pipeline.run.stage_ingest`'s own code (`packages/dataplat/
src/dataplat/pipeline/run.py`) reads: `claimed = ctx.metadata.claim_ingestion_run(...); if claimed
is None: return _skipped_receipt(ctx)` -- a losing concurrent attempt is a clean, correct no-op,
never a partial/duplicate write. The claim itself is a single, atomically-evaluated `UPDATE ...
WHERE idempotency_key = %(key)s AND (status IN ('PENDING','FAILED') OR (status='RUNNING' AND
lease_expires_at < now()))`, guarded by `meta.ingestion_runs.idempotency_key`'s own real, live
`UNIQUE` constraint (migration 0004) -- this test races TWO real, independent connections against
the identical row under real PostgreSQL MVCC/row-locking semantics (never mocked, never serialized
by the test itself), and asserts the database -- not application code -- allows exactly one winner.

Deliberately does NOT go through a real file upload or a real `csv_ingest_customers`/
`csv_ingest_orders` DagRun: `claim_ingestion_run`'s own WHERE clause only ever matches a row whose
`status` is `PENDING`/`FAILED`/an expired `RUNNING` lease, and a REAL Airflow-triggered pipeline's
own `stage` pod would almost certainly win the claim before this test's own two racing threads
could ever get a turn (the pod's `claim_ingestion_run` call happens within milliseconds of the
task starting) -- making the "exactly one winner" assertion a test of timing luck against a live,
independently-scheduled pipeline, not of the ledger constraint itself. This test instead inserts
ONE isolated, uniquely-marked `PENDING` `meta.ingestion_runs` row of its own (`file_id`/`batch_id`
both `NULL` -- both nullable per migration 0004; genuinely representative of the row shape
`get_or_create_ingestion_run` produces during real discovery, before any file/batch is definitely
known to be new) using the SAME real `dataset_id`/`config_version_id` a genuine `customers` run
would carry, then races two connections to claim THAT row -- a deterministic, concurrency-safe,
live proof of the identical mechanism, with zero risk of colliding with or being pre-empted by any
concurrently-running production DagRun on this shared cluster.
"""

from __future__ import annotations

import threading
import uuid
from typing import TYPE_CHECKING, Any

import pytest

from tests.e2e.slice.conftest import open_analytics_connection

if TYPE_CHECKING:
    import subprocess
    from collections.abc import Callable

    import psycopg

pytestmark = [pytest.mark.cluster, pytest.mark.chaos]

_DATASET_NAME = "customers"
_CLAIM_RACE_TIMEOUT_SECONDS = 30

# The EXACT SQL `MetadataRepository.claim_ingestion_run` issues (`packages/dataplat/src/dataplat/
# metadata/postgres.py`), reproduced here rather than imported: this test's own two-connection
# race needs each claim attempt on its OWN psycopg connection, never sharing one across threads,
# and `PostgresMetadataRepository` wraps a `psycopg_pool.ConnectionPool` this test has no clean way
# to construct with a live-resolved, port-forwarded DSN without reaching into `tests/e2e/slice/
# conftest.py`'s own PRIVATE credential-resolution helpers (that module's own docstring: private,
# leading-underscore names are copied in shape, never imported, by design). The WHERE clause below
# is the real, live-migrated (0004) UNIQUE `idempotency_key` + conditional-claim ledger mechanism
# under test, byte-for-byte identical to production (verified against the cited source file at
# execution time) -- LOAD-08-adjacent, exercised directly, never reimplemented differently.
_CLAIM_SQL = """
    UPDATE meta.ingestion_runs
       SET status = 'RUNNING',
           try_number = 1,
           k8s_pod_name = %(pod_name)s,
           started_at = COALESCE(started_at, now()),
           lease_expires_at = now() + interval '5 minutes'
     WHERE idempotency_key = %(key)s
       AND (
           status IN ('PENDING', 'FAILED')
           OR (status = 'RUNNING' AND lease_expires_at < now())
       )
    RETURNING run_id, status
"""


def _current_dataset_and_config_version(conn: psycopg.Connection[Any]) -> tuple[int, int]:
    """Return `(dataset_id, config_version_id)` for the dataset's CURRENT (`valid_to IS NULL`)
    config."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT d.dataset_id, cv.config_version_id "
            "FROM meta.datasets d "
            "JOIN meta.config_versions cv ON cv.dataset_id = d.dataset_id AND cv.valid_to IS NULL "
            "WHERE d.dataset_name = %s",
            (_DATASET_NAME,),
        )
        row = cur.fetchone()
    assert row is not None, (
        f"no CURRENT (valid_to IS NULL) meta.config_versions row for dataset={_DATASET_NAME!r} "
        f"-- this test needs the dataset already synced at least once"
    )
    return int(row[0]), int(row[1])


def _insert_pending_run(
    conn: psycopg.Connection[Any],
    *,
    idempotency_key: str,
    dataset_id: int,
    config_version_id: int,
) -> int:
    """Insert one isolated, uniquely-marked PENDING `meta.ingestion_runs` row (module docstring)."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO meta.ingestion_runs (
                idempotency_key, dataset_id, file_id, batch_id,
                config_version_id, processor_version, processor_image_digest, status
            ) VALUES (%s, %s, NULL, NULL, %s, %s, %s, 'PENDING')
            RETURNING run_id
            """,
            (
                idempotency_key,
                dataset_id,
                config_version_id,
                "e2e-chaos-duplicate-batch-test",
                "e2e-chaos-duplicate-batch-test",
            ),
        )
        row = cur.fetchone()
        conn.commit()
    assert row is not None
    return int(row[0])


def _claim(
    conn: psycopg.Connection[Any],
    *,
    idempotency_key: str,
    pod_name: str,
    barrier: threading.Barrier,
    results: dict[str, tuple[int, str] | None],
) -> None:
    """Wait at `barrier`, then fire the claim UPDATE — run inside a thread by the test."""
    barrier.wait(timeout=_CLAIM_RACE_TIMEOUT_SECONDS)
    with conn.cursor() as cur:
        cur.execute(_CLAIM_SQL, {"pod_name": pod_name, "key": idempotency_key})
        row = cur.fetchone()
        conn.commit()
    results[pod_name] = None if row is None else (int(row[0]), str(row[1]))


def test_two_concurrent_claims_of_the_identical_batch_never_both_win(
    kubectl_context: str,
    kubectl_json: Callable[..., Any],
    kubectl: Callable[..., subprocess.CompletedProcess[str]],
    vault_addr: str,
    analytics_connection: psycopg.Connection[Any],
) -> None:
    """LOAD-08-adjacent: two real, independent connections race the identical claim; exactly one
    wins.

    Opens TWO independent `etl_app`-authenticated connections (the exact role real pipeline pods
    authenticate as) via `open_analytics_connection` — `analytics_connection` itself is the THIRD,
    used only to set up and independently re-verify the row, never to race. Fires both claim
    attempts from separate threads, synchronized on a `threading.Barrier` so neither gets a head
    start, and asserts: (1) exactly one of the two returns a non-`None` `(run_id, status)`, (2) the
    other returns `None` (the real, correct "someone else already has this" outcome
    `stage_ingest`'s own code treats as a clean skip, never an error), (3) the row itself confirms
    `status='RUNNING'` with `k8s_pod_name` equal to the WINNING thread's own marker — never both,
    never neither, never a corrupted mix of the two attempts' values.
    """
    marker = uuid.uuid4().hex[:12]
    idempotency_key = f"e2e-chaos-duplicate-batch-{marker}"
    pod_name_a = f"e2e-chaos-claimant-a-{marker}"
    pod_name_b = f"e2e-chaos-claimant-b-{marker}"

    dataset_id, config_version_id = _current_dataset_and_config_version(analytics_connection)
    run_id = _insert_pending_run(
        analytics_connection,
        idempotency_key=idempotency_key,
        dataset_id=dataset_id,
        config_version_id=config_version_id,
    )

    barrier = threading.Barrier(2)
    results: dict[str, tuple[int, str] | None] = {}

    with (
        open_analytics_connection(
            kubectl_context, kubectl_json, kubectl, vault_addr, role="etl_app"
        ) as conn_a,
        open_analytics_connection(
            kubectl_context, kubectl_json, kubectl, vault_addr, role="etl_app"
        ) as conn_b,
    ):
        thread_a = threading.Thread(
            target=_claim,
            kwargs={
                "conn": conn_a,
                "idempotency_key": idempotency_key,
                "pod_name": pod_name_a,
                "barrier": barrier,
                "results": results,
            },
        )
        thread_b = threading.Thread(
            target=_claim,
            kwargs={
                "conn": conn_b,
                "idempotency_key": idempotency_key,
                "pod_name": pod_name_b,
                "barrier": barrier,
                "results": results,
            },
        )
        thread_a.start()
        thread_b.start()
        thread_a.join(timeout=_CLAIM_RACE_TIMEOUT_SECONDS)
        thread_b.join(timeout=_CLAIM_RACE_TIMEOUT_SECONDS)

    assert not thread_a.is_alive(), "claimant A's thread never finished within the race timeout"
    assert not thread_b.is_alive(), "claimant B's thread never finished within the race timeout"
    assert set(results) == {pod_name_a, pod_name_b}, (
        f"expected a result recorded for both claimants, got {results!r}"
    )

    winners = {pod_name: outcome for pod_name, outcome in results.items() if outcome is not None}
    losers = {pod_name: outcome for pod_name, outcome in results.items() if outcome is None}
    assert len(winners) == 1, (
        f"expected EXACTLY ONE claimant to win the race for idempotency_key={idempotency_key!r}, "
        f"got {len(winners)} winner(s): {winners!r} (run_id={run_id!r}) -- LOAD-08's ledger "
        f"constraint failed to serialize two concurrent claims of the identical batch"
    )
    assert len(losers) == 1, (
        f"expected exactly one loser (claim_ingestion_run returned None), got {losers!r}"
    )

    winning_pod_name = next(iter(winners))
    winning_run_id, winning_status = winners[winning_pod_name]
    assert winning_run_id == run_id, (
        f"the winning claim's own run_id ({winning_run_id!r}) does not match the row this test "
        f"inserted ({run_id!r})"
    )
    assert winning_status == "RUNNING", (
        f"expected the winning claim's status to be 'RUNNING', got {winning_status!r}"
    )

    with analytics_connection.cursor() as cur:
        cur.execute(
            "SELECT status, k8s_pod_name FROM meta.ingestion_runs WHERE run_id = %s",
            (run_id,),
        )
        final_row = cur.fetchone()
    assert final_row is not None, f"meta.ingestion_runs row for run_id={run_id!r} disappeared"
    final_status, final_pod_name = final_row
    assert final_status == "RUNNING", (
        f"expected the row's final status to be 'RUNNING' (claimed exactly once), got "
        f"{final_status!r}"
    )
    assert final_pod_name == winning_pod_name, (
        f"expected the row's final k8s_pod_name to be the WINNING claimant's own marker "
        f"({winning_pod_name!r}), got {final_pod_name!r} -- a mismatch here would mean the "
        f"'losing' claim's UPDATE partially or eventually landed anyway, corrupting the winner's "
        f"own claimed identity"
    )
