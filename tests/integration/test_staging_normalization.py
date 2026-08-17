"""Integration proof: ``customers``' real config normalizes for real, end-to-end (06-16 Task 3).

Drives the REAL ``configs/datasets/customers.yaml`` config (via
``dataplat.config.loader.load_config``, not a locally-reconstructed stand-in
-- unlike ``tests/integration/test_staging_loader.py``'s and
``tests/integration/test_discover_files.py``'s own per-file
``_make_config()``/``_skip_config()`` conventions) through a REAL
``csv_processor.source.CsvSource`` reading a REAL uploaded MinIO object, and
a REAL ``StagingLoader.load()`` call against the throwaway PostgreSQL. This
is deliberately NOT ``test_staging_loader.py``'s ``_FakeSource``/
``_FakeRecordStream`` shape: this file's whole point is proving the ONE real
call site (``StagingLoader._build_stages``, plan 06-16 Task 1) normalizes
``customers``' one real nullable typed column (``birth_date``) correctly
against the real pipeline, not merely against hand-built in-memory chunks.

Five rows cover this task's four required regression proofs in one staged
table: a clean row (proving normalization does not corrupt already-clean
data), an empty-``birth_date`` row (the direct regression proof for the
BLOCKER this plan revision fixes -- ``NullTokenNormalizer`` running before
``DateNormalizer`` for a nullable column), a deliberately-invalid-date row
(proving rejection, not silent staging), and an NFC/NFD ``name`` pair
(proving ``_record_hash`` is NFC-invariant in the real pipeline, not merely
in ``UnicodeNormalizer``'s own isolated unit test).
"""

from __future__ import annotations

import datetime as dt
import hashlib
import unicodedata
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

from csv_processor.source import CsvSource
from dataplat.config.loader import load_config
from dataplat.load.staging import StagingLoader
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.objectstore import S3ObjectStore

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
TARGET_COLUMNS = ("customer_id", "name", "country", "birth_date", "event_ts")

_BUCKET = "raw"
_KEY = "customers/normalize_proof/normalize_proof.csv"

# Byte-distinct, visually-identical forms of the SAME name -- corpus fixture
# 44's own pair (dataplat.normalize.unicode's module docstring), computed via
# unicodedata.normalize() rather than hand-typed so correctness never depends
# on which normalization form this source file itself happened to be saved
# in.
_NFC_NAME = unicodedata.normalize("NFC", "Wiśniewski")
_NFD_NAME = unicodedata.normalize("NFD", "Wiśniewski")

# customer_id, name, country, birth_date, event_ts -- customers' real header
# order (customers.yaml's own comment). Values shaped like
# tests/fixtures/slice-corpus.yaml's own generator output.
_CLEAN_ROW = ("1", "Alice", "US", "1950-03-14", "2026-01-05T08:15:00Z")
_EMPTY_BIRTH_DATE_ROW = ("3", "Carol", "CA", "", "2026-03-01T09:30:00Z")
_INVALID_DATE_ROW = ("4", "Dave", "DE", "2026-13-45", "2026-04-01T10:00:00Z")
# Distinct customer_ids (5, 6) -- NOT the same business key: as of plan
# 08-11, customers.yaml carries a real QUALITY_UNIQUENESS rule on
# customer_id (D-09), so two staged rows sharing one customer_id would
# now correctly have the second REJECT_RECORD-ed before ever reaching
# _record_hash computation, making Assertion 4 below vacuous. NFC-invariant
# hashing is proven per-row instead (see Assertion 4).
_NFC_NAME_ROW = ("5", _NFC_NAME, "PL", "1980-05-20", "2026-05-01T11:00:00Z")
_NFD_NAME_ROW = ("6", _NFD_NAME, "PL", "1980-05-20", "2026-05-01T11:00:00Z")

_ROWS = (
    _CLEAN_ROW,
    _EMPTY_BIRTH_DATE_ROW,
    _INVALID_DATE_ROW,
    _NFC_NAME_ROW,
    _NFD_NAME_ROW,
)


def _csv_body() -> bytes:
    """Build the source CSV's raw bytes: header + five data rows, comma-delimited."""
    lines = [",".join(TARGET_COLUMNS), *(",".join(row) for row in _ROWS)]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _expected_utc_instant(raw: str) -> str:
    """The exact transform ``DateNormalizer`` applies to a ``%z``-bearing timestamp.

    Parses ``raw`` under ``customers.yaml``'s own declared ``event_ts``
    format and renders the resulting instant via ``.astimezone(UTC).isoformat()``
    -- ``DateNormalizer``'s own canonical rendering (``dataplat.normalize.dates``
    ``_parse_plain_format``). A ``"Z"``-suffixed input's canonical UTC
    ``isoformat()`` rendering is ``"+00:00"``, never a literal ``"Z"`` --
    the SAME instant, not a corruption -- so this helper, not a byte-for-byte
    comparison against ``raw`` itself, is this test's correct "did the
    normalizer preserve this value's meaning" oracle for ``event_ts``.
    ``birth_date`` has no such caveat: a bare calendar date's
    ``.date().isoformat()`` rendering is always byte-identical to a
    ``YYYY-MM-DD`` input.
    """
    parsed = dt.datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S%z")
    return parsed.astimezone(dt.UTC).isoformat()


@pytest.fixture(scope="module", autouse=True)
def _ensure_raw_bucket(s3_client: Any) -> None:
    """Ensure `raw` exists on the shared session MinIO container -- idempotent."""
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _BUCKET not in existing:
        s3_client.create_bucket(Bucket=_BUCKET)


@pytest.fixture
def object_store(minio_config: dict[str, str]) -> S3ObjectStore:
    """A real `S3ObjectStore`, built from the same credentials `s3_client` uses."""
    return S3ObjectStore(
        endpoint_url=f"http://{minio_config['endpoint']}",
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
    )


@pytest.fixture
def conn(migrated_dsn: str) -> Iterator[psycopg.Connection[Any]]:
    """One open psycopg connection per test, over the migrated database."""
    with psycopg.connect(migrated_dsn) as connection:
        yield connection


def test_customers_real_config_normalizes_birth_date_and_event_ts_end_to_end(
    object_store: S3ObjectStore,
    conn: psycopg.Connection[Any],
) -> None:
    """The BLOCKER regression proof, plus the other three required assertions, in one run."""
    config = load_config(
        REPO_ROOT / "configs" / "datasets" / "customers.yaml",
        defaults_path=REPO_ROOT / "configs" / "defaults.yaml",
    )
    object_store.put_object(_BUCKET, _KEY, _csv_body())

    ctx = PipelineContext(
        run=RunContext(run_id=90_016, idempotency_key="test-run-90016", file_id=1, batch_id=1),
        config=config,
        metadata=None,  # type: ignore[arg-type]  # unused by StagingLoader.load()
        objects=object_store,
        db=None,  # type: ignore[arg-type]  # unused by StagingLoader.load()
        log=None,  # type: ignore[arg-type]  # unused by StagingLoader.load() (it builds its own logger)
        source=CsvSource(bucket=_BUCKET, key=_KEY),
    )
    loader = StagingLoader(target_columns=TARGET_COLUMNS)

    result = loader.load(ctx, conn)
    conn.commit()

    # --- rows_read/rows_parsed/rows_rejected: exactly one row (the invalid
    # date) is rejected; the other four stage successfully.
    assert result.rows_read == 5
    assert result.rows_parsed == 4
    assert result.rows_rejected == 1

    staged = conn.execute(
        f"""
        SELECT customer_id, name, birth_date, event_ts, _record_hash
          FROM {result.staging_table}
         ORDER BY _source_row_number
        """,  # noqa: S608 -- result.staging_table is derived from config/run identity, never row content (see staging.py's own threat-model comment)
    ).fetchall()
    by_customer_id: dict[str, list[tuple[Any, ...]]] = {}
    for row in staged:
        by_customer_id.setdefault(row[0], []).append(row)

    # --- Assertion 1: a clean, already-canonical row normalizes without
    # corruption. birth_date is byte-identical (a bare date never changes
    # format); event_ts's canonical UTC rendering replaces "Z" with "+00:00"
    # -- the SAME instant, not a corruption (see _expected_utc_instant).
    clean_rows = by_customer_id["1"]
    assert len(clean_rows) == 1
    _, _, clean_birth_date, clean_event_ts, _ = clean_rows[0]
    assert clean_birth_date == _CLEAN_ROW[3]
    assert clean_event_ts == _expected_utc_instant(_CLEAN_ROW[4])

    # --- Assertion 2 (the BLOCKER regression proof): the empty-birth_date
    # row stages successfully with birth_date SQL NULL -- never rejected,
    # never a crash from UnicodeNormalizer downstream when it later
    # encounters this column's now-None value.
    empty_birth_date_rows = by_customer_id["3"]
    assert len(empty_birth_date_rows) == 1
    _, _, empty_birth_date, empty_event_ts, _ = empty_birth_date_rows[0]
    assert empty_birth_date is None
    assert empty_event_ts == _expected_utc_instant(_EMPTY_BIRTH_DATE_ROW[4])

    # --- Assertion 3: the deliberately-invalid date is REJECTED, not
    # silently staged -- absent from the staged table entirely.
    assert "4" not in by_customer_id

    # --- Assertion 4: an NFC-sourced row and an NFD-sourced row (distinct
    # customer_ids -- see _NFD_NAME_ROW's comment) each independently stage
    # with the SAME NFC-normalized name and a hash computed from that NFC
    # form -- UnicodeNormalizer runs before _record_hash is computed, not
    # only in its own unit test. Sanity check FIRST: the two SOURCE forms
    # fed into the CSV really are byte-distinct (this is what makes the
    # proof below non-vacuous) -- unicodedata.normalize("NFC", ...) vs
    # "NFD" always differ in byte length for a character with a canonical
    # decomposition, e.g. "ś".
    assert _NFC_NAME != _NFD_NAME
    nfc_rows = by_customer_id["5"]
    nfd_rows = by_customer_id["6"]
    assert len(nfc_rows) == 1
    assert len(nfd_rows) == 1
    # UnicodeNormalizer runs unconditionally LAST, replacing each row's own
    # `name` field with its NFC form BEFORE _record_hash is computed -- so
    # the once-NFD row's staged name has already converged onto the SAME
    # NFC text as the once-NFC row's, and each row's own hash is computed
    # from that converged NFC form -- this convergence-per-row is exactly
    # what closes T-06-01 in the real pipeline, not merely in
    # UnicodeNormalizer's own isolated unit test.
    assert nfc_rows[0][1] == _NFC_NAME
    assert nfd_rows[0][1] == _NFC_NAME
    expected_nfc_pipe_joined = "|".join(
        ("5", _NFC_NAME, "PL", "1980-05-20", _expected_utc_instant(_NFC_NAME_ROW[4])),
    )
    expected_nfd_pipe_joined = "|".join(
        ("6", _NFC_NAME, "PL", "1980-05-20", _expected_utc_instant(_NFD_NAME_ROW[4])),
    )
    assert (
        bytes(nfc_rows[0][4]) == hashlib.sha256(expected_nfc_pipe_joined.encode("utf-8")).digest()
    )
    assert (
        bytes(nfd_rows[0][4]) == hashlib.sha256(expected_nfd_pipe_joined.encode("utf-8")).digest()
    )
