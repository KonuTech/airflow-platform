"""Property test for the fully-wired pipeline's determinism -- QUAL-16, T-06-23.

This is deliberately the LAST plan of Phase 6. 06-RESEARCH.md's own Code
Examples section sketched this test's shape against an abstract
``run_pipeline_and_hash`` placeholder because "that entry point does not
exist until later this phase's tasks land" -- it exists now:
``dataplat.load.staging.StagingLoader.load()`` normalizes for real
(plan 06-16) and computes ``_record_hash`` over normalized values, and
``csv_processor.source.CsvSource`` runs real encoding/dialect/header
detection (plan 06-14/06-15) rather than 03-CONTEXT.md D-01's old hardcoded
UTF-8/comma/header-row-0 shape. This file drives that REAL entry point,
adapted from the abstract sketch exactly as 06-17-PLAN.md's own
``<interfaces>`` instructs.

Two properties, mirroring ``tests/property/test_chunking_properties.py``'s
structure (module docstring, a ``@st.composite`` strategy over one of this
platform's own fixture pools, a budget-rationale comment on ``@settings``):

1. ``test_identical_input_yields_identical_output_hash`` -- staging the
   IDENTICAL source bytes under the IDENTICAL ``DatasetConfig`` twice (two
   independent ``run_id``s/staging tables) produces the SAME ordered
   ``_record_hash`` list both times, over a range of Hypothesis-generated
   ``customers``-shaped tables -- not just one hand-picked example. Rows are
   drawn from ``tests/fixtures/slice-corpus.yaml``'s own ``customers_small.csv``
   value pools (never arbitrary text) and are always non-null,
   always-parseable under ``customers.yaml``'s own declared ``strptime``
   formats: this property is about determinism given VALID input, not about
   re-testing every rejection path plans 06-09/06-11 already cover elsewhere
   in this phase.

2. ``test_a_genuine_normalization_config_change_yields_a_different_hash_set``
   -- separately proves the hash is NOT vacuously constant. ``customers``
   has no numeric/currency column (D-12's consequence, 06-CONTEXT.md), so a
   ``decimal_separator`` difference -- 06-RESEARCH.md's own named example --
   cannot exercise this dataset's one real nullable column. Instead this
   test changes ``NormalizationConfig.null_sentinels`` (still a
   NormalizationConfig field, still genuinely normalization-relevant): under
   ``customers.yaml``'s real config (no ``normalization:`` block at all,
   D-14's platform default NULL-token set -- empty string only), the literal
   string ``"N/A"`` is not a recognized ``birth_date`` sentinel, so
   ``DateNormalizer`` fails to parse it and the row is REJECTED. Declaring
   ``null_sentinels={"birth_date": ["N/A"]}`` recognizes it instead, and the
   SAME row stages successfully with ``birth_date`` SQL ``NULL`` -- a
   genuine, deterministic, config-caused difference in what gets hashed, not
   a coincidence dependent on which values Hypothesis happened to draw.

Both properties drive REAL ``StagingLoader.load()`` calls against a
throwaway testcontainers PostgreSQL + MinIO (via
``tests/integration/conftest.py``'s fixtures, re-exported into this module
below -- ``tests/property/`` is a SIBLING of ``tests/integration/``, not a
descendant, so pytest's own conftest inheritance cannot reach those fixtures
for free; ``tests/e2e/slice/conftest.py`` already establishes this exact
cross-directory import-and-re-export pattern for the identical reason).
Marked ``@pytest.mark.integration`` (needs Docker) so it runs alongside the
rest of the Docker-dependent suite, not the fast offline default.
"""

from __future__ import annotations

import itertools
from pathlib import Path
from typing import TYPE_CHECKING, Any

import psycopg
import pytest

# PyYAML ships no `py.typed` and `types-PyYAML` is not in the `dev` group --
# same suppression, same reasoning, as `dataplat.config.loader`'s own
# `import yaml` line (this plan's declared file scope does not include
# `pyproject.toml`'s dependency groups either). Narrowed to this one import
# rather than a project-wide `ignore_missing_imports` override.
import yaml  # type: ignore[import-untyped]
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from csv_processor.source import CsvSource
from dataplat.config.loader import load_config
from dataplat.config.model import DatasetConfig, NormalizationConfig
from dataplat.load.staging import StagingLoader, StagingResult
from dataplat.models.identity import RunContext
from dataplat.pipeline.protocol import PipelineContext
from dataplat.storage.objectstore import S3ObjectStore
from tests.integration.conftest import (  # noqa: F401 -- re-exported as pytest fixtures below
    _require_docker,
    migrated_dsn,
    minio_config,
    postgres_dsn,
    run_migrations,
    s3_client,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
_SLICE_CORPUS_PATH = REPO_ROOT / "tests" / "fixtures" / "slice-corpus.yaml"

# customer_id, name, country, birth_date, event_ts -- customers' real header
# order (customers.yaml's own comment; StagingLoader has no header-to-column
# name mapping, only positional correspondence -- 04-04-SUMMARY.md's own
# documented pattern).
TARGET_COLUMNS = ("customer_id", "name", "country", "birth_date", "event_ts")

_BUCKET = "raw"
_MIN_ROWS = 2
_MAX_ROWS = 10

# Fresh, globally-unique identifiers for every real StagingLoader.load() call
# and every uploaded object across this whole module -- shared by BOTH test
# functions and every Hypothesis-internal example within
# test_identical_input_yields_identical_output_hash (see that test's own
# suppress_health_check comment for why a single connection/object_store is
# intentionally reused across examples). A high starting point avoids
# colliding with other integration test files' own hardcoded run_id literals
# (e.g. tests/integration/test_staging_loader.py's 101-106,
# test_staging_normalization.py's 90_016) even though nothing here shares a
# database session with those files' own hardcoded values.
_run_ids = itertools.count(90_100)
_upload_ids = itertools.count(1)


def _slice_corpus_pool(column: str) -> tuple[str, ...]:
    """Read ``customers_small.csv``'s own value pool for ``column`` from slice-corpus.yaml.

    Reused verbatim, never re-typed -- this property's rows must look like
    the platform's one real dataset, not an independently-invented fixture
    (06-CONTEXT.md's "reuse tests/fixtures/slice-corpus.yaml's own value
    pools as the generation source" instruction). Parsing the real YAML
    document (rather than hand-copying its lists into this file) means a
    future edit to the manifest's pools can never silently drift from what
    this property actually exercises.
    """
    document = yaml.safe_load(_SLICE_CORPUS_PATH.read_text(encoding="utf-8"))
    fixtures_by_name = {fixture["name"]: fixture for fixture in document["fixtures"]}
    values = fixtures_by_name["customers_small.csv"]["row_spec"][column]["values"]
    return tuple(values)


_NAMES = _slice_corpus_pool("name")
_COUNTRIES = _slice_corpus_pool("country")
_BIRTH_DATES = _slice_corpus_pool("birth_date")
_EVENT_TIMESTAMPS = _slice_corpus_pool("event_ts")


@st.composite
def _customers_rows(draw: st.DrawFn) -> tuple[tuple[str, str, str, str, str], ...]:
    """Draw 2-10 well-formed, always-parseable ``customers``-shaped rows.

    ``customer_id`` is a bounded integer (rendered as its decimal string --
    ``customer_id`` is contract-typed ``string``, never normalized, so no
    particular rendering is required beyond being a plain digit string);
    every other field is drawn from ``slice-corpus.yaml``'s own fixed pools.
    Duplicate values (including duplicate ``customer_id``s) are permitted
    and harmless: the staging table this property drives carries no unique
    constraint (``dataplat.load.staging`` module docstring, Pitfall 9).
    """
    row_count = draw(st.integers(min_value=_MIN_ROWS, max_value=_MAX_ROWS))
    rows: list[tuple[str, str, str, str, str]] = []
    for _ in range(row_count):
        customer_id = draw(st.integers(min_value=1, max_value=999_999))
        name = draw(st.sampled_from(_NAMES))
        country = draw(st.sampled_from(_COUNTRIES))
        birth_date = draw(st.sampled_from(_BIRTH_DATES))
        event_ts = draw(st.sampled_from(_EVENT_TIMESTAMPS))
        rows.append((str(customer_id), name, country, birth_date, event_ts))
    return tuple(rows)


def _csv_bytes(rows: tuple[tuple[str, ...], ...]) -> bytes:
    """Serialize ``rows`` under ``TARGET_COLUMNS``'s header, comma-delimited.

    A naive ``",".join`` (never ``csv.writer``) mirrors
    ``tests/integration/test_staging_normalization.py``'s own
    ``_csv_body()`` convention -- safe here because none of this property's
    pool values (names, ISO country codes, ISO dates/timestamps) ever
    contain a comma or a double quote.
    """
    lines = [",".join(TARGET_COLUMNS), *(",".join(row) for row in rows)]
    return ("\n".join(lines) + "\n").encode("utf-8")


def _upload(object_store: S3ObjectStore, rows: tuple[tuple[str, ...], ...]) -> str:
    """Upload ``rows`` as one CSV object under a fresh key; return the key.

    One upload per call -- callers that need "the identical source bytes"
    staged more than once reuse the returned key across multiple
    ``_stage_and_hash`` calls rather than uploading again.
    """
    key = f"customers/determinism_proof/{next(_upload_ids)}.csv"
    object_store.put_object(_BUCKET, key, _csv_bytes(rows))
    return key


def _stage_and_hash(
    *,
    config: DatasetConfig,
    object_store: S3ObjectStore,
    key: str,
    conn: psycopg.Connection[Any],
) -> tuple[StagingResult, list[bytes]]:
    """Run one real ``StagingLoader.load()`` against a fresh ``run_id``; return result and hashes.

    Mirrors ``test_staging_normalization.py``'s real-``PipelineContext``
    shape exactly (``metadata``/``db``/``log`` unused by ``load()``,
    ``dataset_id`` left unset on ``CsvSource`` -- schema resolution is
    plan 06-15's own separate concern, not this determinism property's).
    """
    run_id = next(_run_ids)
    ctx = PipelineContext(
        run=RunContext(
            run_id=run_id,
            idempotency_key=f"determinism-proof-{run_id}",
            file_id=1,
            batch_id=1,
        ),
        config=config,
        metadata=None,  # type: ignore[arg-type]  # unused by StagingLoader.load()
        objects=object_store,
        db=None,  # type: ignore[arg-type]  # unused by StagingLoader.load()
        log=None,  # type: ignore[arg-type]  # unused by StagingLoader.load() (it builds its own logger)
        source=CsvSource(bucket=_BUCKET, key=key),
    )
    result = StagingLoader(target_columns=TARGET_COLUMNS).load(ctx, conn)
    conn.commit()
    staged = conn.execute(
        f"SELECT _record_hash FROM {result.staging_table} ORDER BY _source_row_number",  # noqa: S608 -- result.staging_table is derived from config/run identity, never row content (staging.py's own threat-model comment)
    ).fetchall()
    return result, [bytes(row[0]) for row in staged]


@pytest.fixture(scope="module", autouse=True)
def _ensure_raw_bucket(
    s3_client: Any,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
) -> None:
    """Ensure ``raw`` exists on the shared session MinIO container -- idempotent."""
    existing = {bucket["Name"] for bucket in s3_client.list_buckets().get("Buckets", [])}
    if _BUCKET not in existing:
        s3_client.create_bucket(Bucket=_BUCKET)


@pytest.fixture
def object_store(
    minio_config: dict[str, str],  # noqa: F811 -- pytest fixture-injection param name
) -> S3ObjectStore:
    """A real ``S3ObjectStore``, built from the same credentials ``s3_client`` uses."""
    return S3ObjectStore(
        endpoint_url=f"http://{minio_config['endpoint']}",
        access_key=minio_config["access_key"],
        secret_key=minio_config["secret_key"],
    )


@pytest.fixture
def conn(
    migrated_dsn: str,  # noqa: F811 -- pytest fixture-injection param name, not a real redefinition
) -> Iterator[psycopg.Connection[Any]]:
    """One open psycopg connection per test, over the migrated database."""
    with psycopg.connect(migrated_dsn) as connection:
        yield connection


@pytest.fixture(scope="module")
def base_config() -> DatasetConfig:
    """The real ``customers.yaml``, loaded once. Module-scoped: frozen and read-only."""
    return load_config(
        REPO_ROOT / "configs" / "datasets" / "customers.yaml",
        defaults_path=REPO_ROOT / "configs" / "defaults.yaml",
    )


@pytest.mark.integration
@settings(
    max_examples=25,
    deadline=None,
    # `conn`/`object_store` are function-scoped pytest fixtures, resolved
    # ONCE per pytest test-function call and then intentionally REUSED
    # across every Hypothesis-internal example within that one call --
    # exactly the pattern HealthCheck.function_scoped_fixture exists to
    # flag. Reuse is safe here: every example stages into its OWN
    # uniquely-named `staging.customers__r<run_id>` table (`_run_ids`, a
    # module-wide counter), so no example's writes can ever be observed by
    # another. The alternative -- a fresh testcontainers-backed connection
    # per internal example -- would multiply this test's own real Docker
    # I/O cost for no correctness benefit.
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(rows=_customers_rows())
def test_identical_input_yields_identical_output_hash(
    rows: tuple[tuple[str, str, str, str, str], ...],
    base_config: DatasetConfig,
    object_store: S3ObjectStore,
    conn: psycopg.Connection[Any],
) -> None:
    """Staging the identical bytes/config/processor-version twice yields the identical hash list.

    ``base_config`` (customers.yaml, unmodified) and ``rows`` (Hypothesis-
    generated, always-valid) never change between the two ``load()`` calls
    below -- only ``run_id`` (and therefore the staging table) differs, the
    same way a real retry or a genuine re-run differs from its predecessor.
    """
    key = _upload(object_store, rows)

    result_1, hashes_1 = _stage_and_hash(
        config=base_config,
        object_store=object_store,
        key=key,
        conn=conn,
    )
    result_2, hashes_2 = _stage_and_hash(
        config=base_config,
        object_store=object_store,
        key=key,
        conn=conn,
    )

    # Non-vacuous: every generated row is valid and non-null by construction
    # (see _customers_rows), so this compares two genuinely populated hash
    # lists -- never two empty ones that would trivially "match".
    assert result_1.rows_rejected == 0
    assert result_2.rows_rejected == 0
    assert len(hashes_1) == len(rows)

    assert hashes_1 == hashes_2


# The direct regression proof for the non-vacuousness claim below: "N/A" is
# NOT customers.yaml's default null token (D-14: empty string only), so it
# reaches DateNormalizer as literal text and fails to parse under
# birth_date's declared "%Y-%m-%d" format -- REJECTED under the unmodified
# config, STAGED (as SQL NULL) once null_sentinels declares it.
_NA_SENTINEL_ROWS: tuple[tuple[str, str, str, str, str], ...] = (
    ("1", "Anna Kowalski", "PL", "1970-05-06", "2026-01-05T08:15:00Z"),
    ("2", "Piotr Nowak", "US", "N/A", "2026-01-19T13:42:11Z"),
)


@pytest.mark.integration
def test_a_genuine_normalization_config_change_yields_a_different_hash_set(
    base_config: DatasetConfig,
    object_store: S3ObjectStore,
    conn: psycopg.Connection[Any],
) -> None:
    """A real ``NormalizationConfig`` difference changes what gets hashed -- never vacuous.

    A single, deterministic example rather than a Hypothesis range:
    unlike the "same config twice" property above, this is an EXISTENCE
    claim ("some genuine config change moves the hash"), not a universal
    one -- ``customers`` has no numeric/currency column for
    ``decimal_separator`` (06-RESEARCH.md's own named example) to exercise,
    so this uses the dataset's one real nullable column instead
    (``null_sentinels``, still a genuine ``NormalizationConfig`` field).
    """
    config_with_na_sentinel = base_config.model_copy(
        update={"normalization": NormalizationConfig(null_sentinels={"birth_date": ["N/A"]})},
    )
    key = _upload(object_store, _NA_SENTINEL_ROWS)

    result_a, hashes_a = _stage_and_hash(
        config=base_config,
        object_store=object_store,
        key=key,
        conn=conn,
    )
    result_b, hashes_b = _stage_and_hash(
        config=config_with_na_sentinel,
        object_store=object_store,
        key=key,
        conn=conn,
    )

    # Config A (unmodified customers.yaml): only the clean row survives --
    # "N/A" matches no declared birth_date sentinel under D-14's platform
    # default, so DateNormalizer rejects it.
    assert result_a.rows_parsed == 1
    assert result_a.rows_rejected == 1
    # Config B (null_sentinels declares "N/A" for birth_date): BOTH rows
    # survive -- "N/A" is now recognized and normalized to SQL NULL before
    # DateNormalizer ever parses it.
    assert result_b.rows_parsed == 2
    assert result_b.rows_rejected == 0

    # The hash SETS genuinely differ: config B produced a hash config A
    # never did (the now-staged "N/A" row) -- a real, deterministic
    # difference, never a coincidental non-match.
    assert set(hashes_b) - set(hashes_a)
    # The clean row's own hash is UNCHANGED by the config difference --
    # confirms this is a targeted, single-row effect, not evidence that
    # _record_hash is simply unstable across any two calls (which would
    # undermine, not support, QUAL-16).
    assert hashes_a[0] in hashes_b
