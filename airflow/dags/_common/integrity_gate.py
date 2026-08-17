"""LOAD-10's pre-pod-launch file-integrity gate (D-18).

A second, narrowly-scoped exception to "the DAG folder never touches
business logic or the analytical database", alongside
``kpo.py``/``tracing_kpo.py``.

Every check named by LOAD-10 that can be resolved BEFORE a
``KubernetesPodOperator`` pod is ever launched lives here, as plain Airflow
``@task`` functions running in the scheduler/worker process: wrong
extension, an empty object, an object that changes between two ``HEAD``
calls five seconds apart (D-21 -- the crude, deliberately simple stand-in
for "is this object still being written"), and an object whose bytes cannot
genuinely be read/hashed (D-22's "checksum" -- there is no external checksum
FILE to compare against per D-22, so "checksum" here means proving the
object is readable at all by computing its real ``content_sha256``). LOAD-11's
optional ``_BATCH_COMPLETE`` control-file check is NOT this module's job.

``meta.files.content_sha256`` is ``NOT NULL`` and part of
``UNIQUE(dataset_id, object_uri, content_sha256)`` (``migrations/versions/
0002_meta_files.py``). Because ``discover_files`` -- the code that normally
creates a ``meta.files`` row -- never runs for a file THIS gate rejects, D-20
requires every rejection path here to write its OWN ``meta.files`` row
anyway, with a real, non-null hash: the one case where the real bytes are
genuinely known (an empty file) gets the real SHA-256 of ``b""``; every case
where the real bytes are unknown or ambiguous (wrong extension -- never
read; an unstable object -- which snapshot?; an unreadable stream) gets a
deterministic ``INTEGRITY_GATE_REJECTED:<object_uri>:<reason>`` sentinel hash
instead, resolved by ``_reject_file`` below. ``_reject_file`` is therefore the
ONE sanctioned exception to ADR-0004's "Airflow never writes to the
analytical database directly" rule -- a narrow, explicit inline ``psycopg``
INSERT, never a ``dataplat`` import; the SQL shape below is duplicated from
``get_or_create_dataset``/``create_file``
(``packages/dataplat/src/dataplat/metadata/postgres.py``), not imported from
it.

``list_matched_keys`` exists because ``S3KeySensor`` pushes no key list to
XCom -- verified directly against the pinned ``apache-airflow-providers-
amazon`` package's installed source (``sensors/s3.py``): ``execute()``
returns ``None`` (both the deferrable and non-deferrable paths), and
``poke()`` returns a plain ``bool``. Neither pushes a matched-key list
anywhere. ``list_matched_keys`` is the concrete, Airflow-native answer: a
thin ``@task`` around ``S3Hook.list_keys(..., apply_wildcard=True)``, listing
the SAME ``bucket``/``prefix`` shape a DAG's own ``wait_for_files`` sensor
already names, so a later plan can ``.expand(key=list_matched_keys(...))``
over ``integrity_gate`` without inventing a key-resolution mechanism at
DAG-authoring time.
"""

from __future__ import annotations

import hashlib
import time

import psycopg
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import task
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.exceptions import AirflowFailException

# Five seconds between the two HEAD checks (D-21): long enough that a
# still-in-flight multi-part PUT's size/ETag has a real chance to have
# changed, short enough not to meaningfully slow discovery for the common
# case (an already-complete object). Config-not-magic-number.
_STABILITY_CHECK_INTERVAL_SECONDS = 5

# Matches Phase 6's supported compression formats (csv_processor's own
# detected-extension set) -- a file this gate would otherwise pass through
# to `discover_files` only to have IT reject on extension is worse than
# rejecting it here, before any pod launches.
_EXPECTED_EXTENSIONS = (".csv", ".csv.gz", ".csv.zip")

# Prefix for the deterministic sentinel hash `_reject_file` computes when the
# real object bytes are unknown or ambiguous (wrong extension, an unstable
# object, an unreadable stream) -- never mistakeable for a real content hash
# at a glance, and namespaced so it can never collide with a real file's
# genuine SHA-256 of its own bytes.
_REJECTION_SENTINEL_PREFIX = b"INTEGRITY_GATE_REJECTED:"

# Mirrors `dataplat.discovery`'s own `_HASH_CHUNK_BYTES` streaming-hash
# convention (duplicated, not imported -- ADR-0004): bounded-memory chunked
# reads, never the whole object loaded into the worker process at once
# (T-08-24).
_HASH_CHUNK_BYTES = 1_048_576

# The Airflow Connection this gate resolves its own DSN through -- an
# Airflow Connection (itself Vault-backed via SEC-05's
# AIRFLOW__SECRETS__BACKEND=VaultBackend wiring, Phase 5), never a literal.
# `common_kpo_kwargs()`'s DATAPLAT_DB_DSN=vault://etl/analytics-db#dsn value
# is resolved INSIDE a pod by `dataplat.secrets.resolver.resolve_secret()` --
# unusable here, since this gate runs in the scheduler/worker process, which
# never imports `dataplat` (ADR-0004). This is this gate's OWN, independent
# DSN resolution path, name-based like `S3KeySensor`'s own
# `aws_conn_id="minio_default"`.
_ANALYTICS_DB_CONN_ID = "analytics_db_default"


@task
def list_matched_keys(bucket: str, prefix: str) -> list[str]:
    """The Airflow-side answer to "which keys currently match".

    `S3KeySensor` itself pushes no XCom key list (verified against the
    pinned provider source, see module docstring), so this task performs
    its own, independent `list_keys` call against the
    SAME `bucket`/`prefix` shape the sensor's own `bucket_key` names. A file
    that disappears between the sensor's poke and this listing is simply
    absent from the returned list -- no error, no special case;
    `integrity_gate.expand(key=...)` runs over however many keys genuinely
    exist at call time.
    """
    hook = S3Hook(aws_conn_id="minio_default")
    return hook.list_keys(bucket_name=bucket, prefix=prefix, apply_wildcard=True)


@task
def integrity_gate(bucket: str, key: str, dataset_name: str) -> dict[str, object]:
    """Every LOAD-10 check this gate covers, cheapest/most-certain first.

    Order matters (and is proven by Task 2's tests): extension (no network
    call at all) -> empty-file (one HEAD) -> stability (two HEADs) ->
    real read+hash (one GET, streamed). Each check short-circuits on the
    first failure -- no wasted network calls once an earlier, cheaper check
    has already failed. Every rejection path calls `_reject_file` (D-20) then
    raises `AirflowFailException` -- a dead end before any pod launches: no
    `run_id`, no `meta.ingestion_runs` row, no `rejected_records` rows.
    """
    if not key.endswith(_EXPECTED_EXTENSIONS):
        reason = f"{key}: extension not in {_EXPECTED_EXTENSIONS}"
        _reject_file(
            bucket=bucket,
            key=key,
            dataset_name=dataset_name,
            reason=reason,
            content_sha256=None,
            size_bytes=None,
        )
        raise AirflowFailException(reason)

    hook = S3Hook(aws_conn_id="minio_default")
    client = hook.get_conn()
    first = client.head_object(Bucket=bucket, Key=key)

    if first["ContentLength"] == 0:
        # The real, exact hash of the object's known-empty content -- no GET
        # needed, `b""` is authoritative here, not a sentinel.
        reason = f"{key}: empty file (0 bytes)"
        _reject_file(
            bucket=bucket,
            key=key,
            dataset_name=dataset_name,
            reason=reason,
            content_sha256=hashlib.sha256(b"").digest(),
            size_bytes=0,
        )
        raise AirflowFailException(reason)

    time.sleep(_STABILITY_CHECK_INTERVAL_SECONDS)
    second = client.head_object(Bucket=bucket, Key=key)

    if (first["ContentLength"], first["ETag"]) != (second["ContentLength"], second["ETag"]):
        # content_sha256=None: ambiguous which snapshot's bytes would even be
        # hashed -- `_reject_file` resolves the sentinel. size_bytes is
        # best-effort/non-authoritative, still informative for diagnosis.
        reason = f"{key}: object not stable between HEAD checks"
        _reject_file(
            bucket=bucket,
            key=key,
            dataset_name=dataset_name,
            reason=reason,
            content_sha256=None,
            size_bytes=first["ContentLength"],
        )
        raise AirflowFailException(reason)

    digest = hashlib.sha256()
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"]
        while True:
            chunk = body.read(_HASH_CHUNK_BYTES)
            if not chunk:
                break
            digest.update(chunk)
    except Exception as exc:
        # D-22: ANY read failure here is a real-world rejection to record,
        # not a bug to propagate as a raw traceback.
        reason = f"{key}: object could not be read/hashed: {exc}"
        _reject_file(
            bucket=bucket,
            key=key,
            dataset_name=dataset_name,
            reason=reason,
            content_sha256=None,
            size_bytes=second["ContentLength"],
        )
        raise AirflowFailException(reason) from exc

    # Success: this gate does NOT itself write a meta.files row here -- that
    # remains discover_files's existing, unchanged job moments later in the
    # DAG; writing here would race/duplicate that INSERT.
    return {
        "content_length": second["ContentLength"],
        "etag": second["ETag"],
        "content_sha256_hex": digest.hexdigest(),
    }


def _reject_file(  # noqa: PLR0913 -- one keyword per genuinely distinct D-20 input; see docstring
    *,
    bucket: str,
    key: str,
    dataset_name: str,
    reason: str,
    content_sha256: bytes | None,
    size_bytes: int | None,
) -> None:
    """D-20: land a real `meta.files` row for EVERY rejection, no exceptions.

    When the caller already knows the real hash (the empty-file case),
    that real hash is written as-is. When the caller passes `None` (the
    real bytes are unknown or ambiguous -- wrong extension, an unstable
    object, an unreadable stream), a deterministic sentinel is computed
    instead: `sha256(INTEGRITY_GATE_REJECTED: + "{bucket}/{key}:{reason}")`.
    Deterministic per `(object_uri, reason)` so a REPEATED identical failure
    on the same object (e.g. the sensor keeps re-matching a still-unstable
    file) idempotently `ON CONFLICT`s onto the SAME row instead of raising a
    duplicate-row error, while a DIFFERENT reason for the same object_uri is
    intentionally a distinct row -- a real change in why the object was
    rejected. `_reject_file` therefore ALWAYS has a non-null hash to write
    and never skips the INSERT.

    The row's `status` column carries the real, human-readable rejection
    reason -- D-20's actual traceability requirement (T-08-25). The sentinel
    hash exists solely to satisfy `meta.files`' `NOT NULL`/`UNIQUE`
    constraint for a case where the real bytes are unknown by definition; it
    is not, and is never presented as, a hash of the object's real content.
    """
    resolved_hash = (
        content_sha256
        if content_sha256 is not None
        else hashlib.sha256(
            _REJECTION_SENTINEL_PREFIX + f"{bucket}/{key}:{reason}".encode()
        ).digest()
    )

    dsn = BaseHook.get_connection(_ANALYTICS_DB_CONN_ID).get_uri()
    object_uri = f"s3://{bucket}/{key}"
    filename = key.rsplit("/", 1)[-1]
    status = f"REJECTED: {reason}"

    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        # Same upsert shape the dataplat metadata repository's own
        # dataset-resolution method uses, duplicated as raw SQL here and
        # never imported from that package (ADR-0004).
        cur.execute(
            """
            INSERT INTO meta.datasets (dataset_name) VALUES (%s)
            ON CONFLICT (dataset_name) DO UPDATE
                SET dataset_name = EXCLUDED.dataset_name
            RETURNING dataset_id
            """,
            (dataset_name,),
        )
        dataset_id_row = cur.fetchone()
        dataset_id = dataset_id_row[0]

        cur.execute(
            """
            INSERT INTO meta.files (
                dataset_id, object_uri, content_sha256, hash_version,
                size_bytes, filename, status
            ) VALUES (%s, %s, %s, 1, %s, %s, %s)
            ON CONFLICT (dataset_id, object_uri, content_sha256) DO UPDATE
                SET status = EXCLUDED.status
            RETURNING file_id
            """,
            (dataset_id, object_uri, resolved_hash, size_bytes or 0, filename, status),
        )
