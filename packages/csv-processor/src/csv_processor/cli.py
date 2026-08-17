"""``discover``/``ingest`` -- the two CLI subcommands a KubernetesPodOperator pod invokes.

This module is ALLOWED to import ``dataplat`` -- the established, permitted
direction (ADR-0002's Decision Outcome; ``setup.cfg``'s import-linter
contract 1 only forbids the OTHER direction, ``dataplat`` importing
``csv_processor``) -- and is exactly why ``discover``/``ingest`` live here,
in ``csv_processor``, rather than inside ``dataplat.cli`` itself: this
module needs ``csv_processor.source.CsvSource``, and ``dataplat`` must never
know that type exists.

Neither command is ever invoked directly by an operator typing
``csv-processor discover``; both attach to the SHARED ``dataplat.cli.cli``
click group via ``@cli.command()`` below, and only ever run because
``dataplat.cli.main()`` loads this module through the ``dataplat.plugins``
entry point declared in ``packages/csv-processor/pyproject.toml`` (see that
function's own docstring for the other half of this design). The pod
``ENTRYPOINT`` stays ``["dataplat"]``; ``docker run <image> dataplat
discover --dataset customers`` and ``docker run <image> dataplat ingest
--assignment <uri>`` are the two real invocations a KPO pod makes.
"""

from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

import click
from pydantic import ValidationError

from csv_processor.source import CsvSource
from dataplat.cli import cli
from dataplat.config.loader import load_config
from dataplat.config.registry import ConfigRegistry
from dataplat.discovery import discover_files
from dataplat.errors import ConfigurationError, DataPlatformError
from dataplat.metadata.postgres import PostgresMetadataRepository
from dataplat.models.assignment import AssignmentDocument
from dataplat.models.identity import RunContext
from dataplat.models.receipt import Receipt
from dataplat.observability.logging import get_logger
from dataplat.pipeline.protocol import PipelineContext
from dataplat.pipeline.run import run_ingest
from dataplat.schema.repository import SchemaRepository
from dataplat.secrets.resolver import resolve_secret
from dataplat.storage.db import create_pool
from dataplat.storage.objectstore import S3ObjectStore
from dataplat.version import resolve_version

if TYPE_CHECKING:
    from psycopg_pool import ConnectionPool

# KPO's well-known XCom sidecar convention (ARCHITECTURE.md Sec 6.4); the
# env-var override exists solely for local/test invocation outside a real
# pod, where /airflow doesn't exist.
_DEFAULT_XCOM_PATH = "/airflow/xcom/return.json"

# The exact four env-var names the DAG's KubernetesPodOperator pod spec sets
# (established across this phase's other plans) -- resolved through
# SecretsResolver's env:// scheme rather than read via os.environ directly,
# so this module never itself names Vault or a Kubernetes Secret (SEC-15,
# D3): whichever mechanism populated the process environment is opaque here.
_DB_DSN_REF = "env://DATAPLAT_DB_DSN"
_S3_ENDPOINT_URL_REF = "env://DATAPLAT_S3_ENDPOINT_URL"
_S3_ACCESS_KEY_REF = "env://DATAPLAT_S3_ACCESS_KEY"
_S3_SECRET_KEY_REF = "env://DATAPLAT_S3_SECRET_KEY"  # noqa: S105 -- an opaque env:// reference, not a secret value


def _build_common() -> tuple[ConnectionPool, PostgresMetadataRepository, S3ObjectStore]:
    """Resolve credentials and build the pool/metadata/objects trio both commands need.

    Three of the four env vars `kpo.py` sets now hold a SECOND opaque
    reference as their own value (plan 05-02: `DATAPLAT_DB_DSN`,
    `DATAPLAT_S3_ACCESS_KEY` and `DATAPLAT_S3_SECRET_KEY` are each a literal
    `vault://...` string, not a directly-usable value) -- resolving them is
    `resolve_secret()` two levels deep: the outer `env://` call reads the
    process environment and hands back that `vault://` string UNRESOLVED
    (by design -- `resolve_secret()` performs exactly one level of
    resolution, never recursively), and the inner call resolves THAT.
    `DATAPLAT_S3_ENDPOINT_URL` is the one exception -- `kpo.py` sets it to a
    plain, non-secret literal directly, so a single `resolve_secret()` call
    is already its final value; a second call would incorrectly try to
    interpret an `http://` URL as a secret-reference scheme and fail closed.

    Bug fixed live, this plan (05-03): the un-double-resolved form failed
    every real KPO pod with `missing "=" after "vault://etl/analytics-db
    #dsn" in connection info string` -- psycopg received the raw,
    unresolved `vault://` reference string as if it were already a DSN.
    This was the first time any KPO pod actually ran against the
    `vault://`-literal `kpo.py` wiring plan 05-02 shipped (the previously-
    deployed image predated it entirely; see this plan's own SUMMARY.md and
    `.planning/phases/05-vault-secrets-workload-identity/deferred-items.md`),
    so the gap was latent, not a regression this plan introduced.

    Returns:
        `(pool, metadata, objects)` -- `pool` is already opened
        (`pool.open(wait=True)`); the caller owns closing it.
    """
    dsn = resolve_secret(resolve_secret(_DB_DSN_REF))
    endpoint_url = resolve_secret(_S3_ENDPOINT_URL_REF)
    access_key = resolve_secret(resolve_secret(_S3_ACCESS_KEY_REF))
    secret_key = resolve_secret(resolve_secret(_S3_SECRET_KEY_REF))

    pool = create_pool(dsn)
    pool.open(wait=True)
    metadata = PostgresMetadataRepository(pool)
    objects = S3ObjectStore(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)
    return pool, metadata, objects


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an `s3://bucket/key` URI into `(bucket, key)`.

    Args:
        uri: The URI to parse.

    Returns:
        `(bucket, key)`.

    Raises:
        ConfigurationError: `uri`'s scheme is not `s3` (README Sec 5:
            applications address data as `s3://bucket/path`, never a
            filesystem path -- enforced here, not merely by convention).
    """
    parsed = urlsplit(uri)
    if parsed.scheme != "s3":
        msg = f"expected an s3:// URI, got {uri!r}"
        raise ConfigurationError(msg, context={"uri": uri})
    return parsed.netloc, parsed.path.lstrip("/")


def _write_xcom(payload: dict[str, object] | Receipt) -> None:
    """Write `payload` as valid JSON to the KPO XCom sidecar path, on every exit path.

    An invalid-JSON XCom file fails the task even when the main container's
    own exit code was 0 (04-RESEARCH.md, verified this phase) -- every call
    site of this helper must produce valid JSON, never a bare string or a
    half-written file.

    Args:
        payload: A `Receipt` (serialized via `model_dump_json()`) or a plain
            JSON-serializable mapping (serialized via `json.dumps()`).
    """
    path = Path(os.environ.get("DATAPLAT_XCOM_PATH", _DEFAULT_XCOM_PATH))
    path.parent.mkdir(parents=True, exist_ok=True)
    text = payload.model_dump_json() if isinstance(payload, Receipt) else json.dumps(payload)
    path.write_text(text, encoding="utf-8")


@cli.command()
@click.option("--dataset", required=True, help="The dataset name, e.g. 'customers'.")
def discover(dataset: str) -> None:
    """Discover newly-arrived files for `dataset` and freeze one assignment per unit.

    Resolves and syncs `dataset`'s config, then calls
    `dataplat.discovery.discover_files`, writing a receipt-shaped summary to
    the XCom path on success. On a `DataPlatformError`, writes a
    `{"status": "FAILED", ...}` payload for forensic `kubectl logs`/`cat`
    inspection before re-raising -- the XCom sidecar only pushes XCom for a
    task reaching `State.SUCCESS`, so Airflow itself never reads this
    particular payload; it exists for a human debugging a failed pod.

    Args:
        dataset: The dataset name, resolved against
            `configs/datasets/<dataset>.yaml`.
    """
    pool: ConnectionPool | None = None
    try:
        pool, metadata, objects = _build_common()
        # SchemaRepository(pool) mirrors `metadata = PostgresMetadataRepository(pool)`
        # just above -- the same `pool` `_build_common()` already opened.
        # Built locally here, not added to `_build_common()`'s own shared
        # return tuple: `ingest()` (the sibling command sharing that helper)
        # has no use for a `SchemaRepository`, so this stays scoped to its
        # one real caller (plan 06-16).
        schema = SchemaRepository(pool)
        config = load_config(
            Path(f"configs/datasets/{dataset}.yaml"),
            defaults_path=Path("configs/defaults.yaml"),
        )
        registry = ConfigRegistry(pool)
        record = registry.sync(dataset, config)
        dataset_id = metadata.get_or_create_dataset(dataset)
        units = discover_files(
            metadata=metadata,
            objects=objects,
            dataset_id=dataset_id,
            dataset_name=dataset,
            config=config,
            config_version_id=record.config_version_id,
            config_hash=record.config_hash,
            processor_image=os.environ.get("DATAPLAT_PROCESSOR_IMAGE", "unknown"),
            processor_version=resolve_version(),
            schema=schema,
        )
        _write_xcom(
            {
                "status": "SUCCEEDED",
                "units": [dataclasses.asdict(unit) for unit in units],
                "file_count": len(units),
            },
        )
    except DataPlatformError as exc:
        _write_xcom(
            {
                "status": "FAILED",
                "error_type": type(exc).__name__,
                "error_message": str(exc),
            },
        )
        raise
    finally:
        if pool is not None:
            pool.close()


def _failure_receipt(doc: AssignmentDocument | None) -> Receipt:
    """Build the `status="FAILED"` `Receipt` `ingest()` writes on any exception.

    Factored out so both of `ingest()`'s exception branches -- the
    `DataPlatformError` branch and the broader `Exception` branch added by
    WR-01 -- construct the identical shape from the identical guard,
    instead of duplicating the `Receipt(...)` call inline in each branch.

    Args:
        doc: The `AssignmentDocument` parsed so far, or `None` when the
            failure happened before `AssignmentDocument.model_validate_json`
            ever succeeded.

    Returns:
        A `Receipt` with `run_id=doc.run_id if doc is not None else -1`,
        `status="FAILED"`, and every count/duration field zeroed.
    """
    return Receipt(
        run_id=doc.run_id if doc is not None else -1,
        status="FAILED",
        rows_read=0,
        rows_loaded=0,
        rows_invalid=0,
        rows_deduplicated=0,
        duration_ms=0,
        rows_quarantined=0,
        report_uri=None,
    )


@cli.command()
@click.option("--assignment", required=True, help="The s3://bucket/key URI of the assignment JSON.")
def ingest(assignment: str) -> None:
    """Ingest the single file named by the assignment document at `assignment`.

    Fetches and validates the `AssignmentDocument` (T-04-02's actual
    enforcement point -- no field is used before
    `AssignmentDocument.model_validate_json` succeeds), resolves the exact
    `DatasetConfig` version it was written against, and delegates the whole
    claim/stage/publish orchestration to `dataplat.pipeline.run.run_ingest`.
    A `Receipt` is written to the XCom path on every exit path, success or
    failure, for ANY exception -- not only `DataPlatformError` (WR-01;
    04-REVIEW.md) -- the `finally`/`except` pairing below is this command's
    own concern, distinct from `run_ingest`'s (which adds no receipt-writing
    boundary of its own; see that function's docstring).

    Args:
        assignment: The `s3://bucket/key` URI of the frozen assignment
            document to process.
    """
    pool: ConnectionPool | None = None
    doc: AssignmentDocument | None = None
    try:
        pool, metadata, objects = _build_common()
        bucket, key = _parse_s3_uri(assignment)
        with objects.get_object(bucket, key) as stream:
            raw_text = stream.read()
        try:
            doc = AssignmentDocument.model_validate_json(raw_text)
        except ValidationError as exc:
            # The assignment document is technically attacker-influenceable
            # (T-04-02) -- never let a raw pydantic.ValidationError escape;
            # re-raise as a DataPlatformError so this command's own
            # except/finally below still writes a receipt.
            msg = f"invalid assignment document at {assignment}"
            raise ConfigurationError(msg, context={"assignment": assignment}) from exc

        config = ConfigRegistry(pool).get_by_id(doc.config_version_id)
        source_bucket, source_key = _parse_s3_uri(doc.file.object_uri)
        # CSV-11 multipart (plan 06-18): every remaining part of a
        # multipart-grouped delivery, in delivery order -- empty for an
        # ordinary single-file assignment (`doc.additional_parts`'s own
        # additive default), so `CsvSource`'s single-part path stays
        # completely unaffected for every dataset with no
        # `multipart_pattern` configured. Same `_parse_s3_uri` split
        # `source_bucket`/`source_key` above already used; only the key is
        # needed -- every part shares `doc.file.object_uri`'s bucket by
        # construction (`dataplat.discovery`'s own per-group assembly).
        additional_keys = tuple(
            _parse_s3_uri(part.object_uri)[1] for part in doc.additional_parts
        )
        # `get_or_create_dataset` is idempotent (`discover()`'s own prior
        # call already created this row -- `discover_files` requires a real
        # `dataset_id` to run at all) -- this is a read in practice, never a
        # first insert. Threading a real `dataset_id` into `CsvSource` here
        # is what actually fires plan 06-15's schema-sync wiring in
        # production: `dataplat.discovery`'s own idempotency-key formula
        # (`discovery.py`'s module docstring) already reads
        # `SchemaRepository.get_current(dataset_id)` in anticipation of this
        # exact call site populating `meta.schema_versions` for real.
        dataset_id = metadata.get_or_create_dataset(doc.dataset)
        # OBS-07 gap closure (07-09): the 5 AIRFLOW_CTX_* env vars
        # TracingKubernetesPodOperator.build_pod_request_obj() injects into
        # this pod (airflow/dags/_common/tracing_kpo.py), read back the same
        # way AIRFLOW_TASK_TRY_NUMBER already is above -- `.get()`'s own
        # `None` default is already RunContext's correct fallback outside
        # Airflow. map_index has no sensible string default the way attempt
        # has "1", so it is parsed separately below.
        _raw_map_index = os.environ.get("AIRFLOW_CTX_MAP_INDEX")
        run = RunContext(
            run_id=doc.run_id,
            idempotency_key=doc.idempotency_key,
            attempt=int(os.environ.get("AIRFLOW_TASK_TRY_NUMBER", "1")),
            file_id=doc.file.file_id,
            batch_id=doc.batch.batch_id,
            dag_id=os.environ.get("AIRFLOW_CTX_DAG_ID"),
            dag_run_id=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),
            task_id=os.environ.get("AIRFLOW_CTX_TASK_ID"),
            map_index=int(_raw_map_index) if _raw_map_index is not None else None,
            k8s_namespace=os.environ.get("AIRFLOW_CTX_K8S_NAMESPACE"),
        )
        ctx = PipelineContext(
            run=run,
            config=config,
            metadata=metadata,
            objects=objects,
            db=pool,
            log=get_logger(),
            source=CsvSource(
                bucket=source_bucket,
                key=source_key,
                dataset_id=dataset_id,
                additional_keys=additional_keys,
            ),
        )
        heartbeat_interval_seconds = float(
            os.environ.get("DATAPLAT_HEARTBEAT_INTERVAL_SECONDS", "60.0"),
        )
        receipt = run_ingest(ctx, heartbeat_interval_seconds=heartbeat_interval_seconds)
        _write_xcom(receipt)
    except DataPlatformError:
        _write_xcom(_failure_receipt(doc))
        raise
    except Exception:
        # WR-01: deliberate, narrow, always-re-raising catch that exists
        # solely to guarantee ingest()'s own docstring-promised
        # Receipt-on-every-exit-path contract for exceptions outside the
        # DataPlatformError hierarchy (e.g. a raw psycopg.errors.DataError,
        # a network error, MemoryError). Airflow still observes the pod's
        # non-zero exit code either way; this only ensures a Receipt is
        # written before that propagation. Because the DataPlatformError
        # branch above is listed first, DataPlatformError instances are
        # still only ever caught there -- this clause only ever sees what
        # that one did not match, per normal Python except-clause ordering,
        # and never intercepts BaseException-only families like
        # KeyboardInterrupt/SystemExit. No blind-except lint suppression is
        # needed here: ruff's BLE001 check does not fire on a branch that
        # always re-raises rather than swallowing the exception.
        _write_xcom(_failure_receipt(doc))
        raise
    finally:
        if pool is not None:
            pool.close()
