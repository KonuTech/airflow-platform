"""``discover_files`` -- the frozen-manifest-authoring function every discovery run executes.

The ORCH-08 mechanism made concrete: list once, hash once, freeze once,
never re-derive from a live listing mid-run. This is the one place file
identity, content-hash-based deduplication (LOAD-03/D-13), the
idempotency-key formula (ARCHITECTURE.md Q7) and the Dynamic-Task-Mapping
fan-out cap (ORCH-03) all meet.

Idempotency key (ARCHITECTURE.md Q7, verbatim, `.planning/research/
ARCHITECTURE.md` lines 790-798)::

    idempotency_key = sha256(
        dataset_name | file.content_sha256 | config_hash |
        schema_version | processor_image_digest | target_partition | policy_digest
    )

``try_number`` and ``dag_run_id`` are DELIBERATELY ABSENT -- an Airflow
retry must produce the identical key. This phase's adaptation
(04-03-PLAN.md Interfaces): ``schema_version_id``/``target_partition``/
``policy_digest`` have no populated value yet -- no schema-versioning
concept exists until Phase 6, no partitioning or quarantine-policy concept
exists until Phase 6/8 -- so this module computes ``idempotency_key =
sha256(f"{dataset_name}|{content_sha256_hex}|{config_hash}|
{processor_image}").hexdigest()``. A later phase EXTENDS this formula by
appending the missing terms once they have real values; it does not
replace it.

``meta.files.business_date`` is never populated here, and this module reads
no wall-clock time and no Airflow scheduling-interval value anywhere
(README §67 determinism) -- this plan's own acceptance criteria greps this
file's source for the disallowed call/attribute patterns and requires zero
matches.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dataplat.errors import FileInspectionError
from dataplat.models.assignment import AssignmentDocument, BatchAssignment, FileAssignment
from dataplat.observability.logging import get_logger

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence
    from typing import TextIO

    from dataplat.config.model import DatasetConfig
    from dataplat.metadata.repository import MetadataRepository
    from dataplat.storage.objectstore import ObjectStore, ObjectSummary

# Bumping this constant is the only sanctioned way to signal a change to the
# content-hashing recipe below (mirrors dataplat.config.hashing.
# CONFIG_HASH_VERSION's precedent) -- every meta.files.hash_version value
# this module writes traces back to this constant.
_FILE_HASH_VERSION = 1

# Bytes read per streaming-hash chunk -- bounded memory (INCR-08/README
# §39), the same "never load the whole file into memory" discipline
# csv_processor.source's chunked reader established, adapted here: this
# function hashes raw object bytes, not CSV records, so the chunk unit is
# bytes, not a row count.
_HASH_CHUNK_BYTES = 1_048_576


@dataclass(frozen=True, slots=True)
class DiscoveredUnit:
    """One discovery-time unit ready for Dynamic Task Mapping to fan out.

    ~200 bytes per entry -- the shape a later plan's DAG task turns
    directly into ``KubernetesPodOperator.expand(arguments=...)``; the pod
    itself re-reads the full ``AssignmentDocument`` from ``assignment_uri``,
    never carrying more than this through Airflow's XCom.

    Attributes:
        assignment_uri: Object-store URI of the frozen ``AssignmentDocument``
            this unit points to.
        idempotency_key: The run's idempotency key, duplicated here so a
            caller can log/correlate without re-reading the assignment
            document.
        run_id: The ``meta.ingestion_runs.run_id`` this unit claims.
    """

    assignment_uri: str
    idempotency_key: str
    run_id: int


def discover_files(  # noqa: PLR0913 -- one keyword per genuinely distinct input; see module docstring
    *,
    metadata: MetadataRepository,
    objects: ObjectStore,
    dataset_id: int,
    dataset_name: str,
    config: DatasetConfig,
    config_version_id: int,
    config_hash: str,
    processor_image: str,
    processor_version: str,
) -> list[DiscoveredUnit]:
    """List, hash, dedup-check, freeze and cap one dataset's discoverable files.

    Sequence, once per call: (1) list every object under
    ``config.source.bucket``/``config.source.path``, sorted by key -- so
    the same inputs always produce the same manifest (ORCH-08's
    frozen-manifest requirement extends to determinism, not merely to
    "frozen after writing"). (2) For each object, stream its raw bytes
    through ``hashlib.sha256()`` in bounded chunks -- the object is never
    loaded whole into memory. (3) Check ``meta.files`` for a content-hash
    duplicate under a DIFFERENT ``object_uri`` (D-13); when found and
    ``config.source.duplicate_policy == "skip"``, record the file with
    ``duplicate_of_file_id`` set and stop -- no batch, no run, no
    assignment document for it. (4) Otherwise, create a one-file batch
    (this phase's documented one-file-one-batch simplification),
    pre-allocate an ingestion run (idempotent -- a run already
    ``SUCCEEDED`` is skipped, everything else is re-offered), and freeze
    an ``AssignmentDocument`` to
    ``s3://metadata/assignments/<dataset_name>/<run_id>.json``. (5) Cap the
    result at ``config.batching.max_units_per_run``, deterministically
    (never by listing order, which S3-compatible stores do not guarantee
    stable across pages) -- files beyond the cap are left exactly as
    upserted in ``meta.files``/``meta.ingestion_runs`` and are picked up by
    the very next ``discover_files`` call, never lost.

    ``meta.files.business_date`` is never populated here -- this function
    reads no wall-clock time and no Airflow scheduling-interval value
    anywhere (see the module docstring).

    Idempotency key: see the module docstring for the full formula and its
    ARCHITECTURE.md Q7 provenance.

    Args:
        metadata: Typed CRUD surface over ``meta.datasets``/``meta.files``/
            ``meta.batches``/``meta.batch_files``/``meta.ingestion_runs``.
        objects: Object-store list/read/write surface.
        dataset_id: The dataset's ``meta.datasets.dataset_id``.
        dataset_name: The dataset's unique name.
        config: The dataset's already-validated, resolved configuration.
        config_version_id: The ``meta.config_versions`` row this discovery
            run is configured by.
        config_hash: That config version's canonical-JSON sha256 hash.
        processor_image: The container image digest that will execute the
            resulting runs.
        processor_version: The ``dataplat`` distribution version
            discovering these files.

    Returns:
        Up to ``config.batching.max_units_per_run`` ``DiscoveredUnit``
        objects, deterministically ordered, ready for Dynamic Task
        Mapping. Files already ``SUCCEEDED`` or marked duplicate are
        excluded.
    """
    log = get_logger()
    listed = sorted(
        objects.list_objects(config.source.bucket, config.source.path),
        key=lambda summary: summary.key,
    )

    candidates: list[DiscoveredUnit] = []
    for obj in listed:
        object_uri = f"s3://{config.source.bucket}/{obj.key}"
        filename = obj.key.rsplit("/", 1)[-1]

        # Raw-bytes hash via TextIOWrapper.buffer (its own public, documented
        # binary-buffer attribute -- NOT StreamingBody's forbidden private
        # internal state; see storage/objectstore.py's module docstring).
        # Hashing genuine bytes, rather than decoding then re-encoding text,
        # keeps content_sha256 correct independent of any encoding
        # assumption, even though CONTEXT.md D-01 hardcodes UTF-8 for this
        # phase's own parsing path.
        digest = hashlib.sha256()
        with objects.get_object(config.source.bucket, obj.key) as stream:
            while True:
                byte_chunk = stream.buffer.read(_HASH_CHUNK_BYTES)
                if not byte_chunk:
                    break
                digest.update(byte_chunk)
        content_sha256 = digest.digest()
        content_sha256_hex = digest.hexdigest()

        existing_file_id = metadata.find_file_by_content_hash(
            dataset_id=dataset_id,
            content_sha256=content_sha256,
        )
        is_duplicate = existing_file_id is not None and config.source.duplicate_policy == "skip"
        duplicate_of_file_id = existing_file_id if is_duplicate else None

        file_id = metadata.create_file(
            dataset_id=dataset_id,
            object_uri=object_uri,
            content_sha256=content_sha256,
            hash_version=_FILE_HASH_VERSION,
            size_bytes=obj.size_bytes,
            filename=filename,
            status="DISCOVERED",
            duplicate_of_file_id=duplicate_of_file_id,
        )

        # find_file_by_content_hash cannot distinguish "a DIFFERENT
        # object_uri already holds this content" (a genuine D-13 duplicate)
        # from "THIS object_uri was already discovered in a prior
        # discover_files call" (a rediscovery of the same file) -- both
        # return the same content-hash match. If the row create_file just
        # upserted IS the row find_file_by_content_hash found, this is a
        # rediscovery, not a duplicate: correct the wrongly self-
        # referential duplicate_of_file_id back to None with a follow-up
        # idempotent call, so re-running discovery never leaves a file
        # permanently marked as a duplicate of itself.
        if duplicate_of_file_id is not None and file_id == existing_file_id:
            duplicate_of_file_id = None
            file_id = metadata.create_file(
                dataset_id=dataset_id,
                object_uri=object_uri,
                content_sha256=content_sha256,
                hash_version=_FILE_HASH_VERSION,
                size_bytes=obj.size_bytes,
                filename=filename,
                status="DISCOVERED",
                duplicate_of_file_id=None,
            )

        if duplicate_of_file_id is not None:
            log.info(
                "discovery.object_evaluated",
                dataset=dataset_name,
                object_uri=object_uri,
                content_sha256_prefix=content_sha256_hex[:12],
                decision="DUPLICATE",
            )
            continue  # D-13 skip policy: no batch, no run, no assignment document

        batch_key = f"{dataset_name}:{content_sha256_hex[:16]}"
        # get_or_create_batch, NEVER create_batch: batch_key is a pure
        # function of content_sha256, so a rerun over an unchanged object
        # reaches this line with the SAME batch_key every time (this is a
        # rediscovery, not a genuinely new batch -- the same reasoning as
        # create_file's own idempotent upsert above). create_batch's plain
        # INSERT would raise UniqueViolation against
        # uq_batches_dataset_batch_key on exactly this rerun path.
        batch_id = metadata.get_or_create_batch(
            dataset_id=dataset_id,
            batch_key=batch_key,
            status="OPEN",
        )
        # One-file-one-batch: this phase's documented simplification
        # (03-RESEARCH.md) -- sequence_no is always 1. link_batch_file is
        # itself idempotent (ON CONFLICT DO NOTHING on its composite PK),
        # so re-linking the same (batch_id, file_id) pair on a rerun is
        # harmless.
        metadata.link_batch_file(batch_id=batch_id, file_id=file_id, sequence_no=1)

        idempotency_key = hashlib.sha256(
            f"{dataset_name}|{content_sha256_hex}|{config_hash}|{processor_image}".encode(),
        ).hexdigest()

        run_id, status = metadata.get_or_create_ingestion_run(
            idempotency_key=idempotency_key,
            dataset_id=dataset_id,
            file_id=file_id,
            batch_id=batch_id,
            config_version_id=config_version_id,
            processor_version=processor_version,
            processor_image_digest=processor_image,
        )

        if status == "SUCCEEDED":
            log.info(
                "discovery.object_evaluated",
                dataset=dataset_name,
                object_uri=object_uri,
                content_sha256_prefix=content_sha256_hex[:12],
                decision="ALREADY_SUCCEEDED",
            )
            continue

        assignment = AssignmentDocument(
            assignment_version=1,
            run_id=run_id,
            idempotency_key=idempotency_key,
            dataset=dataset_name,
            config_version_id=config_version_id,
            config_hash=config_hash,
            file=FileAssignment(
                file_id=file_id,
                object_uri=object_uri,
                content_sha256=content_sha256_hex,
                size_bytes=obj.size_bytes,
            ),
            batch=BatchAssignment(batch_key=batch_key, batch_id=batch_id),
        )
        assignment_key = f"assignments/{dataset_name}/{run_id}.json"
        objects.put_object(
            bucket="metadata",
            key=assignment_key,
            body=assignment.model_dump_json().encode("utf-8"),
        )

        log.info(
            "discovery.object_evaluated",
            dataset=dataset_name,
            object_uri=object_uri,
            content_sha256_prefix=content_sha256_hex[:12],
            decision="NEW",
        )
        candidates.append(
            DiscoveredUnit(
                assignment_uri=f"s3://metadata/{assignment_key}",
                idempotency_key=idempotency_key,
                run_id=run_id,
            ),
        )

    if len(candidates) > config.batching.max_units_per_run:
        excess = len(candidates) - config.batching.max_units_per_run
        log.warning(
            "discovery.units_capped",
            dataset=dataset_name,
            excess=excess,
            cap=config.batching.max_units_per_run,
        )
        candidates = candidates[: config.batching.max_units_per_run]

    return candidates


@dataclass(frozen=True, slots=True)
class MultipartGroup:
    r"""One multi-part delivery's objects, grouped and ordered for reassembly (CSV-11).

    Attributes:
        group_key: The shared identity extracted from
            ``SourceConfig.multipart_pattern``'s named ``group`` capture --
            every object sharing this key belongs to the same logical
            dataset.
        ordered_object_uris: Member keys, ordered by
            ``multipart_pattern``'s named ``index`` capture, numeric
            ascending -- ``ordered_object_uris[0]`` is the part carrying the
            header, per this platform's multi-part convention (see
            ``open_multipart_stream``'s docstring). These are object KEYS
            relative to their bucket, not bucket-qualified ``s3://`` URIs --
            ``group_multipart_units`` receives no bucket argument; a caller
            wiring this into ``discover_files`` (a later plan -- 06-08-PLAN.md's
            own ``<verification>`` scope note) is responsible for that
            qualification.
    """

    group_key: str
    ordered_object_uris: tuple[str, ...]


def group_multipart_units(listed: Sequence[ObjectSummary], pattern: str) -> list[MultipartGroup]:
    r"""Partition ``listed`` objects into multi-part delivery groups (CSV-11).

    Matches each object's key against ``pattern`` with a whole-string anchor
    (``re.fullmatch`` -- mirrors CSV-01's filename-mask whole-string-anchor
    discipline: a key matching only a fragment of ``pattern`` is not a
    genuine multi-part member, not a partial match to accept). An object
    whose key does not match ``pattern`` at all is not part of any group and
    is silently excluded from the returned list -- this function only
    identifies and orders multi-part groups among objects that match; it
    does not validate that every listed object belongs to one.

    Args:
        listed: Every object under consideration (typically one
            ``ObjectStore.list_objects`` page/listing).
        pattern: A regex with two required named capture groups -- ``group``
            (the shared identity) and ``index`` (the numeric part ordinal),
            e.g. ``r"(?P<group>.+)/part-(?P<index>\d+)"``
            (``SourceConfig.multipart_pattern``).

    Returns:
        One ``MultipartGroup`` per distinct ``group`` capture found, ordered
        by ``group_key`` for determinism, each with its members ordered by
        ``index`` ascending.

    Raises:
        FileInspectionError: A group's ``index`` captures have a gap (e.g.
            ``part-00000``/``part-00002`` present, ``part-00001`` missing)
            (``diagnostic_code="multipart-group-incomplete"``) -- never
            silently skip a missing part.
    """
    compiled = re.compile(pattern)
    members_by_group: dict[str, list[tuple[int, str]]] = {}
    for obj in listed:
        match = compiled.fullmatch(obj.key)
        if match is None:
            continue  # not a multipart member -- this function groups, never validates coverage
        group_key = match.group("group")
        index = int(match.group("index"))
        members_by_group.setdefault(group_key, []).append((index, obj.key))

    groups: list[MultipartGroup] = []
    for group_key in sorted(members_by_group):
        members = sorted(members_by_group[group_key])
        indices = [index for index, _ in members]
        expected = list(range(indices[0], indices[0] + len(indices)))
        if indices != expected:
            msg = f"multipart group {group_key!r} has a gap in its part indices: {indices}"
            raise FileInspectionError(
                msg,
                context={
                    "diagnostic_code": "multipart-group-incomplete",
                    "group_key": group_key,
                    "indices": indices,
                },
            )
        groups.append(
            MultipartGroup(
                group_key=group_key,
                ordered_object_uris=tuple(key for _, key in members),
            ),
        )
    return groups


def open_multipart_stream(streams: Sequence[TextIO]) -> Iterator[str]:
    """Concatenate several already-open text streams into one logical physical-line iterator.

    Yields every physical line from every stream, in stream order. This
    platform's multi-part convention puts the header in the FIRST stream
    ONLY -- verified this session by generating corpus fixture
    ``62_multipart_split`` and reading its two real parts directly:
    ``part-00000`` carries the header plus its share of data rows,
    ``part-00001`` carries pure data, no header line at all
    (``tools/corpus/generators.py::_write_multipart`` writes
    ``header + body`` to part 0 and only ``body`` to every later part).
    Nothing is skipped from a later stream: skipping its first physical
    line would silently drop a genuine data row -- exactly the failure
    fixture ``62_multipart_split``'s own corpus comment names ("part-00001's
    first row is DATA... consuming it as a header silently drops a
    record").

    Args:
        streams: Already-open text streams, in part order (``streams[0]``
            must be the part carrying the header). Ownership (closing them)
            stays with the caller.

    Yields:
        Every physical line from every stream, in order -- the first line
        of ``streams[0]`` is the only header line in the whole sequence.
    """
    for stream in streams:
        yield from stream
