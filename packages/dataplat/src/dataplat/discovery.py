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
``policy_digest`` had no populated value at first -- no schema-versioning
concept existed until Phase 6, no partitioning or quarantine-policy concept
exists until Phase 6/8 -- so this module originally computed
``idempotency_key = sha256(f"{dataset_name}|{content_sha256_hex}|
{config_hash}|{processor_image}").hexdigest()``. Plan 06-16 EXTENDS this
formula (append-only -- the first four terms are never reordered or
replaced) by appending a real ``schema_version`` term, resolved once per
call via ``dataplat.schema.repository.SchemaRepository.get_current()``:
``idempotency_key = sha256(f"{dataset_name}|{content_sha256_hex}|
{config_hash}|{processor_image}|{schema_version}").hexdigest()``. A
dataset with no current schema version yet (its very first discovery run,
before plan 06-15's schema-sync wiring has ever run for it) contributes an
empty string for this term rather than blocking discovery -- ``target_partition``/
``policy_digest`` remain unpopulated, still future-phase territory this
plan does not touch.

``meta.files.business_date`` is never populated here, and this module reads
no wall-clock time and no Airflow scheduling-interval value anywhere
(README §67 determinism) -- this plan's own acceptance criteria greps this
file's source for the disallowed call/attribute patterns and requires zero
matches. Plan 06-16 adds an opt-in ``filename_facets_by_object`` parameter
(D-11) so a caller that HAS already extracted filename facets (mask-derived,
never wall-clock-derived) can hand this function a per-object
``business_date`` facet for observability -- this is a signature addition
only: no dataset declares a filename mask yet (D-10), so no live caller
populates it this phase, and ``meta.files.business_date`` still has no
write path here at all (``MetadataRepository.create_file`` accepts no such
column) -- see that parameter's own docstring for the full scope boundary.
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
    from collections.abc import Iterator, Mapping, Sequence
    from typing import TextIO

    from structlog.typing import FilteringBoundLogger

    from dataplat.config.model import DatasetConfig
    from dataplat.metadata.repository import MetadataRepository
    from dataplat.schema.repository import SchemaRepository
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


@dataclass(frozen=True, slots=True)
class _PartRegistration:
    """One multipart-group member's post-registration identity (this plan, 06-18).

    Purely local bookkeeping for ``discover_files``'s own per-group loop --
    never leaves this module. Carries exactly what building that group's
    ``FileAssignment``/``link_batch_file`` calls needs, once per part, so the
    per-part registration step (``_hash_and_register_file``) and the
    per-group assembly step that follows it stay decoupled.

    Attributes:
        key: The part's object key, relative to ``config.source.bucket``
            (``MultipartGroup.ordered_object_uris``'s own convention).
        file_id: The part's registered ``meta.files.file_id``.
        content_sha256_hex: The part's own content hash, lowercase
            hex-encoded.
        size_bytes: The part's size, in bytes.
    """

    key: str
    file_id: int
    content_sha256_hex: str
    size_bytes: int


def _hash_and_register_file(
    *,
    metadata: MetadataRepository,
    objects: ObjectStore,
    dataset_id: int,
    config: DatasetConfig,
    obj: ObjectSummary,
) -> tuple[int, str, bytes, bool]:
    """Hash one listed object's raw bytes and idempotently register it in ``meta.files``.

    Extracted, as a pure refactor with NO behavior change (this plan,
    06-18), from what used to be ``discover_files``'s own per-object loop
    body -- every part of a CSV-11 multipart group needs this SAME
    per-object lineage (its own ``meta.files`` row, its own content-hash
    dedup check against ``find_file_by_content_hash``, its own
    rediscovery-vs-duplicate self-correction), for the identical reasons the
    ungrouped per-object path already has one. Both ``discover_files``'s
    ungrouped per-object loop and its per-group loop call this function once
    per object.

    Args:
        metadata: Typed CRUD surface over ``meta.files``.
        objects: Object-store read surface, for the raw-bytes streaming
            hash.
        dataset_id: The owning dataset's ``meta.datasets.dataset_id``.
        config: The dataset's already-validated, resolved configuration.
        obj: The listed object to hash and register.

    Returns:
        ``(file_id, content_sha256_hex, content_sha256, is_duplicate)`` --
        ``is_duplicate`` reflects the FINAL determination, after this
        function's own rediscovery-self-correction (re-registering an
        object_uri already known from a prior ``discover_files`` call is
        never mistaken for a genuine D-13 content duplicate).
    """
    object_uri = f"s3://{config.source.bucket}/{obj.key}"
    filename = obj.key.rsplit("/", 1)[-1]

    # Raw-bytes hash via TextIOWrapper.buffer (its own public, documented
    # binary-buffer attribute -- NOT StreamingBody's forbidden private
    # internal state; see storage/objectstore.py's module docstring).
    # Hashing genuine bytes, rather than decoding then re-encoding text,
    # keeps content_sha256 correct independent of any encoding assumption,
    # even though CONTEXT.md D-01 hardcodes UTF-8 for this phase's own
    # parsing path.
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

    # find_file_by_content_hash cannot distinguish "a DIFFERENT object_uri
    # already holds this content" (a genuine D-13 duplicate) from "THIS
    # object_uri was already discovered in a prior discover_files call" (a
    # rediscovery of the same file) -- both return the same content-hash
    # match. If the row create_file just upserted IS the row
    # find_file_by_content_hash found, this is a rediscovery, not a
    # duplicate: correct the wrongly self-referential duplicate_of_file_id
    # back to None with a follow-up idempotent call, so re-running discovery
    # never leaves a file permanently marked as a duplicate of itself.
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

    return file_id, content_sha256_hex, content_sha256, duplicate_of_file_id is not None


def _process_multipart_group(  # noqa: PLR0913 -- one keyword per genuinely distinct input; see discover_files
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
    schema_version_term: str,
    group: MultipartGroup,
    objects_by_key: Mapping[str, ObjectSummary],
    log: FilteringBoundLogger,
) -> DiscoveredUnit | None:
    """Discover one CSV-11 multipart group: hash/register every part, dedup-check, batch+run.

    Extracted from `discover_files`'s own per-group loop body (this plan,
    06-18) purely to keep that function's cyclomatic complexity within this
    codebase's lint gate (ruff C901/PLR0912/PLR0915) -- NO behavior change
    from before this extraction. `discover_files`'s per-group loop calls
    this function once per `MultipartGroup` its own `group_multipart_units`
    call produced.

    Args:
        metadata: Typed CRUD surface over `meta.files`/`meta.batches`/
            `meta.batch_files`/`meta.ingestion_runs`.
        objects: Object-store read/write surface.
        dataset_id: The owning dataset's `meta.datasets.dataset_id`.
        dataset_name: The dataset's unique name.
        config: The dataset's already-validated, resolved configuration.
        config_version_id: The `meta.config_versions` row this run is
            configured by.
        config_hash: That config version's canonical-JSON sha256 hash.
        processor_image: The container image digest that will execute the
            resulting run.
        processor_version: The `dataplat` distribution version discovering
            this group.
        schema_version_term: The idempotency-key schema-version term
            (Pitfall 5), resolved once per `discover_files` call.
        group: The multipart group to discover.
        objects_by_key: Every listed object, keyed by its object key --
            resolves `group.ordered_object_uris` back to their
            `ObjectSummary`.
        log: The structured logger `discover_files` itself already holds.

    Returns:
        A new `DiscoveredUnit` for this group, or `None` when ANY part was a
        content duplicate (D-13, T-06-33 -- the WHOLE group is withheld this
        call) or the group's run already `SUCCEEDED`.
    """
    parts: list[_PartRegistration] = []
    any_duplicate = False
    for key in group.ordered_object_uris:
        part_obj = objects_by_key[key]
        (
            part_file_id,
            part_content_sha256_hex,
            _part_content_sha256,
            part_is_duplicate,
        ) = _hash_and_register_file(
            metadata=metadata,
            objects=objects,
            dataset_id=dataset_id,
            config=config,
            obj=part_obj,
        )
        any_duplicate = any_duplicate or part_is_duplicate
        parts.append(
            _PartRegistration(
                key=key,
                file_id=part_file_id,
                content_sha256_hex=part_content_sha256_hex,
                size_bytes=part_obj.size_bytes,
            ),
        )

    if any_duplicate:
        # T-06-33 (this plan's own threat register): a group-level skip, not
        # a silent one -- every part above was already idempotently
        # recorded in meta.files regardless (_hash_and_register_file's own
        # create_file upsert), so a fresh non-duplicate replacement part
        # resolves this group on the very next discover_files call. No
        # batch, no run, no assignment document for ANY part this call.
        log.info(
            "discovery.object_evaluated",
            dataset=dataset_name,
            decision="DUPLICATE_GROUP",
            group_key=group.group_key,
        )
        return None

    # Order-sensitive (ordered_object_uris is already numerically
    # part-ordered) and deterministic, mirroring the ungrouped path's own
    # single-file content_sha256_hex -- a rerun over an unchanged part-set
    # reaches the identical group hash, and therefore the identical
    # batch_key/idempotency_key below.
    group_content_sha256_hex = hashlib.sha256(
        "|".join(part.content_sha256_hex for part in parts).encode(),
    ).hexdigest()

    batch_key = f"{dataset_name}:{group_content_sha256_hex[:16]}"
    # get_or_create_batch, NEVER create_batch -- see _hash_and_register_file's
    # own sibling reasoning; batch_key is a pure function of the group's
    # combined content.
    batch_id = metadata.get_or_create_batch(
        dataset_id=dataset_id,
        batch_key=batch_key,
        status="OPEN",
    )
    # One batch, every part linked at its own 1-based delivery position --
    # link_batch_file's own docstring already generalizes sequence_no for
    # exactly this multi-part case.
    for sequence_no, part in enumerate(parts, start=1):
        metadata.link_batch_file(
            batch_id=batch_id,
            file_id=part.file_id,
            sequence_no=sequence_no,
        )

    # Pitfall 5: APPENDS the schema-version term -- never reorders or
    # replaces the first four (see this module's own docstring). Substitutes
    # ONLY the content-hash term with the group's own order-sensitive
    # combined hash.
    idempotency_key = hashlib.sha256(
        f"{dataset_name}|{group_content_sha256_hex}|{config_hash}|{processor_image}|"
        f"{schema_version_term}".encode(),
    ).hexdigest()

    run_id, status = metadata.get_or_create_ingestion_run(
        idempotency_key=idempotency_key,
        dataset_id=dataset_id,
        file_id=None,  # no single file identifies a multi-file run
        batch_id=batch_id,
        config_version_id=config_version_id,
        processor_version=processor_version,
        processor_image_digest=processor_image,
    )

    if status == "SUCCEEDED":
        log.info(
            "discovery.object_evaluated",
            dataset=dataset_name,
            decision="ALREADY_SUCCEEDED",
            group_key=group.group_key,
        )
        return None

    first_part, *rest_parts = parts
    assignment = AssignmentDocument(
        assignment_version=1,
        run_id=run_id,
        idempotency_key=idempotency_key,
        dataset=dataset_name,
        config_version_id=config_version_id,
        config_hash=config_hash,
        file=FileAssignment(
            file_id=first_part.file_id,
            object_uri=f"s3://{config.source.bucket}/{first_part.key}",
            content_sha256=first_part.content_sha256_hex,
            size_bytes=first_part.size_bytes,
        ),
        additional_parts=tuple(
            FileAssignment(
                file_id=part.file_id,
                object_uri=f"s3://{config.source.bucket}/{part.key}",
                content_sha256=part.content_sha256_hex,
                size_bytes=part.size_bytes,
            )
            for part in rest_parts
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
        decision="NEW_GROUP",
        group_key=group.group_key,
    )
    return DiscoveredUnit(
        assignment_uri=f"s3://metadata/{assignment_key}",
        idempotency_key=idempotency_key,
        run_id=run_id,
    )


def _process_ungrouped_object(  # noqa: PLR0913 -- one keyword per genuinely distinct input; see discover_files
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
    schema_version_term: str,
    obj: ObjectSummary,
    filename_facets_by_object: Mapping[str, Mapping[str, object]] | None,
    log: FilteringBoundLogger,
) -> DiscoveredUnit | None:
    """Discover one ungrouped object: hash/register, dedup-check, batch+run, freeze an assignment.

    Extracted from `discover_files`'s own per-object loop body (this plan,
    06-18) purely to keep that function's cyclomatic complexity within this
    codebase's lint gate (ruff C901/PLR0912/PLR0915) once
    `_process_multipart_group` was added alongside it -- NO behavior change
    from before this extraction.

    Args:
        metadata: Typed CRUD surface over `meta.files`/`meta.batches`/
            `meta.batch_files`/`meta.ingestion_runs`.
        objects: Object-store read/write surface.
        dataset_id: The owning dataset's `meta.datasets.dataset_id`.
        dataset_name: The dataset's unique name.
        config: The dataset's already-validated, resolved configuration.
        config_version_id: The `meta.config_versions` row this run is
            configured by.
        config_hash: That config version's canonical-JSON sha256 hash.
        processor_image: The container image digest that will execute the
            resulting run.
        processor_version: The `dataplat` distribution version discovering
            this object.
        schema_version_term: The idempotency-key schema-version term
            (Pitfall 5), resolved once per `discover_files` call.
        obj: The ungrouped object to discover.
        filename_facets_by_object: `discover_files`'s own parameter of the
            same name (D-11), passed straight through.
        log: The structured logger `discover_files` itself already holds.

    Returns:
        A new `DiscoveredUnit` for this object, or `None` when it was a
        content duplicate (D-13) or its run already `SUCCEEDED`.
    """
    object_uri = f"s3://{config.source.bucket}/{obj.key}"

    # D-11, signature-addition-only this plan (see discover_files' own
    # `filename_facets_by_object` Args entry): surfaced only in this
    # function's own log lines below, never persisted -- no dataset declares
    # a filename mask yet (D-10), so this is `None` for every real call this
    # phase.
    business_date_facet: object | None = None
    if filename_facets_by_object is not None:
        object_facets = filename_facets_by_object.get(object_uri)
        if object_facets is not None:
            business_date_facet = object_facets.get("business_date")

    file_id, content_sha256_hex, _content_sha256, is_duplicate = _hash_and_register_file(
        metadata=metadata,
        objects=objects,
        dataset_id=dataset_id,
        config=config,
        obj=obj,
    )

    if is_duplicate:
        log.info(
            "discovery.object_evaluated",
            dataset=dataset_name,
            object_uri=object_uri,
            content_sha256_prefix=content_sha256_hex[:12],
            decision="DUPLICATE",
            business_date=business_date_facet,
        )
        return None  # D-13 skip policy: no batch, no run, no assignment document

    batch_key = f"{dataset_name}:{content_sha256_hex[:16]}"
    # get_or_create_batch, NEVER create_batch: batch_key is a pure function
    # of content_sha256, so a rerun over an unchanged object reaches this
    # line with the SAME batch_key every time (this is a rediscovery, not a
    # genuinely new batch -- the same reasoning as create_file's own
    # idempotent upsert above). create_batch's plain INSERT would raise
    # UniqueViolation against uq_batches_dataset_batch_key on exactly this
    # rerun path.
    batch_id = metadata.get_or_create_batch(
        dataset_id=dataset_id,
        batch_key=batch_key,
        status="OPEN",
    )
    # One-file-one-batch: this phase's documented simplification
    # (03-RESEARCH.md) -- sequence_no is always 1. link_batch_file is itself
    # idempotent (ON CONFLICT DO NOTHING on its composite PK), so re-linking
    # the same (batch_id, file_id) pair on a rerun is harmless.
    metadata.link_batch_file(batch_id=batch_id, file_id=file_id, sequence_no=1)

    # Pitfall 5: APPENDS the schema-version term -- never reorders or
    # replaces the first four (see this module's own docstring).
    idempotency_key = hashlib.sha256(
        f"{dataset_name}|{content_sha256_hex}|{config_hash}|{processor_image}|"
        f"{schema_version_term}".encode(),
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
            business_date=business_date_facet,
        )
        return None

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
        business_date=business_date_facet,
    )
    return DiscoveredUnit(
        assignment_uri=f"s3://metadata/{assignment_key}",
        idempotency_key=idempotency_key,
        run_id=run_id,
    )


def _apply_batch_complete_marker_gate(
    *,
    listed: Sequence[ObjectSummary],
    config: DatasetConfig,
    dataset_name: str,
    log: FilteringBoundLogger,
) -> tuple[list[ObjectSummary], bool]:
    """Apply LOAD-11/D-19's opt-in ``_BATCH_COMPLETE`` marker gate (plan 08-06) to one listing.

    Extracted from ``discover_files``'s own body purely to keep that
    function's cyclomatic complexity within this codebase's lint gate (ruff
    C901/PLR0912), the same reason ``_process_multipart_group``/
    ``_process_ungrouped_object`` were extracted -- NO behavior change from
    an inline version.

    When ``config.source.batch_complete_marker`` is ``None`` (the default;
    ``customers``/``orders``, this phase), this is a no-op: ``listed`` comes
    back unchanged and ``batch_withheld`` is always ``False`` -- byte-for-
    byte identical to calling ``discover_files`` before this plan.

    When it is set, the object whose key equals
    ``config.source.path + config.source.batch_complete_marker`` must be
    present in ``listed`` before ANY object in the listing is hashed,
    registered, batched or assigned this call (LOAD-11: "refused before any
    parsing occurs", extended here to "before any discovery bookkeeping
    occurs" for the WHOLE batch, not merely the marker's own object). This
    mirrors Phase 6 D-10's exact "opt-in, unexercised by both live datasets"
    precedent for filename masks: built, corpus/unit-tested, but neither
    live dataset's config sets this field this phase.

    Args:
        listed: The already-sorted object listing ``discover_files`` just
            produced from ``objects.list_objects(...)``.
        config: The dataset's already-validated, resolved configuration.
        dataset_name: The dataset's unique name, for the withheld-batch log
            line.
        log: The structured logger ``discover_files`` itself already holds.

    Returns:
        A ``(listed, batch_withheld)`` pair. When ``batch_withheld`` is
        ``True``, ``listed`` is always ``[]`` and the caller MUST return
        ``[]`` immediately without resolving schema version or entering the
        multipart-partition/per-object loop. When ``False``, ``listed`` is
        either the original listing unchanged (marker not configured) or the
        original listing with the marker object itself removed (marker
        configured and found) -- the marker is never mistaken for a
        candidate data file either way.
    """
    if config.source.batch_complete_marker is None:
        return list(listed), False

    marker_key = config.source.path + config.source.batch_complete_marker
    if not any(obj.key == marker_key for obj in listed):
        log.info(
            "discovery.batch_incomplete",
            dataset=dataset_name,
            marker_key=marker_key,
        )
        return [], True

    return [obj for obj in listed if obj.key != marker_key], False


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
    schema: SchemaRepository,
    filename_facets_by_object: Mapping[str, Mapping[str, object]] | None = None,
) -> list[DiscoveredUnit]:
    """List, hash, dedup-check, freeze and cap one dataset's discoverable files.

    Sequence, once per call: (-1) when ``config.source.batch_complete_marker``
    is set (LOAD-11/D-19, plan 08-06), check whether an object whose key is
    ``config.source.path + config.source.batch_complete_marker`` is present
    in the listing -- when absent, the WHOLE batch is withheld: return
    ``[]`` immediately, before any hashing, registration, batching or
    assignment happens for ANY object this call, even ones that would
    otherwise discover cleanly. This is the same "opt-in, unexercised by
    both live datasets" precedent as Phase 6's filename masks (D-10):
    ``None`` (the default) skips this check entirely, and every dataset that
    does not set it (``customers``, ``orders``, this phase) behaves exactly
    as before this plan. When the marker IS present, it is stripped from the
    listing before every later step -- it is never treated as a candidate
    data file. (0) when ``config.source.multipart_pattern`` is
    set (CSV-11, plan 06-18), partition the listing into multipart-group
    candidates and the remainder via ``group_multipart_units`` -- every
    other dataset (``multipart_pattern is None``, e.g. ``customers``) skips
    this step entirely and behaves exactly as before this plan. (1) list
    every object under ``config.source.bucket``/``config.source.path``,
    sorted by key -- so the same inputs always produce the same manifest
    (ORCH-08's frozen-manifest requirement extends to determinism, not
    merely to "frozen after writing"). (2) For each object (each multipart
    group's every member, each ungrouped object), stream its raw bytes
    through ``hashlib.sha256()`` in bounded chunks -- the object is never
    loaded whole into memory. (3) Check ``meta.files`` for a content-hash
    duplicate under a DIFFERENT ``object_uri`` (D-13); when found and
    ``config.source.duplicate_policy == "skip"``, record the file with
    ``duplicate_of_file_id`` set and stop -- no batch, no run, no assignment
    document for it (for a multipart group, ANY duplicate part stops the
    WHOLE group, T-06-33). (4) Otherwise, create a one-file batch (or, for a
    multipart group, one batch spanning every part -- this phase's
    documented one-file-one-batch simplification generalizes to
    one-group-one-batch), pre-allocate an ingestion run (idempotent -- a run
    already ``SUCCEEDED`` is skipped, everything else is re-offered), and
    freeze an ``AssignmentDocument`` to
    ``s3://metadata/assignments/<dataset_name>/<run_id>.json``. (5) Cap the
    result at ``config.batching.max_units_per_run``, deterministically
    (never by listing order, which S3-compatible stores do not guarantee
    stable across pages) -- files beyond the cap are left exactly as
    upserted in ``meta.files``/``meta.ingestion_runs`` and are picked up by
    the very next ``discover_files`` call, never lost.

    ``meta.files.business_date`` is never populated here -- this function
    reads no wall-clock time and no Airflow scheduling-interval value
    anywhere (see the module docstring). ``filename_facets_by_object`` (D-11)
    is consulted per-object purely for discovery-log observability -- see its
    own Args entry below for the full scope boundary.

    Idempotency key: see the module docstring for the full formula and its
    ARCHITECTURE.md Q7 provenance. This dataset's CURRENT schema version
    (``schema.get_current(dataset_id)``) is resolved exactly ONCE per call,
    before the per-object loop -- schema version is dataset-wide, not
    per-object, and re-resolving it per-object would risk a single
    ``discover_files`` call spanning two different schema-version terms if a
    concurrent schema sync landed mid-call, breaking ORCH-08's frozen-manifest
    determinism.

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
        schema: The dataset's schema-version repository, consulted once
            (``get_current(dataset_id)``) to append a ``schema_version`` term
            to the idempotency key (Pitfall 5). When the dataset has no
            current schema version yet (its very first discovery run, before
            any schema sync has run for it), the term is an empty string
            rather than blocking discovery on it -- a dataset's first file
            establishes its own baseline schema via a later wiring point
            (plan 06-15's job, not this function's).
        filename_facets_by_object: A caller-precomputed mapping from
            ``object_uri`` to that file's already-extracted filename facets
            (``csv_processor.detect.filename.parse_filename``'s return
            shape), or ``None`` when no filename mask is configured for this
            dataset. ``dataplat`` must never import ``csv_processor``
            (import-linter contract 1), so this function never calls
            ``parse_filename`` itself -- the caller (``csv_processor.cli.discover``,
            which is allowed to import both) is responsible for building this
            map. THIS IS A SIGNATURE ADDITION ONLY, this plan (06-16): no
            dataset declares a filename mask yet (D-10), so no live caller
            populates this parameter this phase, and there is no
            ``meta.files.business_date`` write path here at all
            (``MetadataRepository.create_file`` accepts no such column) --
            a present ``business_date`` facet is surfaced only in this
            function's own discovery log line, for operator visibility, never
            persisted. D-11's actual priority rule -- a filename-derived
            business date is a FALLBACK ONLY, consulted strictly when a
            file's data/content carries no derivable date, and must never
            override a data-derived one -- has no consuming implementation
            anywhere in this codebase yet; that remains a future phase's
            integration point, once a dataset that declares a mask exists to
            exercise it.

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

    # LOAD-11/D-19 opt-in `_BATCH_COMPLETE` marker gate (plan 08-06) -- see
    # `_apply_batch_complete_marker_gate`'s own docstring for the full
    # behavior. Extracted into its own function (mirrors
    # `_process_multipart_group`/`_process_ungrouped_object`'s own
    # extraction precedent below) purely to keep this function's cyclomatic
    # complexity within this codebase's lint gate (ruff C901/PLR0912) -- NO
    # behavior change from the inline version.
    listed, batch_withheld = _apply_batch_complete_marker_gate(
        listed=listed,
        config=config,
        dataset_name=dataset_name,
        log=log,
    )
    if batch_withheld:
        return []

    # Resolved ONCE per call, before the per-object loop (see this function's
    # own docstring): schema version is dataset-wide, not per-object.
    # Pitfall 5 -- no current schema version yet (this dataset's very first
    # discovery run) contributes an empty string rather than blocking
    # discovery on it.
    current_schema = schema.get_current(dataset_id)
    schema_version_term = "" if current_schema is None else str(current_schema.version)

    # Step 0 (this plan, 06-18): partition `listed` into multipart-group
    # candidates (keys matching `multipart_pattern` anywhere -- `re.search`,
    # deliberately looser than `group_multipart_units`'s own internal
    # `re.fullmatch` grouping check) and the remainder, which stays exactly
    # as `listed` for the pre-existing per-object loop below. When no
    # `multipart_pattern` is configured (`None` -- e.g. `customers`), no
    # partition happens at all: `remaining` is `listed` unchanged and
    # `groups` is empty, so the per-object loop's behavior is unchanged from
    # before this plan (this dataset's own regression guarantee).
    multipart_pattern = config.source.multipart_pattern
    groups: list[MultipartGroup] = []
    if multipart_pattern is None:
        remaining = listed
    else:
        multipart_candidates: list[ObjectSummary] = []
        remaining = []
        for obj in listed:
            if re.search(multipart_pattern, obj.key):
                multipart_candidates.append(obj)
            else:
                remaining.append(obj)
        groups = group_multipart_units(multipart_candidates, pattern=multipart_pattern)

    candidates: list[DiscoveredUnit] = []

    # `groups` is `[]` (never truthy) whenever `multipart_pattern is None` --
    # this loop is then simply skipped, so no separate `if groups:` guard is
    # needed (one fewer branch keeps `discover_files` under this codebase's
    # ruff C901/PLR0912 complexity gate after the marker-gate call above).
    objects_by_key = {obj.key: obj for obj in listed}
    for group in groups:
        group_unit = _process_multipart_group(
            metadata=metadata,
            objects=objects,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            config=config,
            config_version_id=config_version_id,
            config_hash=config_hash,
            processor_image=processor_image,
            processor_version=processor_version,
            schema_version_term=schema_version_term,
            group=group,
            objects_by_key=objects_by_key,
            log=log,
        )
        if group_unit is not None:
            candidates.append(group_unit)

    for obj in remaining:
        object_unit = _process_ungrouped_object(
            metadata=metadata,
            objects=objects,
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            config=config,
            config_version_id=config_version_id,
            config_hash=config_hash,
            processor_image=processor_image,
            processor_version=processor_version,
            schema_version_term=schema_version_term,
            obj=obj,
            filename_facets_by_object=filename_facets_by_object,
            log=log,
        )
        if object_unit is not None:
            candidates.append(object_unit)

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
