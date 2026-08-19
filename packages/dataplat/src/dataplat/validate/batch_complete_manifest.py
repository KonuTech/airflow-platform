"""``BatchCompleteManifest`` -- the ``_BATCH_COMPLETE`` marker's parsed body (D-23, VALID-06).

09-03-PLAN.md: extends the existing, presence-only ``_BATCH_COMPLETE`` marker gate
(``dataplat.discovery._apply_batch_complete_marker_gate``, LOAD-11/D-19, opt-in) to actually
READ and PARSE its object body, rather than merely checking the object exists. D-23 chose to
extend that existing marker convention (a well-known object key inside the same batch directory
as the data files) rather than invent a new sidecar-file convention -- this model is the JSON
shape that object's body is expected to carry.

Threat model (T-09-06/T-09-07, this plan): the marker arrives via the same untrusted ``raw``
bucket as CSV content, not a trusted internal source -- so this model gets the same
``extra="forbid"``, bounded-length discipline every other attacker-influence-adjacent model in
this codebase carries (``AssignmentDocument``'s own T-04-02 precedent). This module only PARSES
and THREADS the value; the comparison-and-discrepancy-flagging logic (a later plan) always
treats ``expected_*`` as a claim to verify against the actually-loaded count, never as ground
truth to suppress an alert.
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from dataplat.errors import ConfigurationError


class BatchCompleteManifest(BaseModel):
    """The parsed body of a dataset's ``_BATCH_COMPLETE`` marker object.

    Attributes:
        expected_row_count: The control total the marker claims this batch's data files should
            sum to. Never trusted as ground truth on its own -- a later plan compares it against
            what actually got staged.
        expected_checksum: An optional control checksum the marker claims for this batch.
            ``None`` when the marker's author did not supply one. Bounded to 128 characters
            (T-09-07): this body arrives via the same untrusted ``raw`` bucket as CSV content.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    expected_row_count: int = Field(ge=0)
    expected_checksum: str | None = Field(default=None, max_length=128)


def parse_batch_complete_manifest(text: str, *, marker_key: str) -> BatchCompleteManifest:
    """Parse a ``_BATCH_COMPLETE`` marker object's body text into a ``BatchCompleteManifest``.

    Args:
        text: The marker object's raw body, read via ``ObjectStore.get_object(...).read()``.
        marker_key: The object key the body was read from, recorded in the raised error's
            ``context`` when parsing fails -- never the raw body content (T-09-08: information
            disclosure via a log line).

    Returns:
        The parsed, validated manifest.

    Raises:
        ConfigurationError: ``text`` is not valid JSON, or fails ``BatchCompleteManifest``'s own
            validation (non-negative row count, no unrecognized top-level key, bounded checksum
            length) -- never lets a raw ``pydantic.ValidationError`` propagate to the caller, so
            a malformed body can be caught and treated as "marker unreadable" rather than
            crashing discovery.
    """
    try:
        return BatchCompleteManifest.model_validate_json(text)
    except ValidationError as exc:
        msg = f"invalid _BATCH_COMPLETE manifest body at {marker_key}"
        raise ConfigurationError(msg, context={"marker_key": marker_key}) from exc
