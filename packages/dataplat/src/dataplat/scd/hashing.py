"""``tracked_attribute_hash`` -- deterministic normalized-hash change detection (SCD-05).

The Type-2 version-boundary primitive ``dataplat.scd.recompute.
recompute_version_chain`` (this plan's Task 2) calls to decide whether two
consecutive bronze rows describe the same logical version or a new one.

**Recipe provenance.** Matches ``dataplat.load.staging.StagingLoader``'s own
``_record_hash`` computation EXACTLY (staging.py lines ~653-672, the only
existing hash-computation precedent in this codebase): pipe-join the given
values, substituting ``""`` for any ``None``, then SHA-256 the UTF-8-encoded
result. Reusing this exact recipe -- rather than inventing a second one --
keeps "what does a content hash mean in this codebase" a single, auditable
answer.

**Normalization is the CALLER's job, not this function's.** Every value
this function ever receives, in the real pipeline, has already passed
through ``dataplat.normalize.unicode.UnicodeNormalizer``'s unconditional
NFC pass (D-15) before reaching here -- normalization must run BEFORE
hashing so a byte-distinct-but-visually-identical NFC/NFD pair collapses to
one hash, never two. This function deliberately does NOT call
``unicodedata.normalize`` itself: re-normalizing here would be redundant
work on the hot path and would silently mask a caller that forgot to
normalize (better for that bug to surface as a real hash mismatch than be
papered over twice).
"""

from __future__ import annotations

import hashlib


def tracked_attribute_hash(*values: str | None, hash_version: int = 1) -> bytes:
    """Deterministic SHA-256 digest over normalized, pipe-joined tracked values.

    Args:
        *values: The tracked Type-2 column values for one row, in a stable,
            caller-chosen order (e.g. ``name, country``). Accepts any
            number of positional values so a future dataset's own Type-2
            column list needs no signature change. A value of ``None`` is
            valid (a nullable Type-2 column) and is rendered as ``""``,
            matching ``staging.py``'s own convention -- never call
            ``str(None)``.
        hash_version: Recorded alongside the returned hash by the caller
            (mirroring ``_record_hash_version``'s own stored-alongside-not-
            baked-in convention, META-02). Accepted here as an explicit
            extension point for a future recipe change, but NOT currently
            mixed into the hash bytes themselves -- if the recipe below
            ever changes, bump the version AND fold ``hash_version`` into
            the digest input at that time.

    Returns:
        A 32-byte SHA-256 digest of the pipe-joined, UTF-8-encoded values.
    """
    joined = "|".join("" if value is None else value for value in values)
    return hashlib.sha256(joined.encode("utf-8")).digest()
