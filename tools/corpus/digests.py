"""The committed oracle, in standard ``sha256sum`` format.

The format is not a private convention: ``sha256sum -c tests/fixtures/CORPUS.sha256``
must work from the repository root. An oracle only this project can read is an
oracle only this project can be wrong about, and an independent second opinion
is the whole point of committing digests rather than trusting regeneration.

That is why names are written **relative to the repository root** rather than as
bare file names. A listing of bare names resolves only when the checker happens
to be run from inside the corpus directory, which turns the independent check
into a trick you have to know.

``generate`` rewrites this file; ``verify`` only ever reads it. That asymmetry is
what makes a generator change a reviewable diff instead of a silent re-baseline.
"""

from __future__ import annotations

import hashlib
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    from collections.abc import Mapping
    from pathlib import Path

_READ_CHUNK: Final = 1 << 20

# GNU coreutils separates the digest from the name with two spaces for text mode
# and " *" for binary mode. We emit the two-space form and accept both.
_SEPARATOR: Final = "  "


class DigestFormatError(ValueError):
    """A digest listing is not in ``sha256sum`` format."""


def sha256_file(path: Path) -> str:
    """Hash a file without reading it into memory.

    Args:
        path: File to hash.

    Returns:
        The lowercase hex SHA-256 digest.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_READ_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def qualify(digests: Mapping[str, str], prefix: str) -> dict[str, str]:
    """Prefix bare fixture names with the corpus directory.

    Args:
        digests: Fixture name to hex digest, in declared order.
        prefix: Corpus directory relative to the repository root.

    Returns:
        Repository-relative path to hex digest, in the same order.
    """
    if not prefix:
        return dict(digests)
    root = prefix.rstrip("/")
    return {f"{root}/{name}": digest for name, digest in digests.items()}


def format_digests(digests: Mapping[str, str]) -> str:
    """Render a digest listing in ``sha256sum`` format.

    Args:
        digests: Name to hex digest, in the order to be written. The order is
            the manifest's declared order, so adding one fixture adds one line
            rather than reshuffling the file.

    Returns:
        The listing, newline-terminated.
    """
    return "".join(f"{digest}{_SEPARATOR}{name}\n" for name, digest in digests.items())


def parse_digests(text: str, *, source: str = "<digests>") -> dict[str, str]:
    """Parse a ``sha256sum``-format listing.

    Args:
        text: The listing.
        source: Label used in error messages.

    Returns:
        Fixture name to hex digest, in file order.

    Raises:
        DigestFormatError: If a line is not a digest followed by a name.
    """
    parsed: dict[str, str] = {}
    for number, line in enumerate(text.splitlines(), start=1):
        if not line.strip():
            continue
        digest, separator, name = line.partition(" ")
        if not separator or not name:
            msg = f"{source}:{number}: not a sha256sum line: {line!r}"
            raise DigestFormatError(msg)
        if len(digest) != hashlib.sha256().digest_size * 2:
            msg = f"{source}:{number}: digest is not 64 hex characters: {digest!r}"
            raise DigestFormatError(msg)
        # Accept both the text-mode ("  name") and binary-mode (" *name") forms.
        parsed[name.lstrip(" *")] = digest
    return parsed


def read_digests(path: Path) -> dict[str, str]:
    """Read and parse a committed digest listing.

    Args:
        path: The oracle file.

    Returns:
        Fixture name to hex digest, in file order.

    Raises:
        DigestFormatError: If the file is missing or malformed.
    """
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        msg = f"{path}: cannot read the digest oracle: {exc}"
        raise DigestFormatError(msg) from exc
    return parse_digests(text, source=str(path))


def write_digests(path: Path, digests: Mapping[str, str]) -> None:
    """Write a digest listing, replacing any previous content.

    Args:
        path: The oracle file.
        digests: Fixture name to hex digest, in declared order.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(format_digests(digests), encoding="utf-8")
