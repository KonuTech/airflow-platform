"""CLI: ``python -m tools.corpus generate|verify``.

``generate`` materialises the corpus into a directory and (optionally) rewrites
the digest oracle. ``verify`` regenerates into a temporary directory and compares
against the committed oracle **without touching it, and without ever reading the
on-disk corpus**. Its only two inputs are the manifest and the oracle, which is
exactly why a generator change shows up in code review instead of silently
re-baselining every downstream expectation.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

# Relative imports: see the note in generators.py — `tools/` is a namespace
# package, so absolute self-imports would resolve this tree under two names.
from .digests import qualify, read_digests, write_digests
from .generators import generate_corpus
from .manifest import load_manifest

_DEFAULT_MANIFEST = Path("tests/fixtures/corpus.yaml")
_DEFAULT_OUT = Path("tests/fixtures/csv")
_DEFAULT_DIGESTS = Path("tests/fixtures/CORPUS.sha256")

# Names in the oracle are relative to the repository root so `sha256sum -c` works
# there. This is deliberately independent of `--out`, because `verify`
# regenerates into a throwaway directory whose path must never reach the oracle.
_DIGEST_PREFIX = "tests/fixtures/csv"


def build_parser() -> argparse.ArgumentParser:
    """Build the command-line parser.

    Returns:
        The configured parser for ``generate`` and ``verify``.
    """
    parser = argparse.ArgumentParser(
        prog="python -m tools.corpus",
        description="Generate and verify the deterministic CSV fixture corpus.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    generate = subparsers.add_parser("generate", help="materialise the corpus")
    generate.add_argument("--manifest", type=Path, default=_DEFAULT_MANIFEST)
    generate.add_argument("--out", type=Path, default=_DEFAULT_OUT)
    generate.add_argument(
        "--write-digests",
        type=Path,
        default=None,
        metavar="PATH",
        help="rewrite the digest oracle at PATH (refused together with --fast)",
    )
    generate.add_argument(
        "--fast",
        action="store_true",
        help="skip profile: large fixtures — inner development loop only",
    )
    generate.add_argument("--digest-prefix", default=_DIGEST_PREFIX, metavar="DIR")

    verify = subparsers.add_parser("verify", help="prove byte-identity against the oracle")
    verify.add_argument("--manifest", type=Path, default=_DEFAULT_MANIFEST)
    verify.add_argument("--digests", type=Path, default=_DEFAULT_DIGESTS)
    verify.add_argument(
        "--fast",
        action="store_true",
        help="skip profile: large fixtures — inner development loop only",
    )
    verify.add_argument("--digest-prefix", default=_DIGEST_PREFIX, metavar="DIR")
    return parser


def command_generate(args: argparse.Namespace) -> int:
    """Materialise the corpus and optionally rewrite the oracle.

    Args:
        args: Parsed arguments.

    Returns:
        Process exit status.
    """
    if args.fast and args.write_digests is not None:
        print(
            "refusing to rewrite the digest oracle from a --fast run: it would "
            "drop the digest of every profile: large fixture and leave a "
            "partial oracle that looks complete.",
            file=sys.stderr,
        )
        return 2

    manifest = load_manifest(args.manifest)
    digests = generate_corpus(manifest, args.out, fast=args.fast)

    if args.write_digests is not None:
        write_digests(args.write_digests, qualify(digests, args.digest_prefix))
        print(f"wrote {len(digests)} digests to {args.write_digests}")
    else:
        print(f"generated {len(digests)} fixtures into {args.out}")
    return 0


def command_verify(args: argparse.Namespace) -> int:
    """Regenerate into a temporary directory and compare against the oracle.

    Args:
        args: Parsed arguments.

    Returns:
        Process exit status: 0 when every regenerated fixture matches.
    """
    manifest = load_manifest(args.manifest)
    expected = read_digests(args.digests)

    with tempfile.TemporaryDirectory(prefix="corpus-verify-") as tmp:
        actual = qualify(generate_corpus(manifest, Path(tmp), fast=args.fast), args.digest_prefix)

    problems: list[str] = []
    for name, digest in actual.items():
        if name not in expected:
            problems.append(f"{name}: generated but absent from {args.digests}")
        elif expected[name] != digest:
            problems.append(f"{name}: expected {expected[name]}, regenerated {digest}")

    if not args.fast:
        problems.extend(
            f"{name}: present in {args.digests} but not generated"
            for name in expected
            if name not in actual
        )

    if problems:
        print(f"FIXTURE VERIFICATION FAILED ({len(problems)} problem(s)):", file=sys.stderr)
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        print(
            "\nIf this change to the generator or the manifest is intended, run "
            "`make fixtures` and review the CORPUS.sha256 diff. A digest diff "
            "larger than the corpus.yaml diff means a shared random stream has "
            "coupled the fixtures together (determinism rule R1).",
            file=sys.stderr,
        )
        return 1

    skipped = " (large profile skipped)" if args.fast else ""
    print(f"{len(actual)} fixtures match {args.digests}{skipped}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Run the corpus CLI.

    Args:
        argv: Argument vector, defaulting to ``sys.argv[1:]``.

    Returns:
        Process exit status.
    """
    args = build_parser().parse_args(argv)
    if args.command == "generate":
        return command_generate(args)
    return command_verify(args)


if __name__ == "__main__":
    sys.exit(main())
