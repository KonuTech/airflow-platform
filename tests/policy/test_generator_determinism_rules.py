"""The determinism rules, enforced so they cannot erode (QUAL-08).

Rule R2 rests on a documented CPython guarantee — ``Random.random()`` keeps its
sequence across versions, while ``choice``, ``shuffle``, ``sample``,
``randrange`` and ``randint`` are explicitly allowed to change. Only one Python
version exists on any given machine, so no test run here can *observe* a
cross-version break. This source check is the insurance instead: if the helpers
never appear, the break can never happen.

Rule R6 is the same shape. Nothing fails today when someone adds a "helpful"
``generated_at`` header; it fails months later, on a different machine, as an
unexplained digest mismatch.

Rule R1 is different — it has an observable consequence, so it is tested by
consequence rather than by inspection.

Two deliberate design choices:

* **Docstrings and comments are masked before scanning.** The rules are about
  *code*, and the generator's own module docstring names every forbidden helper
  in order to explain why it must not use them. A scan that flagged its own
  documentation would push authors to delete the explanation — the opposite of
  what the rule wants.
* **The forbidden names are assembled from fragments**, and only
  ``tools/corpus/`` is walked. Both keep this file from matching itself, which
  is the standing trade-off of expressing policy as a test.
"""

from __future__ import annotations

import ast
import io
import re
import tempfile
import tokenize
from pathlib import Path
from typing import Final

from tools.corpus.generators import generate_corpus
from tools.corpus.manifest import load_manifest

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_PACKAGE = REPO_ROOT / "tools" / "corpus"
MANIFEST = REPO_ROOT / "tests" / "fixtures" / "corpus.yaml"

# Assembled from fragments so this module does not match its own pattern.
R2_HELPERS: Final[tuple[str, ...]] = (
    "cho" + "ice",
    "cho" + "ices",
    "shuf" + "fle",
    "sam" + "ple",
    "rand" + "range",
    "rand" + "int",
    "uni" + "form",
)

R6_NONDETERMINISM: Final[tuple[str, ...]] = (
    "n" + "ow",
    "to" + "day",
    "utc" + "now",
    "ur" + "andom",
    "get" + "pid",
    "mono" + "tonic",
    "perf_" + "counter",
    "process_" + "time",
    "uu" + "id1",
    "uu" + "id4",
    "sec" + "rets",
    "System" + "Random",
    "ti" + "me",
)

FORBIDDEN: Final[dict[str, str]] = {
    **dict.fromkeys(R2_HELPERS, "R2"),
    **dict.fromkeys(R6_NONDETERMINISM, "R6"),
}

_WORD = re.compile(r"\b(" + "|".join(sorted(FORBIDDEN)) + r")\b")


def _generator_modules() -> list[Path]:
    return sorted(GENERATOR_PACKAGE.rglob("*.py"))


def _mask_docstrings_and_comments(source: str) -> list[str]:
    """Blank out docstring and comment text, keeping line numbers intact."""
    lines = source.splitlines()
    masked = list(lines)

    tree = ast.parse(source)
    documented = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if not isinstance(node, documented) or not node.body:
            continue
        if ast.get_docstring(node, clean=False) is None:
            continue
        first = node.body[0]
        end = first.end_lineno or first.lineno
        for index in range(first.lineno - 1, end):
            masked[index] = ""

    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            row = token.start[0] - 1
            if masked[row]:
                masked[row] = masked[row][: token.start[1]]

    return masked


def _ast_findings(path: Path, source: str) -> list[str]:
    """Find forbidden identifiers as the parser sees them, not as text."""
    findings: list[str] = []
    for node in ast.walk(ast.parse(source)):
        names: list[str] = []
        if isinstance(node, ast.Attribute):
            names.append(node.attr)
        elif isinstance(node, ast.Name):
            names.append(node.id)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            names.extend(alias.name for alias in node.names)
            if isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module)
        for name in names:
            rule = FORBIDDEN.get(name)
            if rule is not None:
                findings.append(
                    f"{path.relative_to(REPO_ROOT)}:{node.lineno}: "
                    f"{name!r} violates determinism rule {rule}"
                )
    return findings


def _text_findings(path: Path, source: str) -> list[str]:
    """Catch dynamic access the parser cannot see, e.g. getattr by name."""
    findings: list[str] = []
    for number, line in enumerate(_mask_docstrings_and_comments(source), start=1):
        for match in _WORD.finditer(line):
            name = match.group(1)
            findings.append(
                f"{path.relative_to(REPO_ROOT)}:{number}: "
                f"{name!r} violates determinism rule {FORBIDDEN[name]}"
            )
    return findings


def test_the_generator_package_is_not_empty() -> None:
    # A source-inspection policy that silently scans nothing is the archetypal
    # control that fails open.
    modules = _generator_modules()
    assert len(modules) >= 4, f"expected the corpus generator package, found {modules}"


def test_no_unstable_random_helper_or_nondeterministic_call() -> None:
    findings: list[str] = []
    for path in _generator_modules():
        source = path.read_text(encoding="utf-8")
        findings.extend(_ast_findings(path, source))
        findings.extend(_text_findings(path, source))

    assert not findings, (
        "determinism rules violated in tools/corpus/:\n"
        + "\n".join(f"  {finding}" for finding in sorted(set(findings)))
        + "\n\nR2: consume randomness only through Random.random(); the helpers "
        "above are documented as subject to change between Python versions, so "
        "the corpus would silently differ after an interpreter upgrade.\n"
        "R6: no wall-clock, process-identity or OS-entropy value may reach a "
        "fixture; every timestamp is a literal in the manifest."
    )


def test_the_scanner_would_actually_fire() -> None:
    # A control that has never been observed to fail is indistinguishable from
    # a disabled one. Prove both nets catch a violation.
    probe = "import random\n\ndef f(values):\n    return random." + "cho" + "ice(values)\n"
    findings = _ast_findings(Path(__file__), probe) + _text_findings(Path(__file__), probe)
    assert findings, "the forbidden-helper scanner did not fire on a deliberate violation"
    assert any("R2" in finding for finding in findings)
    assert any(":4:" in finding for finding in findings), (
        f"the finding must name the offending line; got {findings}"
    )

    clock = "import datetime\n\ndef g():\n    return datetime.datetime." + "n" + "ow()\n"
    clock_findings = _ast_findings(Path(__file__), clock) + _text_findings(Path(__file__), clock)
    assert any("R6" in finding for finding in clock_findings), (
        f"the wall-clock scanner did not fire; got {clock_findings}"
    )


def test_documentation_of_the_forbidden_helpers_is_not_itself_a_violation() -> None:
    # The masking rule, asserted directly: naming a helper in a docstring or a
    # comment must stay legal, or the generator cannot explain its own design.
    documented = (
        '"""Never call random.' + "cho" + 'ice here."""\n'
        "\n"
        "# Also never datetime." + "n" + "ow().\n"
        "VALUE = 1\n"
    )
    assert not _text_findings(Path(__file__), documented)
    assert not _ast_findings(Path(__file__), documented)


def test_adding_a_fixture_leaves_every_other_digest_unchanged(tmp_path: Path) -> None:
    """Rule R1, tested by its observable consequence rather than by inspection.

    The probe is inserted at the *head* of the fixture list on purpose. A shared
    random stream only shows up in fixtures that are generated *after* the
    insertion point and that actually draw from it, so appending at the end
    would pass even with the bug this rule exists to prevent.
    """
    original = MANIFEST.read_text(encoding="utf-8")
    anchor = '  - name: "01_simple.csv"'
    assert anchor in original, "the manifest no longer declares the anchor fixture"

    probe = (
        '  - name: "99_inserted_probe.csv"\n'
        "    generator: tabular\n"
        "    encoding: utf-8\n"
        '    delimiter: ","\n'
        '    line_terminator: "\\n"\n'
        "    header: [id, label]\n"
        "    rows: 5\n"
        "    row_spec:\n"
        "      id: { kind: zero_padded_int, width: 3, start: 1 }\n"
        '      label: { kind: pick, values: ["alpha", "beta", "gamma"] }\n'
        "\n"
    )
    altered_path = tmp_path / "corpus-altered.yaml"
    altered_path.write_text(original.replace(anchor, probe + anchor, 1), encoding="utf-8")

    baseline = load_manifest(MANIFEST)
    altered = load_manifest(altered_path)

    # fast=True skips the ~293 MB fixture: R1 is a property of stream
    # derivation, and the large profile adds half a minute without adding
    # evidence. test_corpus_determinism.py covers it at full size.
    with (
        tempfile.TemporaryDirectory(prefix="corpus-r1-before-") as before_dir,
        tempfile.TemporaryDirectory(prefix="corpus-r1-after-") as after_dir,
    ):
        before = generate_corpus(baseline, Path(before_dir), fast=True)
        after = generate_corpus(altered, Path(after_dir), fast=True)

    assert set(after) - set(before) == {"99_inserted_probe.csv"}

    drifted = [
        f"{name}: {digest} became {after[name]}"
        for name, digest in before.items()
        if after[name] != digest
    ]
    assert not drifted, (
        "inserting one fixture changed the bytes of "
        f"{len(drifted)} other fixture(s):\n" + "\n".join(f"  {line}" for line in drifted) + "\n\n"
        "This is determinism rule R1: every fixture must draw from a stream "
        "derived from sha256(master_seed | fixture_name), never from one shared "
        "stream. A CORPUS.sha256 diff larger than the corpus.yaml diff is the "
        "warning sign — review becomes impossible and someone regenerates "
        "without looking."
    )
