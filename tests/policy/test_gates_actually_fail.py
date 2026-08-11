"""Meta-verification: every gate in this phase is observed rejecting a bad input.

Six of this phase's twelve requirements are satisfied by "a linter is
configured". A configured linter that has never been seen to fail is
indistinguishable from a disabled one — the same argument the gitleaks
self-test (SEC-11) makes about the secret scanner, generalised to the rest of
the toolchain.

Each gate is therefore exercised twice:

* a **negative** case — the real tool runs against a deliberately-broken sample
  and must exit non-zero **and** name the expected rule. Asserting only the exit
  status would also pass if the tool crashed or could not find its config, so
  the rule identity is what makes this a test about enforcement rather than
  about the tool being installed.
* a **positive control** — the same tool, the same configuration, the matching
  correct sample, and a zero exit. A gate that rejects everything is exactly as
  broken as one that rejects nothing, and only the pair distinguishes them.

Requirements covered: OBS-03 (T201), QUAL-02 (D103, D417), QUAL-01 / CICD-04
(mypy no-untyped-def), CICD-03 (ruff runs and can fail the build), and the
import contract behind README §6.4 / §68.

## Why the samples are copied out of the repository before a gate runs

The samples live under `tests/`, and `pyproject.toml` relaxes `D` and `ANN` for
`tests/**` — test code is not public API. Linting a bad sample *in place* would
prove nothing about the docstring rules, because the path itself suppresses
them. Every case therefore copies its sample into a throwaway library-shaped
package under `tmp_path` and runs the tool there against this repository's real
`pyproject.toml`. The rules are then evaluated exactly as they are for
`packages/*/src`, which is the code the requirements are about.

The import contract is handled the same way, for a different reason: making the
repository's own contract fail would mean editing `setup.cfg`, and a test that
mutates the file it governs cannot be run twice.

## Why `tests/policy/badsamples/` is excluded from the main runs

`pyproject.toml` excludes that directory from ruff (`extend-exclude`) and from
mypy (`exclude`). Without the exclusion `make lint` is permanently red, and the
cheapest way to green a build broken that way is to delete the samples rather
than restore the exclusion — which would remove the only evidence that any of
these gates work. `test_the_main_gate_does_not_lint_the_bad_samples` asserts the
exclusion is in force by running the real targets, so that failure mode surfaces
as a failing test rather than as a mystery.

## Adding a sample

1. Add the broken file **and** its `good_` counterpart. Never one without the
   other: a gate that rejects everything is as broken as one that rejects
   nothing, and only the pair tells them apart.
2. Give both a header naming the rule (`# Trips:` / `# Proves:`) and the
   consuming test. The consumer name is derived from the file name and checked
   by `test_every_sample_declares_its_rule_and_consumer`, so a sample cannot
   drift away from its test while still looking documented.
3. Add the negative case and the positive control below.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SAMPLES = Path(__file__).resolve().parent / "badsamples"
MAKEFILE = REPO_ROOT / "Makefile"

# The scratch package name is distinctive so it can never collide with a real
# distribution on sys.path during the import-contract cases.
SCRATCH_LIB = "gatecheck_lib"
SCRATCH_CORE = "gatecheck_core"
SCRATCH_PLUGIN = "gatecheck_plugin"

# Which rule each negative case must report. Read this table as the phase's
# claim: these five identifiers are the enforcement, and each has been watched
# firing.
EXPECTED_RUFF_RULE = {
    "print_in_library.py": "T201",  # OBS-03  — console write in library code
    "missing_docstring.py": "D103",  # QUAL-02 — undocumented public function
    "missing_param_doc.py": "D417",  # QUAL-02 — undocumented parameter
}
MYPY_UNTYPED_DEF_CODE = "no-untyped-def"  # QUAL-01 / CICD-04


def _locked_runner() -> list[str]:
    """Read the runner prefix out of the Makefile rather than restating it.

    `make lint` and this test must invoke ruff through the *same* locked
    runner, or the meta-test could pass against a different resolution of the
    toolchain than the gate uses. Parsing the Makefile makes that structural: if
    the `RUN :=` line changes shape, this fails loudly instead of silently
    testing something else.
    """
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(r"^RUN\s*:=\s*(.+)$", text, flags=re.MULTILINE)
    assert match, "Makefile no longer defines `RUN :=` — the locked runner moved"
    runner = match.group(1).strip().replace("$(UV)", "uv")
    parts = runner.split()
    assert parts[0] == "uv", f"unexpected locked runner in the Makefile: {runner!r}"
    return parts


def _run(cmd: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run a real tool from the repository root and hand back the real result.

    No `check=True` and no exception handling: a non-zero exit is the signal
    under test in half of these cases, and swallowing a tool failure into a
    defaulting fallback would turn a broken gate into a passing test.
    """
    return subprocess.run(  # noqa: S603  # deliberately invoking the project toolchain
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )


def _library_shaped_copy(tmp_path: Path, sample: str) -> Path:
    """Copy a sample into a throwaway package whose path suppresses no rule."""
    package = tmp_path / SCRATCH_LIB
    package.mkdir(parents=True)
    (package / "__init__.py").write_text('"""Scratch library package."""\n', encoding="utf-8")
    target = package / "sample.py"
    shutil.copyfile(SAMPLES / sample, target)
    return target


def _ruff(target: Path) -> tuple[int, set[str], str]:
    """Run the configured ruff over one file; return its exit code and rules."""
    proc = _run(
        [
            *_locked_runner(),
            "ruff",
            "check",
            "--no-cache",
            "--config",
            "pyproject.toml",
            "--output-format",
            "json",
            str(target),
        ],
    )
    # A ruff that could not parse its configuration exits 2 with empty JSON.
    # Decoding without a fallback is deliberate: a crash must surface as an
    # error here, never as "no findings, therefore silent".
    assert proc.returncode in {0, 1}, f"ruff failed to run:\n{proc.stdout}\n{proc.stderr}"
    findings = json.loads(proc.stdout or "[]")
    return proc.returncode, {f["code"] for f in findings}, proc.stdout + proc.stderr


def _mypy(target: Path, tmp_path: Path) -> subprocess.CompletedProcess[str]:
    """Run the configured mypy over one file, with a throwaway cache."""
    return _run(
        [
            *_locked_runner(),
            "mypy",
            "--config-file",
            "pyproject.toml",
            "--no-incremental",
            "--cache-dir",
            str(tmp_path / ".mypy_cache"),
            str(target),
        ],
    )


def _import_contract(tmp_path: Path, core_sample: str) -> subprocess.CompletedProcess[str]:
    """Build a two-package scratch project and check a forbidden contract on it.

    The contract mirrors `setup.cfg` contract 1 — a core package that must not
    depend on a plugin package — but over throwaway packages, so the repository's
    own contract file is never edited to manufacture a failure.
    """
    core = tmp_path / SCRATCH_CORE
    plugin = tmp_path / SCRATCH_PLUGIN
    core.mkdir()
    plugin.mkdir()
    shutil.copyfile(SAMPLES / core_sample, core / "__init__.py")
    (plugin / "__init__.py").write_text(
        '"""Scratch plugin package."""\n\nNAME = "gatecheck_plugin"\n',
        encoding="utf-8",
    )
    config = tmp_path / "setup.cfg"
    config.write_text(
        "[importlinter]\n"
        "root_packages =\n"
        f"    {SCRATCH_CORE}\n"
        f"    {SCRATCH_PLUGIN}\n"
        "\n"
        "[importlinter:contract:1]\n"
        f"name = {CONTRACT_NAME}\n"
        "type = forbidden\n"
        "source_modules =\n"
        f"    {SCRATCH_CORE}\n"
        "forbidden_modules =\n"
        f"    {SCRATCH_PLUGIN}\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = str(tmp_path)
    return _run([*_locked_runner(), "lint-imports", "--config", str(config)], env=env)


CONTRACT_NAME = "gatecheck core must not depend on the plugin"


# --------------------------------------------------------------------------
# OBS-03 / CICD-03 — the console-write ban
# --------------------------------------------------------------------------


def test_print_in_library_is_rejected(tmp_path: Path) -> None:
    code, rules, output = _ruff(_library_shaped_copy(tmp_path, "print_in_library.py"))
    assert code == 1, f"ruff accepted a print() in library code:\n{output}"
    assert EXPECTED_RUFF_RULE["print_in_library.py"] in rules, (
        f"ruff failed the file but not for the print: reported {sorted(rules)}"
    )


def test_good_print_in_library_is_accepted(tmp_path: Path) -> None:
    code, rules, output = _ruff(_library_shaped_copy(tmp_path, "good_print_in_library.py"))
    assert code == 0, f"ruff rejected a correct library module:\n{output}\n{sorted(rules)}"


# --------------------------------------------------------------------------
# QUAL-02 — docstrings on public API
# --------------------------------------------------------------------------


def test_missing_docstring_is_rejected(tmp_path: Path) -> None:
    code, rules, output = _ruff(_library_shaped_copy(tmp_path, "missing_docstring.py"))
    assert code == 1, f"ruff accepted an undocumented public function:\n{output}"
    assert EXPECTED_RUFF_RULE["missing_docstring.py"] in rules, (
        f"ruff failed the file but not for the missing docstring: {sorted(rules)}"
    )


def test_good_missing_docstring_is_accepted(tmp_path: Path) -> None:
    code, rules, output = _ruff(_library_shaped_copy(tmp_path, "good_missing_docstring.py"))
    assert code == 0, f"ruff rejected a documented public function:\n{output}\n{sorted(rules)}"


def test_missing_param_doc_is_rejected(tmp_path: Path) -> None:
    """D417 is live, which is QUAL-02's parameter half.

    Honest limit, restated here because it is easy to over-read this test:
    D417 fires only when an `Args:` section EXISTS and omits a parameter. A
    docstring with no `Args:` section at all is not flagged by any rule, so
    "documents every parameter" remains partly a review-time rule. The case is
    kept rather than deleted because the half that IS mechanical is worth
    holding in place.
    """
    code, rules, output = _ruff(_library_shaped_copy(tmp_path, "missing_param_doc.py"))
    assert code == 1, f"ruff accepted a docstring omitting a parameter:\n{output}"
    assert EXPECTED_RUFF_RULE["missing_param_doc.py"] in rules, (
        f"ruff failed the file but not for the undocumented parameter: {sorted(rules)}"
    )


def test_good_missing_param_doc_is_accepted(tmp_path: Path) -> None:
    code, rules, output = _ruff(_library_shaped_copy(tmp_path, "good_missing_param_doc.py"))
    assert code == 0, f"ruff rejected a fully documented signature:\n{output}\n{sorted(rules)}"


# --------------------------------------------------------------------------
# QUAL-01 / CICD-04 — complete type annotations
# --------------------------------------------------------------------------


def test_untyped_public_def_is_rejected(tmp_path: Path) -> None:
    target = _library_shaped_copy(tmp_path, "untyped_public_def.py")
    proc = _mypy(target, tmp_path)
    assert proc.returncode != 0, f"mypy accepted an unannotated public function:\n{proc.stdout}"
    assert MYPY_UNTYPED_DEF_CODE in proc.stdout, (
        f"mypy failed but not with [{MYPY_UNTYPED_DEF_CODE}]:\n{proc.stdout}"
    )


def test_good_untyped_public_def_is_accepted(tmp_path: Path) -> None:
    target = _library_shaped_copy(tmp_path, "good_untyped_public_def.py")
    proc = _mypy(target, tmp_path)
    assert proc.returncode == 0, f"mypy rejected a fully annotated module:\n{proc.stdout}"


# --------------------------------------------------------------------------
# The import contract — README §6.4 / §68
# --------------------------------------------------------------------------


def test_forbidden_import_is_rejected(tmp_path: Path) -> None:
    proc = _import_contract(tmp_path, "forbidden_import.py")
    assert proc.returncode != 0, f"the import checker accepted a banned import:\n{proc.stdout}"
    assert f"{CONTRACT_NAME} BROKEN" in proc.stdout, (
        f"the checker failed without naming the broken contract:\n{proc.stdout}"
    )
    assert f"{SCRATCH_CORE} is not allowed to import {SCRATCH_PLUGIN}" in proc.stdout, (
        f"the checker named no offending import:\n{proc.stdout}"
    )


def test_good_forbidden_import_is_accepted(tmp_path: Path) -> None:
    proc = _import_contract(tmp_path, "good_forbidden_import.py")
    assert proc.returncode == 0, f"the import checker rejected a clean core:\n{proc.stdout}"
    assert f"{CONTRACT_NAME} KEPT" in proc.stdout, (
        f"the checker passed without evaluating the contract:\n{proc.stdout}"
    )


# --------------------------------------------------------------------------
# The samples must not poison the gate they exist to prove
# --------------------------------------------------------------------------


def test_the_main_gate_does_not_lint_the_bad_samples() -> None:
    """T-01-28: an unexcluded bad sample makes the whole gate permanently red.

    The cheapest way to green a build broken that way is to delete the samples
    rather than restore the exclusion, which would remove the only evidence
    that any of these gates work. This asserts the exclusion by running the real
    targets over the real tree with the samples present, not by reading a
    configuration key.
    """
    for target, tool in (("lint", "ruff check"), ("typecheck", "mypy")):
        proc = _run(["make", target])
        assert proc.returncode == 0, (
            f"`make {target}` is red with the bad samples present — the "
            f"exclusion in pyproject.toml is not in force.\n{proc.stdout}\n{proc.stderr}"
        )
        # A target that ran nothing would also exit 0. make echoes its recipe,
        # so requiring the tool in the transcript keeps this from passing
        # vacuously if the target is ever gutted.
        assert tool in proc.stdout, (
            f"`make {target}` exited 0 without invoking {tool}:\n{proc.stdout}"
        )


def test_every_sample_declares_its_rule_and_consumer() -> None:
    """Each sample names the rule it trips and the test that consumes it.

    The consumer name is *derived* from the file name rather than free text, so
    a sample cannot drift away from its test while still looking documented.
    """
    module = sys.modules[__name__]
    samples = sorted(p for p in SAMPLES.glob("*.py"))
    assert samples, "no samples found — the meta-verification is testing nothing"

    # "Every file in this directory" is the claim, so the glob above must not be
    # quietly skipping something. A stray file here is either an unconsumed
    # sample or documentation that belongs in this module's docstring.
    stray = sorted(p.name for p in SAMPLES.iterdir() if p.suffix != ".py")
    assert not stray, f"non-sample files in badsamples/: {stray}"

    problems: list[str] = []
    for path in samples:
        stem = path.stem
        expected = (
            f"test_{stem}_is_accepted" if stem.startswith("good_") else f"test_{stem}_is_rejected"
        )
        header = "\n".join(path.read_text(encoding="utf-8").splitlines()[:12])
        reference = f"tests/policy/test_gates_actually_fail.py::{expected}"
        if reference not in header:
            problems.append(f"{path.name}: header does not declare `{reference}`")
        if not hasattr(module, expected):
            problems.append(f"{path.name}: no test named {expected} in this module")
        if not re.search(r"^#\s*(Trips|Proves):", header, flags=re.MULTILINE):
            problems.append(f"{path.name}: header has no `Trips:`/`Proves:` line naming a rule")

    assert not problems, "bad-sample headers are out of step:\n" + "\n".join(problems)
