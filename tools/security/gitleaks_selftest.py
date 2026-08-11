"""SEC-11 negative proof: assert the configured secret scanner fails a build.

A scanner nobody has ever seen fail is indistinguishable from a disabled one.
This module makes the distinction observable: it builds a disposable git
repository in a temporary directory, commits credential-shaped canaries into
*it*, runs the project's own ``.gitleaks.toml`` against it, and asserts that
gitleaks exits non-zero and names the rules it was expected to name.

Doing this literally — committing a canary to the real repository — would
permanently trip every future full-history scan and force a global allowlist,
which is the anti-pattern the allowlists in ``.gitleaks.toml`` exist to avoid.
Nothing here is ever written to this repository's history or working tree.

Three properties are asserted, and the third is the important one:

1. The scanner exits 1 on credential-shaped values and reports the expected
   rule identifiers, so a ruleset change that stops matching is visible rather
   than silent.
2. A synthetic-prefixed value under ``tests/fixtures/`` is NOT reported.
3. The *byte-identical* line under ``packages/`` IS reported. Path is the only
   variable between (2) and (3), which is what proves the allowlists are
   conjunction-scoped rather than repository-wide. 01-RESEARCH.md records this
   as assumption A1 at HIGH risk: the ``condition = "AND"`` semantics were read
   from the gitleaks schema, never exercised. This assertion is where the claim
   becomes evidence.

The canary values are derived at runtime rather than written as literals, for
two reasons. A credential-shaped literal committed here would be found by this
repository's own ``make gitleaks`` — the control would fire on the file that
proves the control works. And vendor-documented example credentials cannot be
used at all: 01-RESEARCH.md Pitfall 3 records, by execution, that gitleaks
8.30.1 stopword-lists AWS's published example access-key id and its paired
example secret, reporting clean and exit 0, so a self-test built on one asserts
the exact opposite of what it claims. (That example key is deliberately not
quoted anywhere in this file, so a policy test can assert its absence by
grep.) Derivation is deterministic (README §67): the same canaries are
produced on every run and on every machine.

Invoked by ``make gitleaks-selftest`` and by the ``Secret scan (full history)``
CI job.
"""

from __future__ import annotations

import hashlib
import json
import logging
import string
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Final

_LOG: Final = logging.getLogger("gitleaks_selftest")

REPO_ROOT: Final = Path(__file__).resolve().parents[2]
CONFIG_PATH: Final = REPO_ROOT / ".gitleaks.toml"
DEFAULT_BINARY: Final = REPO_ROOT / "tools" / "bin" / "gitleaks"

# Namespace for canary derivation. Changing it changes every canary, which is
# harmless — the assertions are about rule identifiers and paths, never values.
_CANARY_NAMESPACE: Final = "airflow-platform/gitleaks-selftest/v1"

_UPPER_ALNUM: Final = string.ascii_uppercase + string.digits
_MIXED_ALNUM: Final = string.ascii_letters + string.digits
_DIGITS: Final = string.digits

# Rules the canaries below must trip. Asserted as a subset of what gitleaks
# reports: an upstream ruleset that grows new detections is fine, one that
# stops detecting these is a regression that must fail this self-test.
EXPECTED_RULE_IDS: Final = frozenset(
    {"aws-access-token", "github-pat", "slack-bot-token"},
)

# Paths inside the disposable repository.
FIXTURE_PROBE: Final = "tests/fixtures/csv/70_synthetic_credentials.csv"
PACKAGE_PROBE: Final = "packages/dataplat/src/dataplat/leak_probe.py"
CANARY_FILE: Final = "packages/dataplat/src/dataplat/credentials.py"


class SelfTestError(RuntimeError):
    """Raised when the scanner does not behave as SEC-11 requires."""


def _derive(tag: str, length: int, alphabet: str) -> str:
    """Derive a deterministic, randomised-looking string of a given length.

    Args:
        tag: Distinguishes one canary from another within the namespace.
        length: Number of characters to produce.
        alphabet: Characters the result may contain.

    Returns:
        A reproducible pseudo-random string. It is not a credential and never
        was one; it only has to *look* like one to the scanner's regexes.
    """
    chars: list[str] = []
    counter = 0
    while len(chars) < length:
        seed = f"{_CANARY_NAMESPACE}|{tag}|{counter}".encode()
        digest = hashlib.sha256(seed).digest()
        chars.extend(alphabet[byte % len(alphabet)] for byte in digest)
        counter += 1
    return "".join(chars[:length])


def build_canaries() -> dict[str, str]:
    """Build the credential-shaped canary values.

    Returns:
        Mapping of canary name to value. Prefixes are literals because a
        prefix alone matches no rule; only the assembled value does.
    """
    # The Slack shape is structural, not decorative: the rule is
    # `xoxb-[0-9]{10,13}-[0-9]{10,13}[a-zA-Z0-9-]*` with an entropy floor, so a
    # flat alphanumeric tail after the prefix matches nothing. Verified by
    # execution — the first run of this self-test reported the AWS and GitHub
    # canaries and stayed silent on a flat Slack one.
    slack_workspace = _derive("slack-workspace", 12, _DIGITS)
    slack_channel = _derive("slack-channel", 12, _DIGITS)
    return {
        "aws": "AKIA" + _derive("aws", 16, _UPPER_ALNUM),
        "github": "ghp_" + _derive("github", 36, _MIXED_ALNUM),
        "slack": f"xoxb-{slack_workspace}-{slack_channel}-" + _derive("slack", 24, _MIXED_ALNUM),
        # The synthetic-corpus value. The generator emits the SYNTH_ prefix by
        # construction, and `.gitleaks.toml` allowlists it only under
        # tests/fixtures/.
        "synthetic": "SYNTH_" + _derive("synthetic", 34, _MIXED_ALNUM),
    }


def _git(repo: Path, *args: str) -> None:
    """Run a git command inside the disposable repository.

    Args:
        repo: Working directory of the disposable repository.
        *args: Arguments passed to git.

    Raises:
        SelfTestError: If git exits non-zero.
    """
    # Identity and signing are forced on the command line so the self-test does
    # not depend on — or disturb — the developer's global git configuration.
    command = [
        "git",
        "-c",
        "user.name=gitleaks selftest",
        "-c",
        "user.email=selftest@localhost",
        "-c",
        "commit.gpgsign=false",
        *args,
    ]
    result = subprocess.run(  # noqa: S603  # fixed argv, no shell, no user input
        command,
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        msg = f"git {' '.join(args)} failed in the disposable repository: {result.stderr}"
        raise SelfTestError(msg)


def _write(repo: Path, relative: str, content: str) -> None:
    """Write a file inside the disposable repository, creating parents."""
    target = repo / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def populate(repo: Path, canaries: dict[str, str]) -> None:
    """Create and commit the disposable repository's contents.

    Args:
        repo: An empty directory that becomes a git repository.
        canaries: Values from :func:`build_canaries`.
    """
    _git(repo, "init", "--quiet", "--initial-branch=main", ".")

    _write(
        repo,
        CANARY_FILE,
        "# Canary module. Every value here is derived, not real.\n"
        f'AWS_ACCESS_KEY_ID = "{canaries["aws"]}"\n'
        f'GITHUB_TOKEN = "{canaries["github"]}"\n'
        f'SLACK_BOT_TOKEN = "{canaries["slack"]}"\n',
    )

    # The AND-scoping experiment. Both files carry the SAME probe line, so path
    # is the only difference between "silenced" and "reported".
    probe_line = f'api_key = "{canaries["synthetic"]}"\n'
    _write(repo, FIXTURE_PROBE, probe_line)
    _write(repo, PACKAGE_PROBE, probe_line)

    _git(repo, "add", "-A")
    _git(repo, "commit", "--quiet", "-m", "canaries")


def run_scanner(binary: Path, repo: Path, report: Path) -> tuple[int, str]:
    """Run gitleaks over the disposable repository using the project config.

    Args:
        binary: Path to the pinned gitleaks binary.
        repo: The disposable repository to scan.
        report: Where the JSON report is written.

    Returns:
        The scanner's exit code and its combined stdout/stderr.
    """
    command = [
        str(binary),
        "git",
        "--log-opts=--all",
        "--config",
        str(CONFIG_PATH),
        # SEC-10: never let a finding reach a log with its value intact. This
        # also makes the "no unredacted value in output" assertion meaningful.
        "--redact",
        "--no-banner",
        "--exit-code",
        "1",
        "--report-format",
        "json",
        "--report-path",
        str(report),
        str(repo),
    ]
    result = subprocess.run(  # noqa: S603  # fixed argv, no shell, no user input
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, result.stdout + result.stderr


def _load_findings(report: Path) -> list[dict[str, Any]]:
    """Read the JSON report, tolerating the empty-report case."""
    if not report.exists():
        return []
    raw = report.read_text(encoding="utf-8").strip()
    if not raw:
        return []
    findings: list[dict[str, Any]] = json.loads(raw)
    return findings


def assert_scanner_fails(exit_code: int, findings: list[dict[str, Any]]) -> None:
    """Assert the scanner failed the build and named the expected rules.

    Args:
        exit_code: Exit status returned by gitleaks.
        findings: Parsed JSON report entries.

    Raises:
        SelfTestError: If the scanner passed, or reported unexpected rules.
    """
    if exit_code != 1:
        msg = (
            f"the scanner exited {exit_code} on a repository full of credentials; "
            "SEC-11 requires exit 1. A scanner that cannot fail is a disabled scanner."
        )
        raise SelfTestError(msg)

    observed = {str(finding["RuleID"]) for finding in findings}
    missing = EXPECTED_RULE_IDS - observed
    if missing:
        msg = (
            f"the scanner exited 1 but did not report {sorted(missing)}. "
            f"Observed rules: {sorted(observed)}. The ruleset no longer detects a "
            "credential shape this project relies on it to detect."
        )
        raise SelfTestError(msg)

    _LOG.info("the scanner exited 1 on the disposable repository")
    _LOG.info("reported rules: %s", ", ".join(sorted(observed)))


def assert_allowlist_is_conjunction_scoped(findings: list[dict[str, Any]]) -> None:
    """Assert the SYNTH_ allowlist is path-scoped, not repository-wide.

    Both probe files hold a byte-identical line. The fixture copy must be
    silenced and the packages copy must be reported; anything else means
    ``condition = "AND"`` is not behaving as the schema documents and the
    allowlist is silencing the prefix everywhere (01-RESEARCH.md A1).

    Args:
        findings: Parsed JSON report entries.

    Raises:
        SelfTestError: If either half of the experiment fails.
    """
    reported_files = {str(finding["File"]) for finding in findings}

    if PACKAGE_PROBE not in reported_files:
        msg = (
            f"a synthetic-prefixed value at {PACKAGE_PROBE} was NOT reported. Either the "
            'allowlist is repository-wide (condition = "AND" not honoured, so the '
            "SYNTH_ prefix silences the whole repository), or the probe line no longer "
            f"matches any rule. Reported files: {sorted(reported_files)}"
        )
        raise SelfTestError(msg)

    silenced = [path for path in reported_files if path.startswith("tests/fixtures/")]
    if silenced:
        msg = (
            f"the synthetic corpus was reported: {sorted(silenced)}. The fixture "
            "allowlist is not taking effect, and the corpus would make the scanner "
            "unusably noisy — the pressure that leads someone to disable it."
        )
        raise SelfTestError(msg)

    _LOG.info("synthetic value at %s: reported (allowlist is path-scoped)", PACKAGE_PROBE)
    _LOG.info("byte-identical value at %s: silenced", FIXTURE_PROBE)


def assert_output_is_redacted(output: str, canaries: dict[str, str]) -> None:
    """Assert no canary value appears in the scanner's output.

    Args:
        output: Combined stdout and stderr from the scanner.
        canaries: The values that must not appear.

    Raises:
        SelfTestError: If any value leaked into the output.
    """
    leaked = sorted(name for name, value in canaries.items() if value in output)
    if leaked:
        msg = (
            f"the scanner printed the value of {leaked} in plain text. Every "
            "invocation must carry --redact (SEC-10); a CI log is readable by "
            "everyone with repository access and is retained."
        )
        raise SelfTestError(msg)

    _LOG.info("no canary value appeared in the scanner's output")


def selftest(binary: Path) -> None:
    """Run the whole negative proof against a disposable repository.

    Args:
        binary: Path to the pinned gitleaks binary.

    Raises:
        SelfTestError: If any assertion fails.
    """
    if not binary.is_file():
        msg = (
            f"gitleaks not found at {binary}. Run tools/security/install_gitleaks.sh "
            "first; it verifies the published checksum before installing."
        )
        raise SelfTestError(msg)
    if not CONFIG_PATH.is_file():
        msg = f"the project's scanner configuration is missing: {CONFIG_PATH}"
        raise SelfTestError(msg)

    canaries = build_canaries()

    # A temporary directory, never a path inside this repository: the real
    # history and working tree must be untouched whether this passes or fails.
    with tempfile.TemporaryDirectory(prefix="gitleaks-selftest-") as tmp:
        workdir = Path(tmp)
        repo = workdir / "disposable"
        repo.mkdir()
        report = workdir / "report.json"

        populate(repo, canaries)
        exit_code, output = run_scanner(binary, repo, report)
        findings = _load_findings(report)

        assert_scanner_fails(exit_code, findings)
        assert_allowlist_is_conjunction_scoped(findings)
        assert_output_is_redacted(output, canaries)


def main(argv: list[str]) -> int:
    """Entry point for ``python -m tools.security.gitleaks_selftest``.

    Args:
        argv: Command-line arguments; an optional path to the gitleaks binary.

    Returns:
        0 when the scanner behaved as SEC-11 requires, 1 otherwise.
    """
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    binary = Path(argv[0]).resolve() if argv else DEFAULT_BINARY

    try:
        selftest(binary)
    except SelfTestError as error:
        _LOG.error("SEC-11 self-test FAILED: %s", error)
        return 1

    _LOG.info("SEC-11 self-test passed: the scanner is live and its allowlists are scoped")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
