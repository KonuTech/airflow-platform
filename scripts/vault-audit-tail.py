#!/usr/bin/env python3
r"""scripts/vault-audit-tail.py -- D-04: human-readable Vault audit log tail.

`make vault-audit-tail` runs this. Reads the last `--lines` (default 200)
entries of Vault's PERSISTENT audit log file inside `vault-0`
(`/vault/audit/audit.log`, the `file` audit device `scripts/vault-bootstrap.py`
enables) via `kubectl exec -i -n vault vault-0 -- tail -n <N> ...` -- the
same stdin-transport, persistent-file mechanism
`tests/e2e/vault/test_audit_log.py` reads through (the SEC-08 live proof
this tool's own output backs). Deliberately never `kubectl logs`: `logs` is
NOT in `tests/policy/test_no_manual_kubectl_surgery.py`'s permitted
read-only subcommand set (`get`, `wait` only) -- 05-RESEARCH.md's own
Common Pitfalls section documents this exact trap. Because this script is
`.py`, not `.sh`, it sits outside that policy test's scanned `SCAN_DIRS`
regardless, but it follows the `exec -i`/persistent-file shape anyway, both
because it is the correct, durable-across-restart design and to keep this
codebase's actual behaviour consistent with its own documented discipline
even where a specific mechanical check does not reach (05-04-PLAN.md's own
Interfaces section).

Prints one compact, human-readable line per audit entry: timestamp,
request path, the calling identity (`auth.metadata.service_account_name`/
`service_account_namespace`, when present -- else `(unauthenticated)`, the
state before a Kubernetes-auth login succeeds), and outcome (`ok`, or
Vault's own error text). Deliberately not raw JSON: this is human-facing
developer tooling, matching the developer-experience bar Phase 4 already
set with `make ingest-demo` (D-04) -- `scripts/ingest-demo.py`'s own
`_print_receipt()` is the exact "deliberately not raw JSON, human-facing"
convention this follows.

**SEC-08 / T-05-11 (this phase's own threat register):** this tool NEVER
reads or prints Vault's `request`/`response` BODIES -- only `time`,
`request.path`, `auth.metadata.*` and the top-level `error` field are ever
touched. Vault HMAC-SHA256-hashes sensitive string values in
`request`/`response` by default (`hmac-sha256:...`, verified live and via
official docs); the safest way to guarantee this tool never mis-renders or
"helpfully" decodes one is to never look at those fields AT ALL, not to
pattern-match for the prefix and hope every case is covered.

A malformed (non-JSON) line is skipped with a warning to stderr -- this
tool never crashes over one bad line, since the audit log is Vault's own
append-only, external artifact this script does not control.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
_VERSIONS_ENV = _REPO_ROOT / "helm" / "versions.env"

_VAULT_NAMESPACE = "vault"
_VAULT_POD = "vault-0"
_AUDIT_LOG_PATH = "/vault/audit/audit.log"

_DEFAULT_LINES = 200


def _versions_env_variable(name: str) -> str:
    """Read a `KEY=value` line from `helm/versions.env` (the single source, plan 02-01).

    Args:
        name: The variable name to look up.

    Returns:
        The variable's value, with surrounding whitespace stripped.

    Raises:
        RuntimeError: `name` is not defined in `helm/versions.env`.
    """
    text = _VERSIONS_ENV.read_text(encoding="utf-8")
    for line in text.splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    msg = f"helm/versions.env does not define {name}"
    raise RuntimeError(msg)


def _kubectl_context() -> str:
    """Return the kubectl context kind registers for this cluster: `kind-<name>`.

    Same convention as `scripts/vault-unseal.py`'s `_kubectl_context` --
    never the ambient current-context.

    Returns:
        The `kind-<CLUSTER_NAME>` context string.
    """
    return f"kind-{_versions_env_variable('CLUSTER_NAME')}"


def _require_kubectl() -> str:
    """Resolve the absolute path to the `kubectl` binary on `PATH`.

    Returns:
        The absolute path to `kubectl`.

    Raises:
        RuntimeError: `kubectl` is not found on `PATH`.
    """
    kubectl_bin = shutil.which("kubectl")
    if kubectl_bin is None:
        msg = "kubectl not found on PATH"
        raise RuntimeError(msg)
    return kubectl_bin


def _tail_audit_log(kubectl_context: str, lines: int) -> str:
    """Read the last `lines` lines of the persistent audit log inside `vault-0`.

    Uses `kubectl exec -i` (stdin transport) against the persistent audit
    log FILE, never `kubectl logs` -- see module docstring.

    Args:
        kubectl_context: The kubectl context to exec through.
        lines: How many trailing lines of the audit log to read.

    Returns:
        The raw stdout text -- one JSON object per line.

    Raises:
        RuntimeError: The `kubectl exec` call fails (e.g. `vault-0` is not
            running, or the audit log file does not exist yet).
    """
    kubectl_bin = _require_kubectl()
    proc = subprocess.run(  # noqa: S603
        [
            kubectl_bin,
            "--context",
            kubectl_context,
            "exec",
            "-i",
            "-n",
            _VAULT_NAMESPACE,
            _VAULT_POD,
            "--",
            "tail",
            "-n",
            str(lines),
            _AUDIT_LOG_PATH,
        ],
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    if proc.returncode != 0:
        msg = (
            f"kubectl exec -i -n {_VAULT_NAMESPACE} {_VAULT_POD} -- tail -n {lines} "
            f"{_AUDIT_LOG_PATH} failed (exit {proc.returncode}):\n{proc.stderr}"
        )
        raise RuntimeError(msg)
    return proc.stdout


def _format_entry(entry: dict[str, Any]) -> str:
    """Render one parsed audit-log entry as a compact, human-readable line.

    Only ever reads `time`, `request.path`, `auth.metadata.*` and the
    top-level `error` field -- see module docstring's SEC-08/T-05-11 note
    on why `request`/`response` bodies are never touched at all.

    Args:
        entry: One `json.loads`-parsed audit-log line.

    Returns:
        A single formatted line, no trailing newline.
    """
    timestamp = entry.get("time", "?")
    path = entry.get("request", {}).get("path", "?")

    metadata = entry.get("auth", {}).get("metadata") or {}
    sa_name = metadata.get("service_account_name")
    sa_namespace = metadata.get("service_account_namespace")
    if sa_name and sa_namespace:
        identity = f"{sa_namespace}:{sa_name}"
    elif sa_name:
        identity = sa_name
    else:
        identity = "(unauthenticated)"

    error = entry.get("error")
    outcome = "ok" if not error else str(error)

    return f"{timestamp}  {path:<40}  {identity:<40}  {outcome}"


def render(raw_text: str) -> list[str]:
    """Parse `raw_text` (one JSON object per line) into formatted output lines.

    A line that fails to parse as JSON is skipped, with a warning printed
    to stderr -- never crashes the whole tool over one malformed line
    (module docstring).

    Args:
        raw_text: The raw `tail` stdout -- one JSON object per line.

    Returns:
        One formatted line (`_format_entry`) per successfully parsed
        audit-log entry, in the same order as `raw_text`.
    """
    formatted: list[str] = []
    for lineno, line in enumerate(raw_text.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError as exc:
            print(f"WARNING: skipping unparseable audit-log line {lineno}: {exc}", file=sys.stderr)
            continue
        formatted.append(_format_entry(entry))
    return formatted


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse this script's command-line arguments.

    Args:
        argv: Argument list to parse. `None` (the default) parses
            `sys.argv[1:]`.

    Returns:
        The parsed namespace, with a `lines: int` attribute.
    """
    parser = argparse.ArgumentParser(
        description="D-04: tail Vault's persistent audit log and render it human-readably.",
    )
    parser.add_argument(
        "--lines",
        type=int,
        default=_DEFAULT_LINES,
        help=f"Number of trailing audit-log lines to read (default: {_DEFAULT_LINES}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Tail and render Vault's audit log.

    Args:
        argv: Argument list to parse. `None` (the default) parses
            `sys.argv[1:]`.

    Returns:
        `0` on success; `1` if the `kubectl exec` call fails.
    """
    args = _parse_args(argv)
    kubectl_context = _kubectl_context()
    try:
        raw_text = _tail_audit_log(kubectl_context, args.lines)
    except RuntimeError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    lines = render(raw_text)
    if not lines:
        print("No audit-log entries found.")
        return 0

    print(f"--- Vault audit log (last {len(lines)} entries) ---")
    for line in lines:
        print(line)
    return 0


if __name__ == "__main__":
    sys.exit(main())
