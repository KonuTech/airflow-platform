"""INFRA-07, stated in the form that is actually decidable.

**Honest limit.** INFRA-07's general claim — "no step ever requires manual
intervention" — is undecidable: it is a claim about the operator's own
behaviour, not about this repository's text. What this file asserts instead
is the stronger STRUCTURAL claim that no script in this repository mutates
live cluster state except by applying a committed file under `kubernetes/`,
piping a Secret manifest it assembles itself to `kubectl apply -f -` on
stdin, or waiting on a condition. That claim is decidable by reading the
scripts, and it is what actually backs "all infrastructure is defined as
code" (DoD 7).

## The permitted set, and why each member is in it

- **`kubectl get`** — read-only. Cannot mutate cluster state by definition.
  Every script here uses it only to test whether a Secret already exists
  (`_secret_exists`) before deciding whether to create one.
- **`kubectl wait`** — 02-RESEARCH.md Pattern 3, structurally necessary: Helm
  4's `--wait` defaults to `hookOnly` and does not block on CRD
  establishment, the CNPG admission webhook (measured: `helm upgrade
  --install cnpg ...` without an explicit wait returned in 1.0s with the
  operator not yet serving, and a `Cluster` CR applied immediately after
  failed with `dial tcp ...: connect: connection refused`), or a `Cluster`
  CR's own `Ready` condition. `scripts/wait-for.sh` is these four waits in
  one place, exactly as `wait_for_crd_established` /
  `wait_for_deploy_available` / `wait_for_cnpg_cluster_ready` /
  `wait_for_statefulset_ready`.
- **`kubectl apply -f <committed path under kubernetes/>`** — the plan's own
  wording. `scripts/stages/20-namespaces.sh`'s own header comment calls
  itself "the ONE permitted `kubectl apply`" naming a committed file:
  namespaces are owned by `kubernetes/namespaces.yaml` alone, so no two Helm
  releases ever fight over the same object — true when it was written, and
  still true of *namespace ownership specifically*. Plan 04-02 adds a second,
  equally narrow instance for a different owned object:
  `scripts/stages/75-etl.sh` applies `kubernetes/rbac-etl.yaml` the same way,
  for the same reason (RBAC objects owned by one committed manifest, not a
  Helm release).
- **`kubectl apply -f -` (stdin)** — a DELIBERATE WIDENING of the plan's
  literal "committed path" wording, recorded here rather than silently
  applied. D-14 requires MinIO's and Airflow's derived credentials to be
  generated during `cluster-up` and to live ONLY in the cluster — by
  definition they can never be a committed path, and 02-RESEARCH.md Pattern 4
  names `scripts/airflow-metadata-secret.sh` as "the one hand-written glue
  piece of the phase" for exactly this reason. `scripts/minio-credentials.sh`
  and `scripts/airflow-metadata-secret.sh` both build a Secret manifest
  in-memory (the whole document is visible in the script text, never fetched
  from an external or untrusted source) and pipe it to `kubectl apply -f -`
  on stdin — this is the D-14 credential-materialisation pattern, not
  imperative surgery, and treating it as a violation would fail this test
  against the very scripts the plan cites in its own read_first list. Every
  OTHER `-f` target must still be a committed `kubernetes/` path.
- **`kubectl exec -i`** (stdin) — a SECOND deliberate widening (plan 04-02),
  for a mutation with no Kubernetes-object shape to express as "apply a
  manifest" at all: `scripts/etl-secrets.sh` sets `etl_app`'s PostgreSQL
  password by piping `ALTER ROLE ... WITH PASSWORD '...'` on stdin into a
  `psql` session opened via `kubectl exec -i` against the CNPG analytical
  primary pod, authenticating under PostgreSQL's own peer/local trust (the
  pod's local socket, not a network connection). There is no committed
  manifest that could express "set this role's password", and no path from
  the host reaches the analytical cluster's network listener without itself
  requiring a `kubectl port-forward` — equally outside the permitted set, and
  no improvement on the argv-safety this rule exists to protect (T-02-23/
  T-04-09: a credential in argv is visible in `ps`/`/proc/<pid>/cmdline`).
  Requiring the literal `-i` token is the enforcement mechanism, exactly
  parallel to requiring `-f -` above: it is what proves the payload travels
  by stdin, never as a `psql -c '...'` argument. A bare `kubectl exec` (no
  `-i`) is still reported — it cannot prove its payload avoided argv.

Everything else — `create`, `edit`, `patch`, `delete`, `replace`, `exec`
(without `-i`), `cp`, `label`, `annotate`, `set`, `scale`, `rollout`,
`cordon`, `drain`, `taint`, `port-forward`, or `apply` against anything but
the two `apply` forms above — is reported.

## How the scan finds a real invocation without a full shell parser

Three problems recur when text-scanning shell scripts for `kubectl`
invocations, and each has a concrete instance in this repository's own
scripts (not hypothetical):

1. **Wrapper functions.** `scripts/minio-credentials.sh` and
   `scripts/airflow-metadata-secret.sh` define a local `_kubectl() { ...
   "${kubectl_bin}" ... "$@" ...}` and call it as `_kubectl apply -f -`;
   `scripts/wait-for.sh` defines `_kubectl_wait()` similarly. The actual
   subcommand is a literal word ONLY at the call site, never in the wrapper's
   own body (which forwards `"$@"`). This scan recognises `kubectl` itself
   and this repository's own `_kubectl` / `_kubectl_<suffix>` naming
   convention as command words, and treats a matched word whose next
   argument is `"$@"` (a forwarding definition, not a call) as nothing to
   check — the real subcommand is read from whoever calls the wrapper.
   *Not* recognised: the exported `${KUBECTL}` override variable used
   directly (bypassing every wrapper). Today it appears exactly once, in
   `scripts/doctor.sh`'s own version-compatibility check
   (`"${KUBECTL}" version --client -o json`) — read-only and never a
   mutation. A future direct `"${KUBECTL}" delete ...` would NOT be caught
   by this scan; recorded here as a known gap rather than silently assumed
   away.
2. **Prose that mentions the word.** `scripts/doctor.sh` prints messages like
   `fail "kubectl not found ..."` and `"install kubectl matching Kubernetes
   ..."` — plain English containing the literal word "kubectl" that is never
   a command invocation. Quoted spans are masked to blanks before searching
   for a command word, so a message ABOUT kubectl cannot be mistaken for a
   command invoking it.
3. **Comments explaining the rule.** Every file in this phase quotes
   `kubectl apply -f -` or `kubectl wait` in its own header comment to
   explain why it does what it does. Comment lines are stripped before
   scanning, mirroring `test_ci_invokes_make_only.py`'s identical carve-out.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
SCAN_DIRS = (REPO_ROOT / "scripts", REPO_ROOT / "tools")

# A bare `name() {` or `name()` function-definition line — never itself an
# invocation, even though the name may contain "kubectl" (e.g. `_kubectl()
# {`, `_kubectl_wait() {`).
_FUNCTION_DEFINITION = re.compile(r"^[\w]+\s*\(\)\s*\{?\s*$")

# The command word itself: bare `kubectl`, or this repository's own
# `_kubectl` / `_kubectl_<suffix>` wrapper-call convention. The negative
# lookbehind excludes a lowercase "kubectl" that is itself a parameter-
# expansion DEFAULT VALUE (`${KUBECTL:-kubectl}`, `${kubectl_bin:-kubectl}`)
# rather than a word — those are always immediately preceded by `-` (from
# `:-`) in every real occurrence in this repository.
_COMMAND_WORD = re.compile(r"(?<!-)\b(?:kubectl|_kubectl(?:_\w+)?)\b")

# A quoted span, `"..."` or `'...'` — masked out before searching for a
# command word so human-readable prose inside a message string is never
# mistaken for an invocation.
_QUOTED_SPAN = re.compile(r'"[^"]*"|\'[^\']*\'')

# A shell token: a quoted span (content extracted with its quotes stripped)
# or a run of non-whitespace characters.
_TOKEN = re.compile(r'"[^"]*"|\'[^\']*\'|\S+')

# Flags that consume the following token as their value, so the subcommand
# search can skip past them. `-n`/`--namespace` are always followed by a
# namespace argument in this repository's scripts, never a subcommand.
_FLAG_WITH_VALUE = frozenset({"--context", "-n", "--namespace"})

_PERMITTED_READ_ONLY_SUBCOMMANDS = frozenset({"get", "wait"})

# A `kubectl apply -f <target>` argument is permitted when it is `-` (stdin
# — the D-14 credential-materialisation pattern, see module docstring) or
# names a path with a `kubernetes/` path segment (a committed manifest).
_COMMITTED_KUBERNETES_PATH = re.compile(r"(^|/)kubernetes/[^/]")


def _mask_quoted(text: str) -> str:
    return _QUOTED_SPAN.sub(lambda m: " " * len(m.group(0)), text)


def _raw_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for match in _TOKEN.finditer(text):
        raw = match.group(0)
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ('"', "'"):
            raw = raw[1:-1]
        tokens.append(raw)
    return tokens


def kubectl_invocation(line: str) -> tuple[str, str | None, bool] | None:
    """Return (subcommand, apply_target, has_dash_i) for the first kubectl-ish call on `line`.

    `apply_target` is the raw text following `-f` when the subcommand is
    `apply`, else None. `has_dash_i` is True when a bare `-i` token appears
    anywhere after the subcommand — the stdin-transport marker `kubectl exec
    -i` needs (see the module docstring's second widening and
    `_PERMITTED` handling in `surgery_problems`). Returns None when `line`
    contains no determinable invocation — either because there is none, or
    because the match is a wrapper-function DEFINITION (its own subcommand is
    `"$@"`, forwarded from whoever calls it, not a literal word on this line).
    """
    stripped = line.strip()
    if not stripped or stripped.startswith("#"):
        return None
    if _FUNCTION_DEFINITION.match(stripped):
        return None

    masked = _mask_quoted(stripped)
    match = _COMMAND_WORD.search(masked)
    if match is None:
        return None

    tokens = _raw_tokens(stripped[match.end() :])
    index = 0
    while index < len(tokens) and tokens[index] in _FLAG_WITH_VALUE:
        index += 2
    if index >= len(tokens):
        return None

    subcommand = tokens[index]
    if subcommand == "$@" or subcommand.startswith("-"):
        return None  # a wrapper definition forwarding "$@", not a call site

    apply_target: str | None = None
    if subcommand == "apply":
        for i in range(index + 1, len(tokens) - 1):
            if tokens[i] == "-f":
                apply_target = tokens[i + 1]
                break

    has_dash_i = "-i" in tokens[index + 1 :]

    return subcommand, apply_target, has_dash_i


def _is_permitted_apply(apply_target: str | None) -> bool:
    if apply_target is None:
        return False
    if apply_target == "-":
        return True
    return bool(_COMMITTED_KUBERNETES_PATH.search(apply_target))


def surgery_problems(text: str, label: str) -> list[str]:
    """Report every imperative kubectl subcommand in `text` outside the permitted set."""
    problems: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        result = kubectl_invocation(line)
        if result is None:
            continue
        subcommand, apply_target, has_dash_i = result
        if subcommand in _PERMITTED_READ_ONLY_SUBCOMMANDS:
            continue
        if subcommand == "apply":
            if _is_permitted_apply(apply_target):
                continue
            problems.append(
                f"{label}:{lineno}: kubectl apply -f {apply_target!r} — not a "
                "committed path under kubernetes/, and not stdin ('-')",
            )
            continue
        if subcommand == "exec":
            if has_dash_i:
                continue
            problems.append(
                f"{label}:{lineno}: kubectl exec without -i — a value-bearing "
                "command must move its payload via stdin, never argv "
                "(T-02-23/T-04-09); see module docstring's kubectl-exec-stdin exception",
            )
            continue
        problems.append(
            f"{label}:{lineno}: kubectl {subcommand} — imperative cluster "
            "mutation outside the permitted set (get, wait, apply -f "
            "<kubernetes/...|->, exec -i)",
        )
    return problems


def _scan_paths() -> list[Path]:
    paths: list[Path] = []
    for directory in SCAN_DIRS:
        paths.extend(sorted(directory.rglob("*.sh")))
    return paths


def test_no_script_performs_manual_kubectl_surgery() -> None:
    problems: list[str] = []
    for path in _scan_paths():
        problems += surgery_problems(
            path.read_text(encoding="utf-8"),
            str(path.relative_to(REPO_ROOT)),
        )
    assert not problems, (
        "INFRA-07: no script may mutate cluster state except by applying a "
        "committed kubernetes/ manifest, piping a self-assembled Secret to "
        "stdin, or waiting on a condition:\n" + "\n".join(problems)
    )


def test_wait_and_committed_apply_are_not_reported() -> None:
    committed_target = "${repo_root}/kubernetes/namespaces.yaml"
    text = (
        'kubectl --context "${KUBECTL_CONTEXT}" wait --for=condition=Ready pod/x\n'
        f'kubectl --context "${{KUBECTL_CONTEXT}}" apply -f "{committed_target}"\n'
        "_kubectl_wait wait --for=condition=established crd/foo\n"
        "_kubectl get secret -n data minio-root\n"
    )
    assert not surgery_problems(text, "scratch"), "a permitted invocation was reported"


def test_stdin_apply_is_not_reported() -> None:
    """The deliberate D-14 widening (module docstring) must actually take effect."""
    text = "} | _kubectl apply -f - >/dev/null"
    assert not surgery_problems(text, "scratch"), (
        "kubectl apply -f - (stdin) — the D-14 credential-materialisation pattern — was reported"
    )


def test_an_imperative_mutation_is_reported() -> None:
    """Non-vacuity: injecting a real mutating subcommand must be caught."""
    for injected in (
        'kubectl --context "${KUBECTL_CONTEXT}" delete pod doomed',
        "kubectl patch deployment foo -p '{}'",
        "kubectl edit configmap foo",
        "kubectl create secret generic foo",
        "kubectl label node foo bar=baz",
        "_kubectl apply -f /tmp/not-committed.yaml",
        "kubectl port-forward svc/analytics-db-rw 5432:5432",
        # exec WITHOUT -i cannot prove its payload avoided argv (e.g. a
        # `psql -c '...'` argument would leak into `ps`) — still forbidden.
        "_kubectl exec -n data analytics-db-1 -- psql -U postgres -c 'select 1'",
    ):
        problems = surgery_problems(injected, "scratch")
        assert problems, f"an imperative `{injected}` was not reported"


def test_stdin_exec_is_not_reported() -> None:
    """The deliberate 04-02 widening (module docstring): kubectl exec -i, stdin only."""
    text = "_kubectl exec -i -n data analytics-db-1 -- psql -U postgres -d analytics\n"
    assert not surgery_problems(text, "scratch"), (
        "kubectl exec -i (stdin) — the 04-02 password-derivation pattern — was reported"
    )


def test_a_comment_explaining_the_rule_is_not_reported() -> None:
    """The false-positive control this module's own docstring predicts.

    A comment quoting `kubectl apply -f -` or `kubectl delete` to EXPLAIN a
    rule must not itself be read as a violation of that rule.
    """
    text = (
        "# NEVER run `kubectl delete` by hand outside a script — every "
        "mutation must be committed.\n"
        "# This script pipes its Secret to `kubectl apply -f -` on stdin.\n"
    )
    assert not surgery_problems(text, "scratch"), "a comment was mistaken for an invocation"


def test_prose_mentioning_kubectl_is_not_reported() -> None:
    """The false-positive control for scripts/doctor.sh's own error messages."""
    text = (
        'fail "kubectl not found (KUBECTL=${KUBECTL})" "install kubectl matching Kubernetes 1.35.5"'
    )
    assert not surgery_problems(text, "scratch"), "a quoted message was mistaken for an invocation"


def test_a_wrapper_function_definition_is_not_reported() -> None:
    """The false-positive control for this repository's own `_kubectl*` wrappers."""
    text = (
        "_kubectl_wait() {\n"
        '  if [ -n "${KUBECTL_CONTEXT:-}" ]; then\n'
        '    kubectl --context "${KUBECTL_CONTEXT}" "$@"\n'
        "  else\n"
        '    kubectl "$@"\n'
        "  fi\n"
        "}\n"
    )
    assert not surgery_problems(text, "scratch"), (
        "a wrapper function definition was mistaken for a call"
    )


def test_the_real_scripts_produce_no_messages() -> None:
    """The false-positive control paired with test_an_imperative_mutation_is_reported."""
    problems: list[str] = []
    for path in _scan_paths():
        problems += surgery_problems(path.read_text(encoding="utf-8"), str(path))
    assert not problems, problems
