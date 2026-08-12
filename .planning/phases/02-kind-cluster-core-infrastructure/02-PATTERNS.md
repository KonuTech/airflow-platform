# Phase 2: kind Cluster & Core Infrastructure - Pattern Map

**Mapped:** 2026-08-12
**Files analyzed:** 38 new/modified files
**Analogs found:** 17 / 38 (21 have no analog — this repo contains no YAML infrastructure, no shell beyond one installer, and no live-cluster test tier)

> **Read this first.** Phase 2 fills directories that today hold only `.gitkeep`. For every
> `kind/`, `helm/values/`, `kubernetes/` and most `scripts/` file the honest answer is
> **no analog exists** — those files must follow `02-RESEARCH.md` § Code Examples and the
> verified excerpts there, not a forced match in this repo. The real pattern-copying work is
> concentrated in five places: the **Makefile**, the **pinned-binary installer**, the
> **policy tests**, the **ADR**, and **pyproject/CI wiring**.

---

## File Classification

### Files WITH an analog

| New/Modified File | Role | Data Flow | Closest Analog | Match |
|---|---|---|---|---|
| `Makefile` (targets `doctor`, `cluster-up`, `cluster-down`, `cluster-rebuild`, `cluster-verify`, `minio-creds`, `manifests`, `helm-lint`) | config / gate definition | batch, request-response | `Makefile` (`uv-guard`, `gitleaks`, `check`, `ci`) | exact (same file) |
| `tools/k8s/install_kind.sh` | utility (installer) | file-I/O, network fetch | `tools/security/install_gitleaks.sh` | exact |
| `tools/k8s/install_helm.sh` | utility (installer) | file-I/O, network fetch | `tools/security/install_gitleaks.sh` | exact |
| `tools/k8s/install_kubeconform.sh` | utility (installer) | file-I/O, network fetch | `tools/security/install_gitleaks.sh` | exact |
| `tests/policy/test_kind_cluster_config.py` | test (policy) | transform (parse YAML → assert) | `tests/policy/test_workflow_secrets.py` | exact |
| `tests/policy/test_values_profiles.py` | test (policy) | transform | `tests/policy/test_pinned_tool_versions_agree.py` (multi-source comparison + load-bearing proof) | exact |
| `tests/policy/test_manifest_resources.py` | test (policy) | transform, batch | `tests/policy/test_workflow_secrets.py` (pure predicate + mutation) | exact |
| `tests/policy/test_manifest_validation_fails_closed.py` | test (policy, non-vacuity) | request-response (subprocess) | `tests/policy/test_gates_actually_fail.py` | exact |
| `tests/policy/test_doctor_fails_closed.py` | test (policy, fault injection) | request-response (subprocess) | `tests/policy/test_gates_actually_fail.py` | exact |
| `tests/policy/test_no_manual_kubectl_surgery.py` | test (policy) | transform (grep scripts) | `tests/policy/test_ci_invokes_make_only.py` | exact |
| `tests/policy/test_workflow_secrets.py` (**modify**: widen scope to `helm/`, `kubernetes/`, `kind/`, `scripts/`) | test (policy) | transform | itself | exact |
| `tests/policy/test_pinned_tool_versions_agree.py` (**modify**: add kind/helm/kubeconform readings) | test (policy) | transform | itself | exact |
| `tests/e2e/cluster/conftest.py` | test fixture / provider | request-response | `tests/conftest.py` | role-match (no live-service conftest exists) |
| `docs/adr/0006-unmaintained-upstream-artifacts.md` | doc | — | `docs/adr/0000-template.md` + `0005-*.md` | exact |
| `docs/adr/README.md` (**modify**: index row) | doc | — | itself | exact |
| `pyproject.toml` (**modify**: `cluster` marker, `cluster` dependency group) | config | — | existing `[tool.pytest.ini_options]` / `[dependency-groups]` | exact |
| `.github/workflows/ci.yml` (**modify**: `make manifests` reached via `make check`) | config (CI) | — | itself | exact |

### Files with NO analog (see § No Analog Found)

`kind/cluster.yaml`, `kind/cluster-ci.yaml`, `helm/versions.env`,
`helm/values/{local,ci}/{airflow,minio,cnpg-airflow,cnpg-analytics,ingress-nginx}.yaml` (10 files),
`helm/schemas/cnpg/*.json` (generated), `kubernetes/namespaces.yaml`,
`scripts/{doctor,cluster-up,cluster-down,cluster-rebuild,minio-credentials,airflow-metadata-secret,wait-for,render-manifests,vendor-crd-schemas}.sh`,
`docs/wsl/wslconfig.example`,
`tests/e2e/cluster/test_{airflow_workloads,postgres_topology,minio_buckets,ingress,node_capacity}.py`.

---

## Pattern Assignments

### `Makefile` — new infrastructure targets (config, gate definition)

**Analog:** `Makefile` itself. Extend it; do **not** add a second gate mechanism (D-09).

**File-header contract** (lines 1-2) — the reason infra targets must live here:
```make
# The ONLY place a quality gate is defined. CI calls `make install` and
# `make check` and nothing else, so the local gate and the CI gate cannot drift.
```

**Pinned-tool version assertion — the template for `doctor`** (lines 6-7, 30-37):
```make
UV ?= uv
UV_REQUIRED_VERSION := 0.12.3

uv-guard:                      ## Fail if the installed uv is not the pinned version
	@have="$$($(UV) --version 2>/dev/null | head -n1 | awk '{print $$2}')"; \
	if [ "$$have" != "$(UV_REQUIRED_VERSION)" ]; then \
	  echo "ERROR: uv $(UV_REQUIRED_VERSION) is required; found '$${have:-none}' (UV=$(UV))." >&2; \
	  echo "Install it with:" >&2; \
	  echo "  curl -LsSf https://astral.sh/uv/$(UV_REQUIRED_VERSION)/install.sh | sh" >&2; \
	  exit 1; \
	fi
```
Copy this shape verbatim per tool for kind / helm / kubectl / kubeconform in `doctor`
(D-10): read the installed version, compare to a `*_REQUIRED_VERSION := ` constant, print
the **exact remediation command** on stderr, `exit 1`. Note `${have:-none}` — a missing tool
and a wrong tool produce the same fail-closed path with distinguishable output.

**Target declaration + self-documenting help** (lines 24-28):
```make
.PHONY: help uv-guard install lock-check lint format typecheck imports test policy \
        fixtures fixtures-verify gitleaks gitleaks-selftest check ci clean

help:                          ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'
```
Every new target must (a) join `.PHONY`, (b) carry a `## comment` — `help` greps
`^[a-z-]+:.*##`, so `cluster-up`, `cluster-down`, `cluster-rebuild`, `cluster-verify`,
`minio-creds`, `manifests`, `helm-lint`, `doctor` all match the `[a-z-]+` character class.

**Delegating to the pinned installer instead of restating a version** (lines 88-91):
```make
gitleaks:                      ## SEC-02/SEC-11: full history + working tree [plan 01-02]
	@tools/security/install_gitleaks.sh
	./tools/bin/gitleaks git --log-opts="--all" --redact --no-banner --exit-code 1 .
```
`tests/policy/test_pinned_tool_versions_agree.py::test_the_makefile_scanner_target_defers_to_the_pinned_installer`
asserts the target names **no version literal**. Apply the same rule: `doctor` /
`cluster-up` call `tools/k8s/install_{kind,helm,kubeconform}.sh`; version numbers live in
the installer (and `helm/versions.env`), not in recipe bodies.

**Gate composition and the offline contract** (lines 96-102):
```make
# `check` must never need the network: ROADMAP success criterion 4 is a clone
# followed by `uv sync && make check` with no services running. That is why
# `gitleaks` (which needs a downloaded binary) lives in `ci` and not here.
check: uv-guard lock-check lint format typecheck imports policy test fixtures-verify  ## Local gate
ci: check gitleaks gitleaks-selftest                                  ## CI gate (superset)
```
Consequences for this phase, both stated in `02-VALIDATION.md`:
- `manifests` joins `check` **only if it is offline** — `helm template` against pre-fetched
  charts and vendored CRD schemas. If it needs `helm repo add` over the network it belongs
  in `ci`, following the `gitleaks` precedent.
- `cluster-verify` joins **neither** `check` nor `ci`.

**Explicit test-path naming (WINDOWS #8)** (lines 62-73):
```make
test:                          ## unit + regression tests, coverage report, no threshold
	# tests/property, tests/integration and tests/e2e are deliberately NOT
	# here: they are empty today and will need testcontainers or a live
	# cluster. Phase 3 must add them to a target that can provide those, and
	# must not assume `make check` already collects them.
	$(RUN) pytest tests/unit tests/regression -q --cov --cov-report=term-missing
```
`tests/e2e/cluster/` is uncollected until a target names it. Follow `policy:` (lines 75-76)
for the new target shape: `cluster-verify: ; $(RUN) pytest tests/e2e/cluster -q`.

---

### `tools/k8s/install_{kind,helm,kubeconform}.sh` (utility, network fetch → file-I/O)

**Analog:** `tools/security/install_gitleaks.sh` — hardened by review finding CR-03. Copy its
five-stage structure; do not re-invent it.

**Stage 1 — strict mode + the in-repo trust anchor** (lines 22-48):
```bash
set -euo pipefail

GITLEAKS_VERSION="${GITLEAKS_VERSION:-8.30.1}"

# The trust anchor, committed to this repository (CR-03).
# The release's own checksums.txt is fetched from the SAME URL prefix as the
# tarball it describes, so whoever can alter one can alter the other. ...
PINNED_SHA256_linux_x64="551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb"
PINNED_SHA256_linux_arm64="e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080"
PINNED_SHA256_darwin_x64="dfe101a4db2255fc85120ac7f3d25e4342c3c20cf749f2c20a18081af1952709"
PINNED_SHA256_darwin_arm64="b40ab0ae55c505963e365f271a8d3846efbc170aa17f2607f13df610a9aeb6a5"
PINNED_VERSION="8.30.1"
```

**Stage 2 — platform resolution, unsupported platforms refused** (lines 50-65):
```bash
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
dest_dir="${repo_root}/tools/bin"
dest="${dest_dir}/gitleaks"
stamp="${dest_dir}/.gitleaks.stamp"

case "$(uname -s)" in
  Linux)  os="linux" ;;
  Darwin) os="darwin" ;;
  *)      echo "ERROR: unsupported OS '$(uname -s)' for a pinned gitleaks build." >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64|amd64)  arch="x64" ;;
  aarch64|arm64) arch="arm64" ;;
  *)             echo "ERROR: unsupported architecture '$(uname -m)'." >&2; exit 1 ;;
esac
```
> Note the artifact naming differs per tool: kind ships a **bare binary**
> (`kind-linux-amd64`, so `sha256sum` the binary directly — no `tar -xzf` stage), helm and
> kubeconform ship **tarballs** (`helm-v4.2.3-linux-amd64.tar.gz` extracts
> `linux-amd64/helm`). The verify-before-extract ordering matters only for the tarball
> cases; for kind it becomes verify-before-`install`.

**Stage 3 — refuse rather than fall back** (lines 71-87):
```bash
pinned_var="PINNED_SHA256_${os}_${arch}"
pinned="${!pinned_var:-}"
if [ "${GITLEAKS_VERSION}" != "${PINNED_VERSION}" ]; then
  echo "ERROR: GITLEAKS_VERSION is ${GITLEAKS_VERSION} but the pinned digests in this" >&2
  echo "script are for ${PINNED_VERSION}. ..." >&2
  exit 1
fi
if [ -z "${pinned}" ]; then
  echo "ERROR: no pinned SHA-256 for ${os}/${arch}. Refusing to install an" >&2
  echo "unverified scanner binary." >&2
  exit 1
fi
```

**Stage 4 — idempotence by digest, never by executing the binary** (lines 89-100):
```bash
# The check is a digest comparison against the in-repo pin, NOT `"${dest}" version` —
# that executed whatever binary happened to sit in the gitignored tools/bin/ ...
if [ -f "${dest}" ] && [ -f "${stamp}" ]; then
  installed_digest="$(sha256sum "${dest}" | cut -d' ' -f1)"
  if [ "$(cat "${stamp}")" = "${pinned}:${installed_digest}" ]; then
    echo "gitleaks ${GITLEAKS_VERSION} already installed and verified at ${dest}"
    exit 0
  fi
  echo "Reinstalling: ${dest} does not match its recorded verification." >&2
fi
```
This matters more here than for gitleaks: `make cluster-up` will call three installers on
every invocation.

**Stage 5 — verify BEFORE extract, advisory cross-check after** (lines 102-134):
```bash
workdir="$(mktemp -d)"
cleanup() { rm -rf "${workdir}"; }
trap cleanup EXIT

curl -sSLf --retry 3 -o "${workdir}/${tarball}"   "${base_url}/${tarball}"
curl -sSLf --retry 3 -o "${workdir}/${checksums}" "${base_url}/${checksums}"
cd "${workdir}"

# The authoritative check: the bytes received must match the digest committed to
# THIS repository. ... so the origin cannot vouch for itself.
echo "${pinned}  ${tarball}" > expected.sha256
if ! sha256sum -c expected.sha256; then
  echo "ERROR: SHA-256 mismatch for ${tarball}." >&2
  echo "Refusing to extract or install." >&2
  exit 1
fi

# Secondary, advisory only ... runs AFTER the authoritative check and can never substitute for it.
if ! grep -qE "^${pinned}[[:space:]]+\*?${tarball}\$" "${checksums}"; then
  echo "WARNING: the release's checksums.txt does not list the pinned digest for" >&2
  ...
fi
```

**Stage 6 — install + stamp** (lines 136-148):
```bash
tar -xzf "${tarball}" gitleaks
mkdir -p "${dest_dir}"
install -m 0755 gitleaks "${dest}"
printf '%s:%s' "${pinned}" "$(sha256sum "${dest}" | cut -d' ' -f1)" > "${stamp}"
echo "Installed gitleaks ${GITLEAKS_VERSION} (digest verified against in-repo pin) -> ${dest}"
```
`tools/bin/` is gitignored; the three new binaries land there too and must never be committed.

---

### `tests/policy/test_*.py` — static assertions over YAML (test, transform)

Applies to **all six new policy tests** plus the two modified ones.

**Analog A (structure / repo-root resolution / pure predicate + mutation):**
`tests/policy/test_workflow_secrets.py`.

**Module docstring states the honest limit of the claim** (lines 1-35, condensed):
```python
"""SEC-10, stated in the form that is actually decidable.

**The general form of SEC-10 is undecidable, and pretending otherwise would be
the most dangerous thing this file could do.** ...
What *is* decidable, and what this phase actually asserts, is a **stronger
structural claim**: this workflow references no repository secret at all.
"""
```
Every new test owes the same paragraph. Concretely: `test_manifest_resources.py` must state
that the request sum is over *rendered* manifests and that the CNPG `Cluster` CR is
special-cased (RESEARCH Pitfall 6 — a walker over Pod-template kinds sums **zero** for both
databases); `test_no_manual_kubectl_surgery.py` must state that a grep for
`kubectl create/edit/patch` cannot decide the general INFRA-07 claim.

**Repo-root anchoring + module-level path constants** (lines 37-51):
```python
from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
MAKEFILE = REPO_ROOT / "Makefile"
```
`parents[2]` from `tests/policy/*.py`. New constants follow the same form:
`KIND_DIR = REPO_ROOT / "kind"`, `VALUES_DIR = REPO_ROOT / "helm" / "values"`,
`MANIFEST_DIR = REPO_ROOT / "build" / "manifests"`, `SCRIPTS_DIR = REPO_ROOT / "scripts"`.

**Pure predicate returning a list of messages, separated from the assertion** (lines 85-107):
```python
def secret_reference_problems(text: str, label: str = "") -> list[str]:
    """Report every repository-secret reference outside the allowed set."""
    problems: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for match in SECRET_REFERENCE.finditer(line):
            name = match.group(1)
            if name not in ALLOWED_SECRETS:
                problems.append(f"{label}line {lineno}: references secrets.{name}")
    return problems
```
This is the load-bearing convention: the predicate is **pure and importable**, so
non-vacuity can be proven against a mutated in-memory copy without touching disk. Assertion
sites read:
```python
assert not problems, "<explanation of the claim>\n\n" + "\n".join(problems)
```

**Non-vacuity by mutation — no file is edited** (lines 146-151, 188-209):
```python
def test_a_new_secret_reference_is_reported() -> None:
    text = (WORKFLOW_DIR / "ci.yml").read_text(encoding="utf-8")
    injected = "env:\n      TOKEN: ${{ secrets.NPM_TOKEN }}"
    mutated = text.replace("runs-on: ubuntu-latest", injected, 1)
    assert mutated != text, "the scratch mutation did not apply — this test proves nothing"
    assert secret_reference_problems(mutated), "a new secrets.* reference was not reported"
```
Note the guard on the mutation itself — copy it. And the paired **false-positive control**:
```python
def test_ordinary_commands_are_not_mistaken_for_dumps() -> None:
    """A pattern that fires on `make install` would be turned off, not fixed."""
```
Every new detector owes both: a positive (bad input reported) and a negative (good input
not reported). Specifically:
- `test_kind_cluster_config.py`: delete a node / an `extraMount` / the
  `KubeletConfiguration` patch from a parsed copy of `kind/cluster.yaml` and require each
  removal to be reported.
- `test_values_profiles.py`: inject a **fourth** divergence axis (D-06 permits only
  replicas, resources, monitoring) and require it reported.
- `test_manifest_resources.py`: strip `resources.requests` from one rendered container and
  require it reported; inflate one request past the 4 CPU / 16 GB budget and require it
  reported.

**Analog B (multi-source drift + load-bearing proof):**
`tests/policy/test_pinned_tool_versions_agree.py` — the model for `test_values_profiles.py`
and for extending pin agreement to kind/helm/kubeconform.

Reader functions, one per source, assembled into a table (lines 118-153):
```python
def uv_readings() -> dict[str, str]:
    return {
        "Makefile": _makefile_variable("UV_REQUIRED_VERSION"),
        ".github/workflows/ci.yml": _workflow_env().get("UV_VERSION", ""),
    }

ALL_READINGS = {"ruff": ruff_readings, "mypy": mypy_readings,
                "gitleaks": gitleaks_readings, "uv": uv_readings}
```
Makefile-variable reader, reusable verbatim for `KIND_VERSION` / `HELM_VERSION` /
`KUBECONFORM_VERSION` (lines 105-108):
```python
def _makefile_variable(name: str) -> str:
    match = re.search(rf"^{name}\s*:?=\s*(\S+)", MAKEFILE.read_text(encoding="utf-8"), re.MULTILINE)
    assert match, f"Makefile no longer defines {name}"
    return match.group(1)
```
And the meta-test proving no source is silently ignored (lines 197-213):
```python
def test_every_source_is_load_bearing() -> None:
    """Perturbing any single source must produce a disagreement."""
    for tool, reader in ALL_READINGS.items():
        readings = reader()
        assert len(readings) >= 2, f"{tool}: only one source, nothing to compare"
        for source in readings:
            mutated = dict(readings)
            mutated[source] = "0.0.0-scratch"
            assert disagreements(tool, mutated), (
                f"changing {source} alone for {tool} was not reported as drift"
            )
```
Copy this for `helm/versions.env` ↔ Makefile ↔ values-file image tags (CONTEXT: "pinned
versions live in exactly one place").

**Analog C (scanning scripts for forbidden constructs):**
`tests/policy/test_ci_invokes_make_only.py` — the model for
`test_no_manual_kubectl_surgery.py`.
```python
# The four gate tools, plus the scanner binary. ... `tools/bin/gitleaks` is the scanner
# itself, as opposed to tools/security/install_gitleaks.sh which merely fetches it.
DIRECT_TOOLS = re.compile(r"\b(ruff|mypy|pytest|lint-imports)\b|tools/bin/gitleaks")
```
Note the deliberate carve-out in its docstring — *"The scanner installer is deliberately not
on the forbidden list"*. `test_no_manual_kubectl_surgery.py` needs the exact analogue:
`kubectl create/edit/patch/apply` is forbidden, while `kubectl wait`, `kubectl get` and
`kubectl apply -f kubernetes/<committed>.yaml` are permitted, and the permitted set must be
justified in the docstring — RESEARCH Pattern 3 requires `kubectl wait` in `cluster-up`.
Also note that the comment-stripping behaviour ("Comment lines inside a run block are
ignored") is needed here too, since the scripts will explain *why* they delegate.

**Analog D (subprocess fault injection / non-vacuity against a real tool):**
`tests/policy/test_gates_actually_fail.py` — the model for
`test_doctor_fails_closed.py` and `test_manifest_validation_fails_closed.py`.

Its opening claim is exactly this phase's D-10/CICD-07 claim (lines 1-18, condensed):
```python
"""Meta-verification: every gate in this phase is observed rejecting a bad input.

A configured linter that has never been seen to fail is indistinguishable from a
disabled one ...

Each gate is therefore exercised twice:
* a **negative** case — the real tool runs against a deliberately-broken sample
  and must exit non-zero **and** name the expected rule. ...
* a **positive control** — the same tool, the same configuration, the matching
  correct sample, and a zero exit. A gate that rejects everything is exactly as
  broken as one that rejects nothing, and only the pair distinguishes them.
"""
```

Subprocess runner — no `check=True`, because the non-zero exit **is** the signal (lines 109-123):
```python
def _run(cmd: list[str], env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    """Run a real tool from the repository root and hand back the real result.

    No `check=True` and no exception handling: a non-zero exit is the signal
    under test in half of these cases, and swallowing a tool failure into a
    defaulting fallback would turn a broken gate into a passing test.
    """
    return subprocess.run(  # noqa: S603  # deliberately invoking the project toolchain
        cmd, cwd=REPO_ROOT, capture_output=True, text=True, env=env, check=False,
    )
```

Broken-sample corpus convention — `tests/policy/badsamples/`, each sample paired with a
`good_` counterpart, each declaring its rule and consuming test in a header (lines 345-376).
`test_manifest_validation_fails_closed.py` needs the same: a deliberately-invalid manifest
**and** its valid twin, both under `badsamples/` (or a `badmanifests/` sibling), each
carrying a `# Trips:` / `# Proves:` header naming
`tests/policy/test_manifest_validation_fails_closed.py::<test>`.

Guard against the samples poisoning the main gate (lines 322-342) — directly relevant,
because a broken manifest sitting in the tree would otherwise make `make manifests`
permanently red:
```python
def test_the_main_gate_does_not_lint_the_bad_samples() -> None:
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
```
The "make echoes its recipe, so require the tool in the transcript" trick is exactly what
`test_doctor_fails_closed.py` needs: `make doctor` exiting 0 without having run any check
would otherwise pass. For `doctor` fault injection, prefer overriding the Makefile's tool
variables (`make doctor KIND=/nonexistent`, mirroring the `UV=` override that `uv-guard`'s
error message already anticipates: `found '${have:-none}' (UV=$(UV))`) over mutating files.

---

### `tests/policy/test_workflow_secrets.py` (modify — widen scope for D-14)

**Current scope** (lines 46-51, 71-72):
```python
REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
MAKEFILE = REPO_ROOT / "Makefile"

def _workflow_paths() -> list[Path]:
    return sorted(p for p in WORKFLOW_DIR.glob("*.y*ml"))
```
The change is additive: add a second scanned surface for `helm/`, `kubernetes/`, `kind/` and
`scripts/`, asserting no credential literal (`rootPassword:`, `fernetKey:`,
`webserverSecretKey:`, `password:` with a non-empty scalar, base64 `data:` blobs). RESEARCH
§ Project Constraints names the forbidden keys precisely, and the chart-side alternatives
(`existingSecret`, `fernetKeySecretName`, `webserverSecretKeySecretName`,
`data.metadataSecretName`) are the permitted forms.

**Do not widen `ALLOWED_SECRETS`** — its docstring (lines 50-51, and lines 19-22) makes
adding an entry an obligation to re-audit SEC-10 in its general form, and this phase adds no
CI secret.

---

### `tests/e2e/cluster/conftest.py` (test fixture provider, request-response)

**Analog:** `tests/conftest.py` — the only existing conftest. It is minimal, so most of this
file is new; copy the anchoring convention and the docstring register.

```python
"""Shared pytest fixtures.

The repository root is resolved once, from this file's own location, so a policy
test never depends on the working directory pytest happened to be started from.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="session")
def repo_root() -> Path:
    """Return the absolute path of the repository root."""
    return REPO_ROOT
```
For `tests/e2e/cluster/conftest.py` use `parents[3]`. New content with no analog: the
session-scoped boto3 client built from `make minio-creds` (D-14 — never a committed
credential), the `kubectl` shell helper, and skip-if-no-cluster. Marker application follows
the existing `markers` registry (see pyproject below); `pytestmark = pytest.mark.cluster` at
module scope in each `test_*.py` is the least-repetitive form, and `--strict-markers`
rejects it until the marker is registered.

---

### `docs/adr/0006-*.md` (doc)

**Analog:** `docs/adr/0000-template.md` — copy verbatim and fill.
```markdown
---
status: {proposed | accepted | rejected | deprecated | superseded by ADR-00NN}
date: YYYY-MM-DD
---

# ADR-00NN: {short title, a decision phrased as a claim}

## Context and Problem Statement
## Considered Options
## Decision Outcome
### Consequences
## Migration trigger
{What observable event would make us revisit this? "None — this is permanent" is a
valid answer and must be written explicitly rather than left blank.}
## References
* README §NN
* .planning/research/{FILE}.md §{section}
```

Conventions from `docs/adr/README.md` and `0005-*.md`:
- Frontmatter is exactly `status:` + `date:`; accepted records read `status: accepted`.
- The **title is a claim, not a topic** — cf. *"The CSV fixture corpus is generated from a
  seed and never committed; the digest file is the oracle"*.
- Options are lettered and named with their verdict: `* **A — Commit the corpus.** …`.
- `## Migration trigger` is bespoke to this repo and mandatory.
- Add the index row to `docs/adr/README.md`'s `## Records` table:
  `| [0006](0006-….md) | … | accepted | 2026-08-12 |`.
- Next free number is **0006**; `0000` is permanently the template and never indexed.

**Scope note from RESEARCH § Package Legitimacy Audit:** ADR-0006 must name **three**
unmaintained artifacts, not only MinIO — `pgsty/minio`, `registry.k8s.io/ingress-nginx
1.15.1` (archived read-only 2026-03-24, new since STACK.md) and
`quay.io/minio/mc`. Migration targets: SeaweedFS for MinIO, Gateway API for ingress.

---

### `pyproject.toml` (modify)

**Marker registration** — required by `--strict-markers`:
```toml
[tool.pytest.ini_options]
minversion = "9.0"
testpaths = ["tests"]
addopts = "-ra --strict-markers --strict-config"
markers = [
  "slow: generates or reads a large fixture",
  "regression: permanent test for a specific fixed bug (QUAL-07)",
]
```
Add one entry in the same `name: description` form: `"cluster: requires a live kind cluster"`.

**Dependency group** — the existing `dev` group and its comment show the convention
(rationale comments inline, ranges for non-gate deps, `==` only for gates):
```toml
[dependency-groups]
dev = [
  "dataplat",
  "csv-processor",
  "pytest>=9.1,<10",
  ...
  "ruff==0.16.2",                      # pinned exactly: this is a gate
  "mypy==2.3.0",                       # pinned exactly: this is a gate
  "PyYAML>=6",
]
```
Add a **separate** root group so `uv sync` for the offline gate does not pull boto3:
`cluster = ["boto3>=1.43,<2", "psycopg[binary]>=3.3,<4"]`, then `uv lock`. `PyYAML` is
already present, so the policy tests need no new dependency (RESEARCH § Supporting).
Cross-phase coordination point — RESEARCH Open Question 1.

---

### `.github/workflows/ci.yml` (modify)

**Analog:** itself. The binding invariant, enforced by
`tests/policy/test_ci_invokes_make_only.py` and visible in the file's own comment:
```yaml
      - run: make install
      # The only substantive step. No linter, type checker, test runner or
      # import checker may be invoked directly from this file — the Makefile is
      # the single definition of the gate, and plan 01-05 adds the policy test
      # that enforces it.
      - run: make check
```
Consequence for D-12: **no new step is needed at all** if `manifests` is wired into `check`.
If a separate job is required (network-dependent chart fetch), it must be `- run: make
<target>` and nothing else, with actions pinned by commit SHA:
```yaml
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
```
and the workflow-level `permissions: {contents: read}` untouched
(`test_workflow_secrets.py::test_the_workflow_token_stays_read_only`).

---

## Shared Patterns

### 1. Fail-closed, with the exact remediation command
**Source:** `Makefile:30-37` (`uv-guard`), `tools/security/install_gitleaks.sh:77-87`
**Apply to:** `scripts/doctor.sh`, all three `tools/k8s/install_*.sh`, every Makefile guard
Print to **stderr**, name what was found vs. what was required, print the literal command to
fix it, then `exit 1`. Never warn-and-continue. The single exception this phase grants is
D-04's rebuild wall-clock, which warns by explicit decision and should say so in a comment.

### 2. Non-vacuity: every gate is observed failing
**Source:** `tests/policy/test_gates_actually_fail.py` (subprocess form),
`tests/policy/test_workflow_secrets.py:146-151` (pure-predicate + mutation form),
`Makefile:93-94` (`gitleaks-selftest`)
**Apply to:** `test_doctor_fails_closed.py`, `test_manifest_validation_fails_closed.py`, and
the detector in every other new policy test
Choose the mutation form when the check is a pure function over parsed content (cheaper, no
disk writes); the subprocess form when the gate is an external tool (`kubeconform`, `make
doctor`). Always pair a negative case with a positive control.

### 3. Repo-root anchoring
**Source:** `tests/conftest.py:13`, every `tests/policy/*.py:~35`
**Apply to:** all new tests
```python
REPO_ROOT = Path(__file__).resolve().parents[2]   # tests/policy/*.py
```
Never `os.getcwd()`, never a relative path.

### 4. A version literal appears in exactly one place
**Source:** `tests/policy/test_pinned_tool_versions_agree.py`, enforced bidirectionally by
`test_every_source_is_load_bearing`
**Apply to:** `helm/versions.env`, Makefile `*_REQUIRED_VERSION`, values-file image tags,
`tools/k8s/install_*.sh`
Where duplication is unavoidable, add a reading function to `ALL_READINGS` so drift fails a
test rather than surviving.

### 5. Docstrings that state the limit of the claim
**Source:** `tests/policy/test_workflow_secrets.py:1-35`,
`tests/policy/test_gates_actually_fail.py:251-260` (the D417 honest-limit paragraph)
**Apply to:** every new policy and e2e test
Say what the test does **not** prove. This is a load-bearing repo convention, not decoration.

### 6. `make` is the sole gate definition
**Source:** `Makefile:1-2`, `.github/workflows/ci.yml`, `tests/policy/test_ci_invokes_make_only.py`
**Apply to:** every new target, every CI change, `scripts/*.sh` (scripts are target
*internals*, invoked by make, never by CI directly — with the installer carve-out precedent)

---

## No Analog Found

The planner should take these from `02-RESEARCH.md` (which verified them against a live kind
cluster) rather than looking for a repo precedent.

| File | Role | Data Flow | Reason / Where to source it |
|---|---|---|---|
| `kind/cluster.yaml`, `kind/cluster-ci.yaml` | config | — | No YAML infrastructure exists in this repo. RESEARCH § Pattern 1 carries a **verified working** `cluster.yaml`; § Pitfall 2 carries the reservation arithmetic; § Pattern 2 the `/mnt/persist` "declared but unbound" mechanism |
| `helm/values/{local,ci}/*.yaml` (10 files) | config | — | No values file exists. RESEARCH § Code Examples has verified MinIO deny-delete, CNPG four-key override, and the Airflow minimum honest set; § Pitfall 5 enumerates the 10 + 4 + 1 + 1 required `resources` keys |
| `helm/versions.env` | config | — | New concept. Closest spirit: `Makefile:6-7` `UV_REQUIRED_VERSION` and `.github/workflows/ci.yml` `env:` block — one source, asserted by a policy test |
| `helm/schemas/cnpg/*.json` | generated artifact | transform | Produced by `scripts/vendor-crd-schemas.sh` via `openapi2jsonschema.py`; nothing comparable exists |
| `kubernetes/namespaces.yaml` | config | — | No Kubernetes manifest exists yet (D-13's five namespaces) |
| `scripts/doctor.sh` | utility | request-response | Shell exists only as an installer. Take the **guard shape** from `Makefile:30-37` and the fail-closed idiom from Shared Pattern 1; the checks themselves (inotify, free ext4 disk, Docker, ports 80/443, repo not under `/mnt/`) are new. RESEARCH § Environment Availability lists all of them with measured current values |
| `scripts/cluster-{up,down,rebuild}.sh` | utility | batch | New. RESEARCH § Pattern 3 gives the verified `helm upgrade --install --wait` + `kubectl wait` ordering; § Pitfall 7 the Helm 4 CLI changes (`--atomic` is **gone**; `--wait` defaults to `hookOnly` when omitted) |
| `scripts/minio-credentials.sh` | utility | file-I/O | New (D-14). No credential-generation code exists anywhere in the repo — deliberately |
| `scripts/airflow-metadata-secret.sh` | utility (adapter) | transform | New. RESEARCH § Pattern 4 gives both verified contracts (chart key `connection`; the CNPG `-app` Secret's 11 keys) and the URL-encoding requirement |
| `scripts/wait-for.sh`, `render-manifests.sh`, `vendor-crd-schemas.sh` | utility | batch | New; all three verified by execution in RESEARCH |
| `docs/wsl/wslconfig.example` | config (doc) | — | `docs/wsl/` does not exist. No `.example` file convention exists in this repo |
| `tests/e2e/cluster/test_*.py` (5 files) | test (e2e) | request-response | `tests/e2e/` is empty; there is no live-service test in the repo. Assertion style and docstring register come from `tests/policy/`; the mechanics (boto3, `kubectl`, HTTP through ingress) are new. RESEARCH § Anti-Patterns warns the triggerer renders as a **StatefulSet**, so "four Deployments" is a wrong assertion |

---

## Metadata

**Analog search scope:** `Makefile`, `tools/`, `tests/`, `docs/adr/`, `.github/workflows/`,
`pyproject.toml`, and the (empty) `kind/`, `helm/`, `kubernetes/`, `scripts/`, `airflow/`
trees
**Files scanned:** 24 · **Files read in full:** 10
**Pattern extraction date:** 2026-08-12
