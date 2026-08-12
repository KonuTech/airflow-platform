---
phase: 02-kind-cluster-core-infrastructure
reviewed: 2026-08-12T18:54:35Z
depth: standard
files_reviewed: 64
files_reviewed_list:
  - .github/workflows/ci.yml
  - docs/adr/0006-unmaintained-upstream-artifacts.md
  - docs/adr/0007-helm-4-over-helm-3.md
  - docs/adr/README.md
  - docs/wsl/wslconfig.example
  - helm/schemas/cnpg/README.md
  - helm/values/ci/airflow.yaml
  - helm/values/ci/cnpg-airflow.yaml
  - helm/values/ci/cnpg-analytics.yaml
  - helm/values/ci/cnpg-operator.yaml
  - helm/values/ci/ingress-nginx.yaml
  - helm/values/ci/minio.yaml
  - helm/values/local/airflow.yaml
  - helm/values/local/cnpg-airflow.yaml
  - helm/values/local/cnpg-analytics.yaml
  - helm/values/local/cnpg-operator.yaml
  - helm/values/local/ingress-nginx.yaml
  - helm/values/local/minio.yaml
  - helm/versions.env
  - kind/cluster.yaml
  - kubernetes/namespaces.yaml
  - scripts/airflow-metadata-secret.sh
  - scripts/cluster-down.sh
  - scripts/cluster-rebuild.sh
  - scripts/cluster-up.sh
  - scripts/doctor.sh
  - scripts/helm-install.sh
  - scripts/minio-credentials.sh
  - scripts/render-manifests.sh
  - scripts/stages/10-registry.sh
  - scripts/stages/20-namespaces.sh
  - scripts/stages/30-ingress-nginx.sh
  - scripts/stages/40-cnpg-operator.sh
  - scripts/stages/50-airflow-db.sh
  - scripts/stages/55-analytics-db.sh
  - scripts/stages/60-minio.sh
  - scripts/stages/70-airflow.sh
  - scripts/vendor-crd-schemas.sh
  - scripts/wait-for.sh
  - tests/e2e/__init__.py
  - tests/e2e/cluster/__init__.py
  - tests/e2e/cluster/conftest.py
  - tests/e2e/cluster/test_airflow_workloads.py
  - tests/e2e/cluster/test_ingress.py
  - tests/e2e/cluster/test_minio_buckets.py
  - tests/e2e/cluster/test_node_capacity.py
  - tests/e2e/cluster/test_postgres_topology.py
  - tests/policy/badmanifests/cluster_null_postgresql.yaml
  - tests/policy/badmanifests/good_cluster_null_postgresql.yaml
  - tests/policy/test_doctor_fails_closed.py
  - tests/policy/test_kind_cluster_config.py
  - tests/policy/test_manifest_resources.py
  - tests/policy/test_manifest_validation_fails_closed.py
  - tests/policy/test_no_manual_kubectl_surgery.py
  - tests/policy/test_offline_gate_stays_offline.py
  - tests/policy/test_pinned_tool_versions_agree.py
  - tests/policy/test_supply_chain_guards.py
  - tests/policy/test_values_profiles.py
  - tests/policy/test_workflow_secrets.py
  - tools/k8s/__init__.py
  - tools/k8s/crd_to_jsonschema.py
  - tools/k8s/install_helm.sh
  - tools/k8s/install_kind.sh
  - tools/k8s/install_kubeconform.sh
findings:
  critical: 0
  warning: 4
  info: 2
  total: 6
status: issues_found
---

# Phase 02: Code Review Report

**Reviewed:** 2026-08-12T18:54:35Z
**Depth:** standard
**Files Reviewed:** 64
**Status:** issues_found

## Summary

This phase's infrastructure-as-code (kind cluster config, five pinned Helm
chart value profiles, bootstrap/wait scripts, the CRD-schema vendoring
pipeline, and a large, unusually rigorous policy/e2e test suite) is of high
quality: pinned-tool installers verify SHA-256 digests before extraction and
never execute an unverified binary to check its version, secrets are
generated at `cluster-up` time and piped to `kubectl apply -f -` on stdin
rather than committed, the CI workflow pins actions by commit SHA and holds
`permissions: contents: read` throughout, and the policy tests go well beyond
"the happy path renders" — they assert non-vacuity by mutation for nearly
every gate. No SQL/command injection, hardcoded credential, or `eval`-class
vulnerability was found in the reviewed files.

The defects found are narrower: two credential-provisioning scripts build
Kubernetes Secret YAML by naive string interpolation without escaping every
field they embed; `kind/cluster.yaml` — a file the project's own conventions
treat as universally reproducible, committed infrastructure — hardcodes one
developer's absolute home-directory paths, which will silently produce the
wrong (or an unintended, auto-created) bind mount on any other machine or
account; and `scripts/doctor.sh`'s host-port preflight check fails open
(silently skips, rather than failing) when the `ss` tool is unavailable,
contradicting the script's own stated "never warn-and-continue on a blocking
check" design principle. None of these rise to data loss or an exploitable
vulnerability under the project's current single-operator trust model, but
all four are worth fixing before this phase is treated as done.

## Warnings

### WR-01: `kind/cluster.yaml` hardcodes one developer's absolute host paths

**File:** `kind/cluster.yaml:86-99, 134-137, 164-167`
**Issue:** Every node's `extraMounts` hardcodes
`/home/konutec/projects/airflow-platform/airflow/dags` and
`/home/konutec/.local/share/airflow-platform/pv` as literal `hostPath`
values. This file is committed and is explicitly documented elsewhere in the
repository as "the ENTIRE creation-time-only surface for this platform" and
as reproducible, committed infrastructure — but these two paths bake in one
specific username and one specific clone location. The project's own memory
notes that the same developer works across two machines (a 12-CPU/32GB
laptop and a 64GB PC); if the second machine's username or clone path
differs even slightly, `kind create cluster` will not necessarily fail
loudly — Docker auto-creates a missing bind-mount source directory on the
host, so the cluster comes up with a silently empty or wrong DAG-mount
directory instead of erroring. That is precisely the "silently does the
wrong thing" failure mode this project's own Core Value statement singles
out as unacceptable ("no data is ever silently dropped, duplicated or
corrupted... never silently do the wrong thing").
**Fix:** Resolve the repository root and a configurable persistence root at
generation time instead of hardcoding them — e.g. template `kind/cluster.yaml`
from a script that substitutes `$(pwd)` / `$HOME` (matching the pattern
every other script in this phase already uses:
`repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"`), or document
explicitly in `docs/wsl/wslconfig.example`-style guidance that this file must
be regenerated per machine and is not portable as committed. At minimum, add
a `scripts/doctor.sh` check that fails closed when either `hostPath` does not
already exist and is not the invoking user's own path.

### WR-02: Secret manifests are assembled by raw string interpolation without escaping every embedded field

**File:** `scripts/airflow-metadata-secret.sh:88-106, 135-158`, `scripts/minio-credentials.sh:62-80`
**Issue:** Both `_apply_secret` (airflow-metadata-secret.sh) and
`_create_secret` (minio-credentials.sh) build a `stringData:` YAML block via
`printf '  %s: %s\n' "${key}" "${value}"` with no escaping of `${value}`. In
`minio-credentials.sh` every value is `openssl rand -hex`, which is provably
YAML-safe (0-9a-f only) — that script is fine. But
`airflow-metadata-secret.sh`'s `cmd_ensure` builds
`connection="postgresql://${encoded_user}:${encoded_password}@${host}.${SOURCE_NAMESPACE}:${port}/${dbname}"`
and only URL-encodes `username`/`password` via `_urlencode`; `host`, `port`
and `dbname` are read verbatim from the CNPG-generated Secret and inserted
into both the connection URI and the generated Secret's YAML with no
escaping at all. Today these values are operator-controlled (a k8s service
name, a numeric port, and `airflow`), so there is no practical exploit path
in the current design — but if a value ever contained a literal newline, a
leading YAML indicator character (`*`, `&`, `!`, `%`, a quote), or a colon
followed by whitespace, the manually-assembled YAML piped straight into
`kubectl apply -f -` could be corrupted or, in the worst case, gain
attacker-controlled additional keys in the applied Secret. This is exactly
the class of defect (unsafe construction of structured data from
interpolated strings) the review scope calls out under "unsafe
deserialization / injection."
**Fix:** URL-encode `host`, `port` and `dbname` through the same `_urlencode`
helper already used for `username`/`password` before composing the
connection string, and/or replace the hand-rolled YAML `printf` block with a
call through `kubectl create secret generic ... --dry-run=client -o yaml |
kubectl apply -f -`, which lets `kubectl` itself handle correct YAML/JSON
encoding of arbitrary byte values instead of the script re-implementing a
YAML emitter by hand.

### WR-03: `scripts/doctor.sh`'s host-port check fails open when `ss` is unavailable

**File:** `scripts/doctor.sh:184-198`
**Issue:** `check_ports` pipes `ss -ltn 2>/dev/null` into the rest of the
pipeline. If `ss` is not installed (a stripped-down or non-`iproute2`
environment), `ss -ltn` fails silently (stderr redirected to `/dev/null`),
the pipeline receives empty input, and `grep -qE` never matches — so the
function simply never calls `fail`, and `make doctor` reports success even
though port 80/443 availability was never actually checked. This directly
contradicts the script's own stated design principle in its header comment:
"Never warn-and-continue on a blocking check; exits 1 if any check failed,
after every check has run." Every other check in this file (`docker`,
`kubectl`, `kind`, `helm`) explicitly detects the missing-tool case and calls
`fail`; `check_ports` is the one check that silently degrades to a no-op
instead.
**Fix:** Detect `ss` absence explicitly (`command -v ss`) and call `fail`
with a clear "cannot verify host port availability" message and a
remediation command (install `iproute2`), matching the pattern every other
check in this file already follows.

### WR-04: `helm_install`'s echoed command line omits flags the real invocation uses

**File:** `scripts/helm-install.sh:76-83`
**Issue:** The function echoes
`"helm upgrade --install ${release} ${chart_ref} --version ${version} -n ${namespace} -f ${values_file} --wait=${wait_strategy}"`
for operator visibility, but the actual `helm` invocation two lines below
also passes `--timeout "${HELM_INSTALL_TIMEOUT:-5m}"` and, when
`KUBECTL_CONTEXT` is set, `--kube-context "${KUBECTL_CONTEXT}"`. The printed
line is missing both. A developer debugging a failed install who copies the
printed command verbatim to reproduce it locally will silently run against
the ambient `kubectl` current-context instead of the cluster this project's
scripts always target explicitly — the exact "never the ambient
current-context" pitfall this repository's own e2e test fixtures
(`tests/e2e/cluster/conftest.py`) are careful to avoid.
**Fix:** Build the echoed line from the same array/variables passed to the
real `helm` invocation (or just `set -x` around the call) so the printed
command is always what actually ran.

## Info

### IN-01: `ci/airflow.yaml` still sizes `workers.kubernetes.resources`, which LocalExecutor never uses

**File:** `helm/values/ci/airflow.yaml:132-141`
**Issue:** The CI profile sets `executor: LocalExecutor` (correctly, per the
file's own header comment — KubernetesExecutor's two-pods-per-task model
doesn't fit a 4 CPU/16 GB runner). `workers.kubernetes.resources`, however,
only takes effect under `KubernetesExecutor` and is dead configuration in
this profile. It's harmless today, but a future reader could reasonably
assume it is live and rely on it to size a CI task pod that will never be
created under this executor.
**Fix:** Either drop the key from `ci/airflow.yaml` or add a one-line comment
noting it is inert under `LocalExecutor` and kept only for values-shape
parity with `local/airflow.yaml`.

### IN-02: MinIO's `etl-app` IAM policy has no read/write path to `processed`, `quarantine`, or `metadata`

**File:** `helm/values/local/minio.yaml:93-113`, `helm/values/ci/minio.yaml:68-79`
**Issue:** `etl-app`'s policy statements only reference `raw` and
`validated`. This is documented as deliberate for this phase ("processed,
quarantine and metadata are reached by the admin credential only in this
phase"), so it is not a defect against this phase's own stated scope — but
it means the pipeline's own working credential cannot write to three of the
five buckets it is provisioned for, which will need to be revisited the
moment a later phase's workload tries to write a validated/processed/
quarantined record under its own (non-admin) identity.
**Fix:** No action needed for this phase; flagged so the follow-up phase that
wires the ETL pipeline's actual write path doesn't rediscover this as a
surprise.

---

_Reviewed: 2026-08-12T18:54:35Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
