# The ONLY place a quality gate is defined. CI calls `make install` and
# `make check` and nothing else, so the local gate and the CI gate cannot drift.
SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
UV_REQUIRED_VERSION := 0.12.3
RUN := $(UV) run --frozen

# D-06/PROFILE: `local` (3-node, full sizing) or `ci` (single-node, trimmed).
# Every stage script and helm_install call resolves its values file from this.
PROFILE ?= local

# The `cluster` dependency group (boto3, psycopg[binary]) is deliberately NOT
# in `dev` and NOT in any uv default-groups set, so `make install` /
# `uv sync --locked` never pulls it into the offline gate's environment. Every
# target that touches a live cluster must name the group explicitly — this
# variable is that one place. Plan 02-02's `cluster-verify` is its first
# consumer; it stays otherwise unused here (no target has tests yet).
RUN_CLUSTER := $(RUN) --group cluster

# D-10: the paths `make doctor` reads a kind/helm/kubectl version from.
# Absolute via $(CURDIR) so `scripts/doctor.sh` resolves them the same way
# regardless of the caller's own cwd. Overridable (`make doctor KIND=...`) so
# a fault-injection test can point at a nonexistent binary without touching
# the real pinned install — mirrors uv-guard's `UV=` override.
KIND ?= $(CURDIR)/tools/bin/kind
HELM ?= $(CURDIR)/tools/bin/helm
KUBECTL ?= kubectl

# `tools/` arrives with the corpus generator in plan 01-03. $(wildcard) keeps
# this target honest until then rather than hard-failing on a path that does not
# exist yet.
TYPECHECK_PATHS := packages/dataplat/src packages/csv-processor/src $(wildcard tools)

# `make fixtures FAST=1` skips the ~293 MB fixture for the inner development
# loop. `make check` and CI never set it: a fast path that is also the default
# is a fast path that silently stops testing the thing. A --fast run is also
# forbidden from rewriting the oracle, because a partial oracle looks complete —
# so FAST=1 drops --write-digests rather than passing a truncated listing.
FAST ?=
FIXTURES_FAST  := $(if $(FAST),--fast,)
FIXTURES_WRITE := $(if $(FAST),--fast,--write-digests tests/fixtures/CORPUS.sha256)

.PHONY: help uv-guard install lock-check lint format typecheck imports test policy \
        fixtures fixtures-verify gitleaks gitleaks-selftest check ci clean \
        install-cluster doctor cluster-up cluster-down cluster-rebuild cluster-verify \
        minio-creds helm-lint manifests manifest-policy test-integration image-csv-processor

# `[a-z%-]` (not just `[a-z-]`) so the `stage-%` pattern rule (plan 02-01) is
# discoverable too, without changing which concrete targets match.
help:                          ## Show targets
	@grep -E '^[a-z%-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

uv-guard:                      ## Fail if the installed uv is not the pinned version
	@have="$$($(UV) --version 2>/dev/null | head -n1 | awk '{print $$2}')"; \
	if [ "$$have" != "$(UV_REQUIRED_VERSION)" ]; then \
	  echo "ERROR: uv $(UV_REQUIRED_VERSION) is required; found '$${have:-none}' (UV=$(UV))." >&2; \
	  echo "Install it with:" >&2; \
	  echo "  curl -LsSf https://astral.sh/uv/$(UV_REQUIRED_VERSION)/install.sh | sh" >&2; \
	  exit 1; \
	fi

install: uv-guard              ## Create the venv from the lockfile
	# --locked, never bare `uv sync`: a bare sync REWRITES uv.lock when it is
	# stale, and CI runs `make install` before `make check`. That ordering made
	# `lock-check` inspect a lockfile this target had just refreshed, so a pull
	# request could change a dependency without regenerating the lock and still
	# go green on an unreviewed resolution. --locked fails instead of resolving.
	$(UV) sync --locked

lock-check:                    ## Fail if uv.lock is stale vs the pyproject files
	$(UV) lock --check

lint:                          ## ruff check (T20 => OBS-03, D => QUAL-02, ANN => QUAL-01)
	$(RUN) ruff check .

format:                        ## ruff format --check
	$(RUN) ruff format --check .

typecheck:                     ## mypy strict (QUAL-01)
	$(RUN) mypy $(TYPECHECK_PATHS)

imports:                       ## import-linter contracts
	$(RUN) lint-imports

test:                          ## unit + regression tests, coverage report, no threshold
	# tests/regression is named EXPLICITLY alongside tests/unit. Naming only
	# tests/unit left the regression tree uncollected by every gate: its
	# provenance-enforcing conftest worked when invoked directly, but a
	# regression test placed there would not have run in CI at all — the
	# opposite of what a regression suite is for (QUAL-07).
	#
	# tests/property, tests/integration and tests/e2e are deliberately NOT
	# here: they are empty today and will need testcontainers or a live
	# cluster. Phase 3 must add them to a target that can provide those, and
	# must not assume `make check` already collects them.
	$(RUN) pytest tests/unit tests/regression -q --cov --cov-report=term-missing

policy:                        ## repository policy tests (LOAD-12 ban, CI/Make parity)
	# `-m "not manifests"` deselects tests/policy/test_manifest_validation_
	# fails_closed.py: it needs the network-installed kubeconform binary and
	# helm/schemas/cnpg/-derived rendered output, both absent on a fresh
	# clone with no network. Deselection, not a skip — a skip is a green line
	# nobody reads, and a test that skips in the only gate that collects it
	# has failed to be a gate. Those tests run under `manifest-policy` below.
	$(RUN) pytest tests/policy -q -m "not manifests"

fixtures:                      ## (re)generate the corpus + rewrite CORPUS.sha256 (FAST=1 skips the large profile)
	$(RUN) python -m tools.corpus generate --out tests/fixtures/csv \
	                                       --manifest tests/fixtures/corpus.yaml \
	                                       $(FIXTURES_WRITE)

fixtures-verify:               ## QUAL-08: prove byte-identity against the oracle
	$(RUN) python -m tools.corpus verify --manifest tests/fixtures/corpus.yaml \
	                                     --digests tests/fixtures/CORPUS.sha256 \
	                                     $(FIXTURES_FAST)

gitleaks:                      ## SEC-02/SEC-11: full history + working tree [plan 01-02]
	@tools/security/install_gitleaks.sh
	./tools/bin/gitleaks git --log-opts="--all" --redact --no-banner --exit-code 1 .
	./tools/bin/gitleaks dir  --redact --no-banner --exit-code 1 .

gitleaks-selftest:             ## SEC-11: prove the scanner actually fails a build [plan 01-02]
	$(RUN) python -m tools.security.gitleaks_selftest ./tools/bin/gitleaks

install-cluster: uv-guard      ## Install the `cluster` dependency group (boto3, psycopg) [plan 02-01]
	# Explicit install path for the group `dev`/default-groups deliberately
	# excludes. `--locked`, same reason `install` uses it.
	$(UV) sync --locked --group cluster

doctor:                        ## D-10: fail-closed host preflight; cluster-up cannot skip it [plan 02-02]
	KIND=$(KIND) HELM=$(HELM) KUBECTL=$(KUBECTL) scripts/doctor.sh

cluster-up: doctor             ## Create/update the kind cluster and every stage [plan 02-01]
	# The only bootstrap entry point (D-09) — delegates to scripts/cluster-up.sh
	# and names no chart/tool version literal here; helm/versions.env is the
	# single source (tests/policy/test_pinned_tool_versions_agree.py enforces
	# agreement between it and every installer's PINNED_VERSION). `doctor` is a
	# hard prerequisite (D-10): a broken host must never reach ten minutes of
	# image pulls before failing.
	PROFILE=$(PROFILE) scripts/cluster-up.sh

cluster-down:                  ## Delete the kind cluster if it exists, else no-op [plan 02-01]
	scripts/cluster-down.sh

cluster-rebuild: doctor        ## D-04: destroy+recreate, timed per-stage, warns past budget [plan 02-02]
	scripts/cluster-rebuild.sh

minio-creds:                   ## D-14: print live MinIO credentials, shell-sourceable [plan 02-04]
	@set -a; . helm/versions.env; set +a; \
	KUBECTL_CONTEXT="kind-$$CLUSTER_NAME" scripts/minio-credentials.sh show

cluster-verify:                 ## D-16: run tests/e2e/cluster against the live cluster [plan 02-02]
	# $(RUN_CLUSTER), NOT $(RUN): boto3/psycopg live in the `cluster` group,
	# deliberately excluded from `dev` and from every uv default-group set, so
	# the offline gate's own environment can never import them. Reachable from
	# NEITHER `check` nor `ci` (WINDOWS #8) — it needs a live cluster, and a
	# gate that needs a cluster is a gate people disable. `uv run` syncs the
	# environment exactly on every invocation, so alternating this with
	# `make check` removes and reinstalls boto3/psycopg each time; that is
	# cheap from cache and is the honest cost of an offline gate whose
	# environment provably cannot import them. `make install-cluster` is the
	# standalone install path when you want the environment prepared without
	# running the suite.
	$(RUN_CLUSTER) pytest tests/e2e/cluster -q

test-integration:               ## D-04: testcontainers PostgreSQL+MinIO — migrations, dataplat [plan 03-02]
	# $(RUN_CLUSTER), same reasoning as cluster-verify above: testcontainers
	# is what makes this target need the `cluster` group at all now (boto3/
	# psycopg themselves are already importable via `dev` since dataplat
	# depends on them directly — see pyproject.toml's updated cluster-group
	# comment). Deliberately its own target, reachable from NEITHER `check`
	# nor `ci` — `tests/integration` needs a local Docker daemon to start
	# throwaway PostgreSQL/MinIO containers, and Makefile lines 94-97's own
	# Phase-1-authored instruction is explicit that Phase 3 must not assume
	# `check` already collects it. Still runs in CI, as its own job (see
	# .github/workflows/ci.yml's `integration` job) — separated from `check`
	# for local-dev speed and Docker-optionality, not exempted from CI.
	$(RUN_CLUSTER) pytest tests/integration -q

# GIT_SHA is fixed once per `make` invocation, ahead of the target below, so
# `docker tag`/`docker push`/the Airflow Variable registration all reference
# the exact image `docker build` produced — even though `docker build` can
# run for real minutes on a cold cache, long enough for a concurrent commit
# to move HEAD. `docker build`'s own two inline `git rev-parse --short HEAD`
# calls (build-arg + -t, unchanged from plan 03-07) evaluate together, before
# docker does any work, so they can never drift from this value or from each
# other — tests/policy/test_no_latest_image_tag.py still reads this recipe
# body expecting at least those two literal invocations.
GIT_SHA := $(shell git rev-parse --short HEAD)

image-csv-processor:            ## INFRA-08/U1: build, tag, push to the local registry, register the image for the DAG [plan 03-07, extended 04-02]
	# GIT_SHA is computed inline, TWICE — once for the build arg (which
	# becomes the image's own org.opencontainers.image.revision/.version
	# labels, see the Dockerfile), once for the tag — and never a literal,
	# never a floating tag. tests/policy/test_no_latest_image_tag.py reads this
	# recipe body to prove that regression can't land silently. Joins
	# neither `check` nor `ci`: it is exercised by
	# tests/integration/test_docker_image.py (Task 3) and by a future CI
	# image-publish job (Phase 11), same reasoning as test-integration above
	# — a Docker build is not part of the network-free offline gate.
	docker build \
	  --build-arg GIT_SHA=$$(git rev-parse --short HEAD) \
	  -t csv-processor:$$(git rev-parse --short HEAD) \
	  -f docker/csv-processor/Dockerfile .
	# 04-02/U1: push to this project's local registry (STACK.md's
	# registry-vs-`kind load docker-image` comparison — only changed layers
	# cross the wire, once, and every node pulls on demand instead of a
	# serial per-node re-tar of the whole image). The source tag here uses
	# $(GIT_SHA), NOT a fresh inline git call, so it always names exactly the
	# image `docker build` just produced above, never a same-run race.
	docker tag csv-processor:$(GIT_SHA) localhost:5001/csv-processor:$(GIT_SHA)
	docker push localhost:5001/csv-processor:$(GIT_SHA)
	# 04-02: record the pushed tag as the image csv_ingest_customers (and the
	# U1 smoke DAG) will launch via KubernetesPodOperator.Variable.get(...).
	# Guarded behind the same live-cluster reachability probe
	# tests/e2e/cluster/conftest.py's `_require_cluster` uses (`kubectl get
	# nodes`, bounded by --request-timeout) so a developer with no cluster
	# running still gets a successful build+push — not a raw kubectl
	# connection-refused failure — and can re-run this target once a cluster
	# exists, or set the Variable by hand in the meantime.
	@set -a; . helm/versions.env; set +a; \
	ctx="kind-$$CLUSTER_NAME"; \
	if $(KUBECTL) --context "$$ctx" --request-timeout=5s get nodes -o name >/dev/null 2>&1; then \
	  echo "==> registering csv_processor_image=localhost:5001/csv-processor:$(GIT_SHA)"; \
	  $(KUBECTL) --context "$$ctx" exec -n airflow deploy/airflow-api-server -- \
	    airflow variables set csv_processor_image "localhost:5001/csv-processor:$(GIT_SHA)"; \
	else \
	  echo "WARNING: no live cluster — image pushed but csv_processor_image Variable NOT set; run this target again once the cluster is up, or set it manually" >&2; \
	fi

# D-09 substitution, recorded rather than silent: D-09 asks for "one target
# per component, ordered by Make prerequisites". What is built instead is an
# ordered stage runner (scripts/cluster-up.sh over scripts/stages/*.sh in
# LC_ALL=C order) plus this pattern rule, which gives a reviewer one
# invocable unit per component without every component plan editing this
# file. Determinism and explicit ordering survive — numeric filename
# prefixes are the order and are readable in one `ls`. What is lost is
# composability: there is no prerequisite chain behind `stage-%`, so
# `make stage-airflow` against a bare cluster fails at its first missing
# dependency instead of building the stages before it. `make cluster-up` is
# the thing that bootstraps from nothing; this rule runs exactly one stage
# and bootstraps nothing.
stage-%:                       ## Run exactly one scripts/stages/<name>.sh (no prerequisite chain)
	@script="$$(find scripts/stages -maxdepth 1 -type f -name '*-$*.sh')"; \
	if [ -z "$$script" ]; then \
	  echo "ERROR: no scripts/stages/*-$*.sh matching stage-$*" >&2; \
	  exit 1; \
	fi; \
	set -a; . helm/versions.env; set +a; \
	PROFILE=$(PROFILE) KUBECTL_CONTEXT="kind-$$CLUSTER_NAME" "$$script"

helm-lint:                      ## CICD-07: helm lint all five pinned charts against every values profile [plan 02-07]
	# No version literal here — every version comes from helm/versions.env,
	# the same rule test_the_makefile_scanner_target_defers_to_the_pinned_
	# installer already enforces for the gitleaks target. `helm lint` wants a
	# local chart directory, not a repo/chart reference, so each chart is
	# `helm pull --untar`-ed into a throwaway directory first (cleaned up on
	# exit) rather than duplicating scripts/render-manifests.sh's `helm
	# template` output, which this target does not depend on.
	@set -a; . helm/versions.env; set +a; \
	tools/k8s/install_helm.sh >/dev/null; \
	helm_bin="$(CURDIR)/tools/bin/helm"; \
	"$${helm_bin}" repo add ingress-nginx https://kubernetes.github.io/ingress-nginx >/dev/null 2>&1 || true; \
	"$${helm_bin}" repo add cnpg https://cloudnative-pg.github.io/charts >/dev/null 2>&1 || true; \
	"$${helm_bin}" repo add minio https://charts.min.io >/dev/null 2>&1 || true; \
	"$${helm_bin}" repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true; \
	"$${helm_bin}" repo update >/dev/null; \
	workdir="$$(mktemp -d)"; \
	trap 'rm -rf "$$workdir"' EXIT; \
	failed=0; \
	lint_chart() { \
	  release="$$1"; chart_ref="$$2"; version="$$3"; values_basename="$$4"; \
	  chart_dir="$${workdir}/$${release}"; \
	  "$${helm_bin}" pull "$${chart_ref}" --version "$${version}" \
	    --untar --destination "$${workdir}" --untardir "$${release}" >/dev/null; \
	  for profile in local ci; do \
	    echo "==> helm lint [$${profile}] $${chart_ref} ($${values_basename})"; \
	    "$${helm_bin}" lint "$${chart_dir}"/*/ \
	      -f "helm/values/$${profile}/$${values_basename}.yaml" || failed=1; \
	  done; \
	}; \
	lint_chart ingress-nginx ingress-nginx/ingress-nginx "$${INGRESS_NGINX_CHART_VERSION}" ingress-nginx; \
	lint_chart cnpg-operator cnpg/cloudnative-pg "$${CNPG_OPERATOR_CHART_VERSION}" cnpg-operator; \
	lint_chart airflow-db cnpg/cluster "$${CNPG_CLUSTER_CHART_VERSION}" cnpg-airflow; \
	lint_chart analytics-db cnpg/cluster "$${CNPG_CLUSTER_CHART_VERSION}" cnpg-analytics; \
	lint_chart minio minio/minio "$${MINIO_CHART_VERSION}" minio; \
	lint_chart airflow apache-airflow/airflow "$${AIRFLOW_CHART_VERSION}" airflow; \
	exit $$failed

manifests: helm-lint             ## CICD-07/INFRA-10: render + kubeconform -strict, both profiles, all five charts [plan 02-07]
	# Installs the two binaries it needs as its own first lines — exactly like
	# the gitleaks target calls tools/security/install_gitleaks.sh — so this
	# target (and therefore the CI job that calls it) never assumes a
	# developer has run an installer by hand. Both installers are idempotent
	# by digest, so this costs a stat on a warm tree.
	@tools/k8s/install_helm.sh
	@tools/k8s/install_kubeconform.sh
	scripts/render-manifests.sh

manifest-policy: manifests       ## CICD-07: the manifests marker, with the render ordered ahead of it [plan 02-07]
	# `manifests` is a declared PREREQUISITE, not just a step that happens to
	# run earlier in `ci`'s list — under `make -j` a list position guarantees
	# nothing, a prerequisite edge does. This is what makes the render
	# complete before tests/policy/test_manifest_validation_fails_closed.py
	# reads build/manifests/.
	#
	# REQUIRE_RENDERED_MANIFESTS=1 is the anti-vacuity switch: a test under
	# the `manifests` marker that would otherwise skip because its inputs
	# (build/manifests/) are absent must fail instead when this is set — see
	# test_the_rendered_cluster_manifests_validate's docstring. Because
	# `manifests` already ran as a prerequisite above, that failure path is
	# never actually reached here; the switch matters for someone invoking
	# `pytest -m manifests` directly, without going through this target.
	#
	# pytest exits 5 when a marker selection collects nothing (the second,
	# free anti-vacuity property here) — an emptied `manifests` selection
	# therefore fails this target rather than reporting a vacuous green.
	REQUIRE_RENDERED_MANIFESTS=1 $(RUN) pytest tests/policy -q -m manifests

# `check` must never need the network: ROADMAP success criterion 4 is a clone
# followed by `uv sync && make check` with no services running. That is why
# `gitleaks` (which needs a downloaded binary) lives in `ci` and not here.
# `manifest-policy` (transitively: helm-lint + manifests + the manifests-
# marked tests) is exactly the same case — `helm template`/`helm pull`/`helm
# repo add` all fetch pinned charts over the network — so it joins `ci`, not
# `check`, for the identical reason.
# `fixtures-verify` regenerates the whole corpus into a temporary directory and
# compares against the committed oracle — the QUAL-08 mechanism, on every run.
check: uv-guard lock-check lint format typecheck imports policy test fixtures-verify  ## Local gate
ci: check manifest-policy gitleaks gitleaks-selftest                  ## CI gate (superset)

clean:                         ## Remove the venv and every tool cache
	rm -rf .venv .mypy_cache .pytest_cache .ruff_cache tests/fixtures/csv
