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

# `doctor-live`'s own override point, same shape as KIND=/HELM=/KUBECTL=
# above — lets a test point it at a fake `docker` without touching the real
# cluster (tests/policy/test_doctor_live_detects_mount_state.py).
DOCKER ?= docker

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

# D-14/plan 04-09: the local CSV path `make ingest-demo` uploads. No default —
# `ingest-demo`'s own recipe guard fails loudly (stage-%'s own explicit-
# failure style) rather than silently picking a fixture the caller did not ask
# for.
FILE ?=

.PHONY: help uv-guard install lock-check lint format typecheck imports test policy \
        fixtures fixtures-verify gitleaks gitleaks-selftest check ci clean \
        install-cluster doctor doctor-live doctor-live-check cluster-up cluster-down cluster-rebuild cluster-verify \
        minio-creds helm-lint manifests manifest-policy test-integration image-csv-processor \
        image-airflow image-dbt ingest-demo vault-unseal vault-bootstrap vault-verify vault-audit-tail \
        migrate-analytics

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

doctor-live:                   ## Detect+self-heal the DAGs tmpfs-fallback mount on an already-running cluster [debug: docker-desktop-wsl2-vm-restart]
	# Unlike `doctor`, this checks a cluster that is ALREADY UP, not a
	# preflight before creating one. Restarts only the affected kind node
	# container(s) via `docker restart` if the DAGs hostPath bind mount has
	# fallen back to tmpfs (the exact symptom of a Docker Desktop/WSL2 VM
	# restart during a long unattended session — see the debug session for
	# the researched root cause and its Windows-side mitigation).
	DOCKER=$(DOCKER) scripts/doctor-live.sh

doctor-live-check:             ## Same detection as doctor-live, report-only, no restart
	DOCKER=$(DOCKER) DOCTOR_LIVE_REPAIR=false scripts/doctor-live.sh

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

ingest-demo:                    ## D-14: upload FILE and wait for the real sensor-driven pipeline [plan 04-09]
	@if [ -z "$(FILE)" ]; then echo "ERROR: FILE is required, e.g. make ingest-demo FILE=tests/fixtures/csv/01_simple.csv" >&2; exit 1; fi
	$(RUN_CLUSTER) python scripts/ingest-demo.py --file $(FILE)

vault-unseal:                   ## D-02: init-or-unseal against .secrets/vault-init.json [plan 05-01]
	$(RUN_CLUSTER) python scripts/vault-unseal.py

vault-bootstrap:                ## idempotent Vault admin bootstrap: mounts, auth method, roles/policies, audit device [plan 05-01]
	$(RUN_CLUSTER) python scripts/vault-bootstrap.py

vault-verify:                    ## INFRA-06: run tests/e2e/vault against the live cluster [plan 05-01]
	$(RUN_CLUSTER) pytest tests/e2e/vault -q

vault-audit-tail:                ## D-04: human-readable tail of Vault's persistent audit log [plan 05-04]
	# Same shape as vault-unseal/vault-bootstrap above, not minio-creds's
	# `set -a; . helm/versions.env; set +a` shell-sourcing shape: this script
	# is Python, like its two siblings, and resolves its own kubectl context
	# by reading helm/versions.env directly (scripts/vault-audit-tail.py's own
	# _kubectl_context helper) -- no KUBECTL_CONTEXT env var is read, so
	# sourcing one here would be dead configuration matching nothing the
	# script actually consumes.
	$(RUN_CLUSTER) python scripts/vault-audit-tail.py

migrate-analytics:               ## 08.1-13: alembic upgrade head against the LIVE analytical PostgreSQL, via a port-forward [plan 08.1-13]
	# Mirrors scripts/vault-bootstrap.py's own _port_forwarded_vault shape
	# (port-forward svc/<X> to a free local port, poll until it accepts a
	# connection, run the real work, always tear the tunnel down) as a plain
	# shell recipe rather than a new Python script -- this target's own work
	# (env var + one `alembic upgrade head` invocation) is small enough not
	# to warrant a fourth sibling script.
	#
	# T-08.1-31: the discovered superuser credential is read ONLY into shell
	# variables, used ONLY to build ALEMBIC_DSN (an environment variable
	# migrations/env.py's own _sqlalchemy_url() already expects -- never a
	# CLI argument, never `set -x`'d, never echoed) -- mirrors
	# _kubectl_get_secret_field's own established no-print discipline.
	#
	# `db_name` is a literal, not read from the Secret: CNPG's own
	# superuser Secret's `dbname` field is literally the wildcard `"*"` (a
	# superuser is not scoped to one database) -- `analytics` matches
	# migrations/env.py's own EXPECTED_DATABASE guard, which fails loudly
	# (INFRA-04) if this ever pointed elsewhere.
	@set -a; . helm/versions.env; set +a; \
	ctx="kind-$$CLUSTER_NAME"; \
	ns="data"; \
	cluster="analytics-db"; \
	secret_name="$${cluster}-superuser"; \
	echo "==> discovering $$secret_name (namespace $$ns) credentials"; \
	db_user=$$($(KUBECTL) --context "$$ctx" get secret "$$secret_name" -n "$$ns" -o jsonpath='{.data.username}' | base64 -d); \
	db_pass=$$($(KUBECTL) --context "$$ctx" get secret "$$secret_name" -n "$$ns" -o jsonpath='{.data.password}' | base64 -d); \
	db_name="analytics"; \
	local_port=$$(python3 -c "import socket; s=socket.socket(); s.bind(('127.0.0.1', 0)); print(s.getsockname()[1]); s.close()"); \
	echo "==> port-forwarding svc/$${cluster}-rw ($$ns) to localhost:$$local_port"; \
	$(KUBECTL) --context "$$ctx" -n "$$ns" port-forward "svc/$${cluster}-rw" "$${local_port}:5432" >/tmp/migrate-analytics-portforward.log 2>&1 & \
	pf_pid=$$!; \
	trap 'kill $$pf_pid >/dev/null 2>&1 || true' EXIT; \
	connected=0; \
	for _ in $$(seq 1 30); do \
	  if ! kill -0 $$pf_pid 2>/dev/null; then \
	    echo "ERROR: kubectl port-forward for svc/$${cluster}-rw exited early:" >&2; \
	    cat /tmp/migrate-analytics-portforward.log >&2; \
	    exit 1; \
	  fi; \
	  if python3 -c "import socket, sys; s = socket.socket(); s.settimeout(1); sys.exit(0 if s.connect_ex(('127.0.0.1', $$local_port)) == 0 else 1)"; then \
	    connected=1; \
	    break; \
	  fi; \
	  sleep 1; \
	done; \
	if [ "$$connected" != "1" ]; then \
	  echo "ERROR: kubectl port-forward for svc/$${cluster}-rw never accepted a connection within 30s" >&2; \
	  exit 1; \
	fi; \
	encoded_pass=$$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote(sys.argv[1], safe=''))" "$$db_pass"); \
	echo "==> running alembic upgrade head against $$db_name"; \
	ALEMBIC_DSN="postgresql://$${db_user}:$${encoded_pass}@localhost:$${local_port}/$${db_name}" \
	  $(RUN) alembic -c migrations/alembic.ini upgrade head

cluster-verify:                 ## D-16: run tests/e2e/cluster, tests/e2e/slice and tests/e2e/observability against the live cluster [plan 02-02, extended 04-09, 07-07]
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
	# running the suite. tests/e2e/slice (plan 04-08) joins tests/e2e/cluster
	# here so this one target collects this phase's whole E2E suite.
	# tests/e2e/observability (plan 07-07) joins the same way -- unlike
	# tests/e2e/vault's own dedicated vault-verify target, this package's own
	# prerequisites (cluster-up + vault-bootstrap) are already what every
	# other suite collected here needs too, so a second target would just
	# duplicate this one's own invocation shape for no reason.
	$(RUN_CLUSTER) pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability -q

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

image-dbt:                       ## DEDUP-03: build, tag, push the dbt image to the local registry, register it for the dbt build DAG task [plan 08.1-02]
	# Mirrors image-csv-processor's exact shape above: GIT_SHA computed
	# inline, TWICE -- once for the build arg (this image's own
	# org.opencontainers.image.revision/.version labels), once for the tag
	# -- never a literal, never a floating tag.
	docker build \
	  --build-arg GIT_SHA=$$(git rev-parse --short HEAD) \
	  -t dbt:$$(git rev-parse --short HEAD) \
	  -f docker/dbt/Dockerfile .
	docker tag dbt:$(GIT_SHA) localhost:5001/dbt:$(GIT_SHA)
	docker push localhost:5001/dbt:$(GIT_SHA)
	# plan 08.1-12's `dbt build` KPO task resolves its image dynamically via
	# Variable.get("dbt_image"), the SAME way ingest/discover resolve
	# csv_processor_image -- register a NEW Airflow Variable here (dbt_image,
	# never csv_processor_image), unlike image-airflow below which is
	# referenced statically via Helm values instead.
	@set -a; . helm/versions.env; set +a; \
	ctx="kind-$$CLUSTER_NAME"; \
	if $(KUBECTL) --context "$$ctx" --request-timeout=5s get nodes -o name >/dev/null 2>&1; then \
	  echo "==> registering dbt_image=localhost:5001/dbt:$(GIT_SHA)"; \
	  $(KUBECTL) --context "$$ctx" exec -n airflow deploy/airflow-api-server -- \
	    airflow variables set dbt_image "localhost:5001/dbt:$(GIT_SHA)"; \
	else \
	  echo "WARNING: no live cluster — image pushed but dbt_image Variable NOT set; run this target again once the cluster is up, or set it manually" >&2; \
	fi

image-airflow:                  ## OBS-10: build, tag, push the custom Airflow[otel] image to the local registry [plan 07-04]
	# Mirrors image-csv-processor's exact shape above: GIT_SHA computed inline,
	# TWICE -- once for the build arg (which becomes this image's own
	# org.opencontainers.image.revision/.version labels, see the Dockerfile),
	# once for the tag -- never a literal, never a floating tag.
	# tests/policy/test_no_latest_image_tag.py is scoped to image-csv-processor
	# only (TARGET = "image-csv-processor"); this target follows the identical
	# pattern by convention, not because that test reads it too.
	#
	# Unlike image-csv-processor, this target does NOT register an Airflow
	# Variable: the Airflow image itself is referenced by the Airflow Helm
	# chart's own `defaultAirflowRepository`/`defaultAirflowTag` values keys
	# (helm/values/{local,ci}/airflow.yaml, plan 07-04 Task 2) at `helm
	# upgrade` time, never resolved dynamically at DAG-parse/task-run time the
	# way csv_processor_image is -- there is no equivalent runtime Variable
	# lookup to update here.
	docker build \
	  --build-arg GIT_SHA=$$(git rev-parse --short HEAD) \
	  -t airflow:$$(git rev-parse --short HEAD) \
	  -f docker/airflow/Dockerfile .
	docker tag airflow:$(GIT_SHA) localhost:5001/airflow:$(GIT_SHA)
	docker push localhost:5001/airflow:$(GIT_SHA)

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

helm-lint:                      ## CICD-07: helm lint all nine pinned charts against every values profile [plan 02-07, 07-03, 07-07, 11-03]
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
	"$${helm_bin}" repo add open-telemetry https://open-telemetry.github.io/opentelemetry-helm-charts >/dev/null 2>&1 || true; \
	"$${helm_bin}" repo add grafana-community https://grafana-community.github.io/helm-charts >/dev/null 2>&1 || true; \
	"$${helm_bin}" repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true; \
	"$${helm_bin}" repo add kyverno https://kyverno.github.io/kyverno/ >/dev/null 2>&1 || true; \
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
	lint_chart otel-collector open-telemetry/opentelemetry-collector "$${OTEL_COLLECTOR_CHART_VERSION}" otel-collector; \
	lint_chart tempo grafana-community/tempo "$${TEMPO_CHART_VERSION}" tempo; \
	lint_chart monitoring prometheus-community/kube-prometheus-stack "$${KUBE_PROMETHEUS_STACK_CHART_VERSION}" monitoring; \
	lint_chart kyverno kyverno/kyverno "$${KYVERNO_CHART_VERSION}" kyverno; \
	exit $$failed

manifests: helm-lint             ## CICD-07/INFRA-10: render + kubeconform -strict, both profiles, all nine charts [plan 02-07, 07-03, 07-07, 11-03]
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
