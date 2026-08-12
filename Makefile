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
        install-cluster doctor cluster-up cluster-down cluster-rebuild cluster-verify

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
	$(RUN) pytest tests/policy -q

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
	@script="$$(find scripts/stages -maxdepth 1 -type f -name '*-$**.sh')"; \
	if [ -z "$$script" ]; then \
	  echo "ERROR: no scripts/stages/*-$*.sh matching stage-$*" >&2; \
	  exit 1; \
	fi; \
	set -a; . helm/versions.env; set +a; \
	PROFILE=$(PROFILE) KUBECTL_CONTEXT="kind-$$CLUSTER_NAME" "$$script"

# `check` must never need the network: ROADMAP success criterion 4 is a clone
# followed by `uv sync && make check` with no services running. That is why
# `gitleaks` (which needs a downloaded binary) lives in `ci` and not here.
# `fixtures-verify` regenerates the whole corpus into a temporary directory and
# compares against the committed oracle — the QUAL-08 mechanism, on every run.
check: uv-guard lock-check lint format typecheck imports policy test fixtures-verify  ## Local gate
ci: check gitleaks gitleaks-selftest                                  ## CI gate (superset)

clean:                         ## Remove the venv and every tool cache
	rm -rf .venv .mypy_cache .pytest_cache .ruff_cache tests/fixtures/csv
