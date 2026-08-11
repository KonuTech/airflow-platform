# The ONLY place a quality gate is defined. CI calls `make install` and
# `make check` and nothing else, so the local gate and the CI gate cannot drift.
SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
UV_REQUIRED_VERSION := 0.12.3
RUN := $(UV) run --frozen

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
        fixtures fixtures-verify gitleaks gitleaks-selftest check ci clean

help:                          ## Show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | sed 's/:.*##/\t/'

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

# `check` must never need the network: ROADMAP success criterion 4 is a clone
# followed by `uv sync && make check` with no services running. That is why
# `gitleaks` (which needs a downloaded binary) lives in `ci` and not here.
# `fixtures-verify` regenerates the whole corpus into a temporary directory and
# compares against the committed oracle — the QUAL-08 mechanism, on every run.
check: uv-guard lock-check lint format typecheck imports policy test fixtures-verify  ## Local gate
ci: check gitleaks gitleaks-selftest                                  ## CI gate (superset)

clean:                         ## Remove the venv and every tool cache
	rm -rf .venv .mypy_cache .pytest_cache .ruff_cache tests/fixtures/csv
