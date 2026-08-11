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
	$(UV) sync

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

test:                          ## unit tests, with a coverage report and no threshold
	$(RUN) pytest tests/unit -q --cov --cov-report=term-missing

policy:                        ## repository policy tests (LOAD-12 ban, CI/Make parity)
	$(RUN) pytest tests/policy -q

fixtures:                      ## (re)generate the corpus + rewrite CORPUS.sha256 [plan 01-03]
	$(RUN) python -m tools.corpus generate --out tests/fixtures/csv \
	                                       --manifest tests/fixtures/corpus.yaml \
	                                       --write-digests tests/fixtures/CORPUS.sha256

fixtures-verify:               ## QUAL-08: prove byte-identity against the oracle [plan 01-03]
	$(RUN) python -m tools.corpus verify --manifest tests/fixtures/corpus.yaml \
	                                     --digests tests/fixtures/CORPUS.sha256

gitleaks:                      ## SEC-02/SEC-11: full history + working tree [plan 01-02]
	./tools/bin/gitleaks git --log-opts="--all" --redact --no-banner --exit-code 1 .
	./tools/bin/gitleaks dir  --redact --no-banner --exit-code 1 .

gitleaks-selftest:             ## SEC-11: prove the scanner actually fails a build [plan 01-02]
	$(RUN) python -m tools.security.gitleaks_selftest ./tools/bin/gitleaks

# `check` must never need the network: ROADMAP success criterion 4 is a clone
# followed by `uv sync && make check` with no services running. That is why
# `gitleaks` (which needs a downloaded binary) lives in `ci` and not here.
# Plan 01-03 appends `fixtures-verify` once a corpus exists.
check: uv-guard lock-check lint format typecheck imports policy test  ## Local gate
ci: check gitleaks gitleaks-selftest                                  ## CI gate (superset)

clean:                         ## Remove the venv and every tool cache
	rm -rf .venv .mypy_cache .pytest_cache .ruff_cache tests/fixtures/csv
