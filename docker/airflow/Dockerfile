# docker/airflow/Dockerfile -- the custom Airflow image this phase discovered is genuinely
# required (Phase 7, OBS-10). Every prior phase left this directory as `.gitkeep`: nothing
# needed a custom Airflow image until now.
#
# RESEARCH.md's Common Pitfalls #1 verified this LIVE, not from documentation: the stock
# `apache/airflow:3.3.0-python3.12` image contains ZERO `opentelemetry` packages --
# `docker run --rm apache/airflow:3.3.0-python3.12 pip list | grep -i otel` returned no
# matches, this session. `apache-airflow[otel]` is an opt-in extra, confirmed absent from
# the production image's own default `AIRFLOW_EXTRAS` build arg. Without this image,
# `[traces] otel_on = True` (helm/values/local/airflow.yaml, plan 07-04 Task 2) would
# either raise `ModuleNotFoundError` inside every Airflow pod, or -- more insidiously --
# silently emit zero traces.
#
# Single stage, never multi-stage (unlike docker/csv-processor/Dockerfile's from-scratch
# `uv` build): this file only layers one dependency install plus OCI labels on top of the
# already-correct upstream base -- WORKDIR and ENTRYPOINT are untouched, inherited as-is
# from `apache/airflow:3.3.0-python3.12`.
#
# Deliberately NOT `_PIP_ADDITIONAL_REQUIREMENTS` (Airflow's own env-var mechanism for
# adding packages at container start): the Airflow project's own documentation calls this
# "very bad and dangerous... useful only when iterating and debugging" -- it re-resolves
# from PyPI on every pod start with no lockfile, non-deterministic across restarts and a
# direct violation of this project's determinism constraint (.claude/CLAUDE.md §67). A
# real, git-SHA-tagged, built-once image is the only acceptable mechanism here, matching
# the INFRA-08 precedent `docker/csv-processor/Dockerfile` already established.
#
# The pinned constraints URL below matches STACK.md's own installation recipe exactly
# (T-07-11, mitigated in this plan's threat register): never an unpinned/`latest`
# constraints fetch, so `apache-airflow[otel]`'s transitive package set resolves under
# Airflow's own tested pin set for 3.3.0, not whatever PyPI happens to publish that day.
ARG GIT_SHA=unknown

FROM apache/airflow:3.3.0-python3.12

# Re-declared after FROM: an ARG declared only before FROM is scoped to the FROM
# instruction itself and is NOT visible to later instructions in this (single) stage --
# without this line, `${GIT_SHA}` in the LABEL block below would expand to an empty string.
ARG GIT_SHA

# No `--user` flag, no `USER root` switch: the upstream `apache/airflow` image is built to
# allow exactly this customization as the already-active, already-non-root `airflow` user
# (Airflow's own documented "Customizing the image" pattern -- installing an extra provider
# via a plain `RUN pip install` on top of the base image, no privilege change). This file
# adds no USER directive of its own; the base image's own non-root identity carries through
# unchanged.
RUN pip install --no-cache-dir "apache-airflow[otel]==3.3.0" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.12.txt"

# Phase 8 (LOAD-10): `airflow/dags/_common/integrity_gate.py` imports `psycopg`
# directly at DAG-parse time -- ADR-0004/D-18/D-20's "no dataplat import, no KPO
# pod launch" rule means it writes its own narrow `meta.files` INSERT inline,
# so it needs psycopg in THIS image, not only in the csv-processor image.
# Pinned to the exact version `uv.lock` resolves elsewhere in this project
# (psycopg[binary]==3.3.4), never a floating range, matching this
# Dockerfile's own determinism reasoning above.
RUN pip install --no-cache-dir "psycopg[binary]==3.3.4"

# Plan 11-08 post-merge fix: `airflow/dags/_common/retention_query.py` imports
# `dataplat.config.model.DatasetConfig` and `dataplat.retention.policy`
# directly at DAG-parse time -- that module's own docstring documents this as
# a FIFTH, narrower ADR-0004 exception (pure, I/O-free evaluator/contract
# modules, no CSV-parsing or database-writing side effects of their own),
# and 11-08-PLAN.md's own Interfaces section names this exact wiring as
# intended. No prior plan actually added `dataplat` itself to this image
# though, so the intent never worked: confirmed live, this fix,
# `airflow dags list-import-errors` reported
# `ModuleNotFoundError: No module named 'dataplat'` for both
# `platform_retention.py` and `_common/retention_query.py` before this line
# existed.
#
# `--no-deps`, deliberately NOT a bare `pip install packages/dataplat`
# (which resolves dataplat's full `[project.dependencies]` tree --
# packages/dataplat/pyproject.toml declares PyYAML, psycopg, boto3>=1.43.68,
# pydantic, structlog, click, hvac, opentelemetry-sdk>=1.44,
# opentelemetry-exporter-otlp-proto-http>=1.44). Verified directly, this fix
# (`pip list` inside `apache/airflow:3.3.0-python3.12` plus the `[otel]`/
# psycopg layers above): every import `dataplat.config.model` /
# `dataplat.retention.policy` / `dataplat.observability.logging` actually
# reach -- pydantic (2.13.4) and structlog (26.1.0) -- is ALREADY present in
# this image and already satisfies dataplat's own declared ranges
# (`pydantic>=2.13,<3`, `structlog>=26,<27`). But this image's `boto3`
# (1.43.0, from `apache-airflow-providers-amazon`) and both
# `opentelemetry-*` packages (1.43.0, from the `[otel]` extra above) sit
# BELOW dataplat's pins -- a dependency-resolving install would upgrade them
# past whatever Airflow's own providers/extras need, which is exactly the
# uncontrolled-drift ADR-0004 exists to prevent (see docs/adr/0004-two-
# images-two-dependency-sets.md). `--no-deps` installs only dataplat's own
# code against what THIS image already has; it triggers no resolver pass
# that could touch a version any other RUN in this file pinned.
#
# `--no-deps` does not skip pip verifying that an ALREADY-installed
# dependency satisfies dataplat's declared range -- it only skips resolving
# NEW installs for missing ones. A genuinely missing/incompatible transitive
# import would still fail loudly, just later: at DAG-parse time
# (`airflow dags list-import-errors`, checked below) or in
# `tests/dagtest/test_platform_retention_dagrun.py`'s `dag.test()` proof
# against this exact image -- never silently.
#
# `--chown=airflow:0`: the base image runs as the already-non-root `airflow`
# user (uid 50000, gid 0 -- verified: `docker run --entrypoint id
# apache/airflow:3.3.0-python3.12`), matching this Dockerfile's own "no
# `USER root` switch" convention above. Without `--chown`, `COPY` defaults to
# root:root ownership and the final `rm -rf packages/` cleanup below fails
# with `Permission denied` for every copied file (observed directly, this
# fix) -- `airflow` cannot remove files it does not own.
#
# The two-COPY-then-RUN-then-remove shape (rather than leaving
# `packages/dataplat/` in the final image) keeps this image's only durable
# artifact the installed distribution itself -- `packages/` here is
# transient build input, not shipped application source, mirroring
# `docker/csv-processor/Dockerfile`'s own `builder`-stage-only source tree
# (this file stays single-stage; the same intent is achieved with a
# same-layer `&& rm -rf` instead of a second stage, since this is one
# narrow addition, not a full workspace `uv sync`).
COPY --chown=airflow:0 packages/dataplat/pyproject.toml packages/dataplat/pyproject.toml
COPY --chown=airflow:0 packages/dataplat/src packages/dataplat/src
RUN pip install --no-cache-dir --no-deps packages/dataplat/ && rm -rf packages/

# INFRA-08-style replayability, the identical convention docker/csv-processor/Dockerfile
# already uses: which commit built this image, answerable from the running container's own
# metadata, without a registry lookup. No semver release for this image exists yet, so its
# version IS its git SHA -- same as csv-processor's own LABEL block.
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.source="https://github.com/KonuTech/airflow-platform" \
      org.opencontainers.image.version="${GIT_SHA}"
