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

# INFRA-08-style replayability, the identical convention docker/csv-processor/Dockerfile
# already uses: which commit built this image, answerable from the running container's own
# metadata, without a registry lookup. No semver release for this image exists yet, so its
# version IS its git SHA -- same as csv-processor's own LABEL block.
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.source="https://github.com/KonuTech/airflow-platform" \
      org.opencontainers.image.version="${GIT_SHA}"
