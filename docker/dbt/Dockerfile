# docker/dbt/Dockerfile -- the third single-purpose image (ADR-0004's two-image
# discipline extended to three, per 08.1-RESEARCH.md's Anti-Patterns table).
#
# Contains no `dataplat`, `csv_processor` or `apache-airflow` distribution: the
# only things this image installs are `dbt-core`/`dbt-postgres` (via `uv pip
# install --system`, a flat non-venv install -- there is no local
# `pyproject.toml`/`uv.lock` for dbt to sync against, unlike
# `docker/csv-processor/Dockerfile`'s workspace-member install). No builder/
# runtime split is needed either, for the same reason: there is no compiled
# venv to copy out of a separate stage.
#
# `uv` itself is still sourced the SAME way `docker/csv-processor/Dockerfile`
# does -- `FROM ghcr.io/astral-sh/uv:0.12.3 AS uv` as a preceding stage, then
# `COPY --from=uv /uv /usr/local/bin/uv` -- never a bare `pip install`
# (CLAUDE.md names `uv` as this project's packaging tool project-wide, not
# only for `dataplat`/`csv-processor` workspace members).
#
# `ENTRYPOINT ["python", "/app/resolve_secrets.py"]`: the resolver script is
# the entrypoint, not a bare `dbt` invocation, because `profiles.yml`'s
# `env_var()` calls must already be populated (DBT_PG_HOST/PORT/USER/PASSWORD/
# DBNAME) before `dbt` itself starts (08.1-RESEARCH.md's own explicit
# call-out). `resolve_secrets.py` `execvp`s into `dbt` once those env vars are
# set, so PID 1 becomes `dbt` for proper signal handling.
#
# `COPY dbt/ /app/dbt/` names the dbt project directory plan 08.1-08
# populates with real models/sources/profiles.yml. It is safe to reference
# now even though it is empty in this plan's own worktree (only a `.gitkeep`
# placeholder exists here so `docker build` has something to COPY) -- by the
# time anyone runs this build for real end-to-end, plan 08.1-08 has landed
# and replaced the placeholder with the actual dbt project.
ARG GIT_SHA=unknown

FROM ghcr.io/astral-sh/uv:0.12.3 AS uv
FROM python:3.12-slim-bookworm
ARG GIT_SHA
ENV GIT_SHA=${GIT_SHA}

COPY --from=uv /uv /usr/local/bin/uv
RUN groupadd -r app -g 1000 && useradd -r -g app -u 1000 -m app

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Flat, non-venv install -- `--system` because there is no local
# pyproject.toml/uv.lock for dbt to sync against (unlike
# docker/csv-processor/Dockerfile's workspace-member `uv sync`). Never a bare
# `pip install` (CLAUDE.md's project-wide packaging-tool mandate). `hvac`
# pinned to the SAME version the rest of the platform uses (Airflow's own
# constraints pin it too) -- resolve_secrets.py's only runtime dependency
# beyond the stdlib.
# D-07 (2026-08-23): dbt-core 1.12.2's own pinned dependency range
# (`sqlparse<0.6.0,>=0.5.5`) made CVE-2026-54284/CVE-2026-59893/CVE-2026-71491
# (HIGH, fixed in sqlparse 0.6.0) unfixable at that pin -- trivy's first real
# scan of this image caught it. dbt-core 1.12.3 (a same-day patch release,
# verified via `pip index versions dbt-core`) relaxes the constraint to
# `sqlparse<0.7.0,>=0.5.5`, which lets uv resolve sqlparse>=0.6.0 -- a real
# upstream fix existed, so this is a version bump, not a .trivyignore
# suppression. dbt-postgres stays at 1.11.0 (already latest; no dbt-core
# 1.12.x-specific adapter pin observed).
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system --no-cache dbt-core==1.12.3 dbt-postgres==1.11.0 hvac==2.4.0

COPY dbt/ /app/dbt/
COPY docker/dbt/resolve_secrets.py /app/resolve_secrets.py

# T-08.1-05 / INFRA-08 replayability: which commit built this image,
# answerable from the running container's own metadata, without a registry
# lookup -- mirrors docker/csv-processor/Dockerfile's identical LABEL triple.
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.source="https://github.com/KonuTech/airflow-platform" \
      org.opencontainers.image.version="${GIT_SHA}"

RUN chown -R app:app /app
ENV PATH="/usr/local/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

USER 1000

# The resolver script is the entrypoint (see header comment above): it
# populates DBT_PG_* env vars from Vault, then `os.execvp`s into `dbt build`
# so PID 1 becomes `dbt` itself.
ENTRYPOINT ["python", "/app/resolve_secrets.py"]
