# docker/csv-processor/Dockerfile -- the ETL pod image (Phase 3 success criterion 3).
#
# Contains no `apache-airflow` distribution (ADR-0004): the workspace this
# image installs from names exactly `dataplat` and `csv-processor`, never
# Airflow. `ENTRYPOINT ["dataplat"]` invokes the console script
# `packages/dataplat/pyproject.toml`'s `[project.scripts]` declares (plan
# 03-07 Task 1), so `docker run <image> dataplat --version` prints the
# installed distribution's version -- success criterion 3, proven by
# tests/integration/test_docker_image.py (Task 3).
#
# Layer ordering is load-bearing, not stylistic (ADR-0004 "Consequences"):
# the dependency-only layer runs `uv sync --frozen --no-install-workspace`
# BEFORE `packages/` is copied in, because `--locked` would try to validate
# member `pyproject.toml` files whose `src/` trees are not yet present and
# fail. Only after `packages/` exists does the build switch to `--locked`
# (full lockfile re-verified). Collapsing this into one `uv sync` call, or
# reordering the two COPY/RUN pairs, breaks the dependency layer's cache
# reuse across source-only changes -- see docs/adr/0004-two-images-two-
# dependency-sets.md.
#
# `--all-packages --no-dev` on BOTH invocations, not bare `--no-dev`: the root
# `pyproject.toml`'s virtual workspace only lists `dataplat`/`csv-processor`
# inside the `dev` dependency-group (see that file's own comment on why --
# ROADMAP criterion 4's bare `uv sync && make check`). A bare `--no-dev` here
# therefore excludes the ONE group that names either workspace member, so
# NEITHER package nor its dependencies would resolve at all -- observed
# directly: the runtime image built, but `dataplat` was absent from
# `.venv/bin`, and `docker run <image> dataplat --version` failed with
# "executable file not found in $PATH". `--all-packages` selects every
# workspace member directly (bypassing the root's dependency-group
# indirection entirely), so combined with `--no-dev` it installs exactly
# `dataplat` + `csv-processor` and their own `[project.dependencies]` --
# never pytest/ruff/mypy/alembic/sqlalchemy/boto3-stubs/pre-commit/
# import-linter, all of which are dev-only tooling with no place in a
# runtime image.
#
# `--no-editable` on the second (full) sync: uv installs local path/workspace
# dependencies editable by default -- a `.pth`-style pointer back to
# `/app/packages/*/src`, not a copied package. The `runtime` stage below
# copies ONLY `/app/.venv` out of `builder`, never `/app/packages`, so an
# editable install left the venv's own `dataplat` unimportable at runtime
# (`ModuleNotFoundError: No module named 'dataplat'`, observed directly).
# `--no-editable` makes uv install the workspace members as real, self-
# contained packages under `.venv/site-packages`, which is what makes copying
# only `.venv` into `runtime` correct.
#
# `configs/` (plan 04-05): the `dataplat discover --dataset <name>` CLI
# command resolves `configs/datasets/<name>.yaml` + `configs/defaults.yaml`
# as paths relative to the process's own working directory, via
# `dataplat.config.loader.load_config`. `ingest` never reads `configs/` at
# all -- it resolves its `DatasetConfig` from `meta.config_versions` by ID
# instead (`ConfigRegistry.get_by_id`), so this image ships `configs/`
# purely for `discover`'s benefit. `COPY configs/ configs/` is copied AFTER
# the dependency-resolution layer (it is not a `uv sync` input) but still
# inside the `builder` stage, then copied again into `runtime` alongside
# `/app/.venv` -- and `runtime` now declares `WORKDIR /app` explicitly
# (previously absent: without it, the container's default working directory
# is `/`, and `discover`'s relative `Path("configs/...")` would resolve to
# `/configs/...`, not `/app/configs/...`, even though the files are present).
ARG GIT_SHA=unknown

FROM ghcr.io/astral-sh/uv:0.12.3 AS uv
FROM python:3.12-slim-bookworm AS builder
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

# Dependency layer only -- workspace member SOURCE is not copied in yet, so
# --frozen (not --locked): --locked would try to validate member pyproject
# files whose src/ trees aren't present, and fail.
COPY pyproject.toml uv.lock ./
COPY packages/dataplat/pyproject.toml packages/dataplat/pyproject.toml
COPY packages/csv-processor/pyproject.toml packages/csv-processor/pyproject.toml
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-workspace --all-packages --no-dev

# Now the member sources exist -- switch to --locked (full lockfile re-verified).
COPY packages/ packages/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --all-packages --no-dev --no-editable

# Not a uv sync input -- copied after the dependency-resolution layer so a
# configs-only change never invalidates that layer's cache. See the header
# comment block above for why discover needs this and runtime needs a
# matching WORKDIR.
COPY configs/ configs/

FROM python:3.12-slim-bookworm AS runtime
ARG GIT_SHA
RUN groupadd -r app -g 1000 && useradd -r -g app -u 1000 -m app
WORKDIR /app
COPY --from=builder --chown=app:app /app/.venv /app/.venv
COPY --from=builder --chown=app:app /app/configs /app/configs
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1

# T-03-15 / INFRA-08 replayability: which commit built this image, answerable
# from the running container's own metadata, without a registry lookup. The
# same GIT_SHA string is what `make image-csv-processor` also uses as the
# image TAG (below) -- one build-time input, two places it shows up, never
# out of step. `.version` is this image's own version, distinct from
# `dataplat`'s installed package version (see `dataplat --version`): there is
# no semver release for this image yet, so its version IS its git SHA, same
# as INFRA-08 mandates for the tag itself.
LABEL org.opencontainers.image.revision="${GIT_SHA}" \
      org.opencontainers.image.source="https://github.com/KonuTech/airflow-platform" \
      org.opencontainers.image.version="${GIT_SHA}"

USER 1000
# `dataplat` is the console-script name (packages/dataplat/pyproject.toml
# [project.scripts]), NOT `csv-processor` -- that's only the image's name
# (ADR-0004). Success criterion 3 literally invokes `dataplat --version`.
ENTRYPOINT ["dataplat"]
