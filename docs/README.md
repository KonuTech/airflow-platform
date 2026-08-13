# Repository map

Every top-level directory below exists from Phase 1 so that later phases add
files rather than invent locations. A directory carrying only a `.gitkeep` is
deliberately empty; the "Filled by" column names the phase that populates it.

The structure reconciles README §75 with the two-package split recorded in
ADR-0002 (plan 01-04). Where it departs from README §75, the departure is
noted.

| Path | Purpose | Filled by |
|---|---|---|
| `packages/dataplat/` | Source-agnostic ETL platform core. Never imports the CSV plugin. | Phase 1 (skeleton), Phase 3 (library) |
| `packages/csv-processor/` | CSV source plugin for `dataplat`. Departure from README §75, which placed `csv_processor/` at the repository root. | Phase 1 (marker), Phase 3 / 6 |
| `airflow/dags/` | DAG files. A leaf: nothing imports it. Stays empty until the first DAG, which is when the import-linter contract forbidding imports of it becomes non-vacuous. | Phase 4 |
| `airflow/config/` | Airflow configuration fragments. | Phase 2 |
| `configs/` | §65 dataset configurations. | Phase 3 |
| `schemas/` | §22 dataset contracts. | Phase 6 |
| `migrations/` | Alembic revisions for the analytical database. Hand-written, never bare autogenerate output. | Phase 3 |
| `docker/airflow/` | Airflow image. Installs providers under Airflow's own constraints file — deliberately outside the uv workspace. | Phase 2 |
| `docker/csv-processor/` | ETL image. Contains no Airflow distribution. | Phase 3 |
| `kubernetes/` | Cluster manifests and the kind cluster definition. | Phase 2 |
| `helm/values/local/` | Full multi-node profile values. | Phase 2 |
| `helm/values/ci/` | Trimmed single-node profile for a 4 CPU / 16 GB runner. Written in Phase 2 even though Phase 11 consumes it — retrofitting profile parameterization is expensive. | Phase 2 |
| `scripts/` | Operator shell and Python scripts. The only tree where `print()` is permitted, via a ruff `per-file-ignores` carve-out scoped to this path alone. | Phase 2 onward |
| `docs/` | Project documentation. `docs/adr/` holds the decision log. | Phase 1 (ADRs 0001–0005), then per phase |
| `tools/` | Repository tooling that is neither library code nor a shell script — the fixture corpus generator and the gitleaks self-test. Not in README §75: it lives here rather than in `scripts/` so it stays under the `print()` ban and mypy strict. | Phase 1 (plans 01-02, 01-03) |
| `tests/unit/` | Pure tests, no I/O. | Phase 1 onward |
| `tests/policy/` | Repository rules expressed as tests — architecture bans and CI/Make parity. Runs locally via `make check`, not only in CI. | Phase 1 |
| `tests/regression/` | QUAL-07: one permanent test per fixed bug, each naming its bug. | Phase 1 (plan 01-04) |
| `tests/integration/` | testcontainers-backed tests: PostgreSQL and MinIO, no cluster. | Phase 3 |
| `tests/property/` | Hypothesis property tests. | Phase 6 |
| `tests/e2e/` | End-to-end tests against an ephemeral kind cluster. | Phase 11 |
| `tests/fixtures/` | `corpus.yaml` (the specification) and `CORPUS.sha256` (the oracle). The generated corpus under `tests/fixtures/csv/` is gitignored and never committed. | Phase 1 (plan 01-03) |

## Where the gate is defined

`Makefile` is the only definition of the quality gate. `.github/workflows/ci.yml`
calls `make install` and `make check` and defines no check of its own, so the
command a developer runs locally and the command CI runs cannot drift apart.
