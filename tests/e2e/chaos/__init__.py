"""The live-cluster chaos-testing harness (QUAL-15, README §84).

Every module here carries `pytestmark = [pytest.mark.cluster, pytest.mark.chaos]` and requires a
running `kind-airflow-platform` cluster -- run `make cluster-up` first. Unlike
`tests/e2e/cluster`/`tests/e2e/slice`/`tests/e2e/observability` (collected together by `make
cluster-verify`), this suite is DELIBERATELY excluded from that target -- it is not named in
`cluster-verify`'s own `pytest tests/e2e/cluster tests/e2e/slice tests/e2e/observability`
invocation, mirroring how `tests/e2e/vault` already has its own dedicated `vault-verify` target
rather than joining that list. Every test here breaks a real, live platform component on purpose
to prove the platform recovers, and `cluster-verify` is the standing per-wave gate other phases
run unattended -- folding a deliberately-destructive suite into it would risk leaving the shared
cluster broken for whatever else depends on it if a teardown itself has a bug (see this suite's
own `conftest.py` fault-injection fixtures and the T-11-22 threat-register entry, both designed
around a guaranteed-restoration `finally`). Run explicitly:
`uv run --group cluster pytest tests/e2e/chaos -m cluster`, or (once wired, plan 11-10/D-25..D-27)
via a dedicated CI job on its own ephemeral cluster.

QUAL-15 (README §84's Failure Scenarios, DoD 89) names 11 scenarios in REQUIREMENTS.md's
traceability table. This plan (11-09) builds the first four -- the infrastructure-unavailability
group, each with a shared, guaranteed-restoring fault-injection fixture in `conftest.py`:

1. Pod crashes             -- test_pod_crash.py
2. Database unavailable    -- test_database_unavailable.py
3. MinIO unavailable       -- test_minio_unavailable.py
4. Vault unavailable       -- test_vault_unavailable.py

Plan 11-10 builds the remaining 7, reusing this same scaffolding:

5. Malformed CSV
6. Invalid encoding
7. Out-of-memory
8. Task timeout
9. Duplicate batch
10. Secret rotation
11. Unauthorized secret access
"""
