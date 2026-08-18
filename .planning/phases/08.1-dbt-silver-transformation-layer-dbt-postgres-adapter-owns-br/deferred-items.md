# Deferred Items — Phase 08.1

| Category | Item | Status | Deferred At |
|----------|------|--------|-------------|
| Test flake (order-dependent, pre-existing) | `tests/policy/test_manifest_validation_fails_closed.py`'s 3 tests failed when run as part of the FULL `tests/policy/` suite in one process, but all 5 tests in that module pass cleanly when run standalone. The module is gated behind `pytest.mark.manifests` / `make manifest-policy` (requires `build/manifests/` pre-rendered via `make manifests`) — unrelated to plan 08.1-02's `docker/dbt/`, `Makefile`, or `pyproject.toml` changes, and out of this plan's declared file scope. | Not fixed (out of scope, SCOPE BOUNDARY) | Plan 08.1-02, self-check pass |
