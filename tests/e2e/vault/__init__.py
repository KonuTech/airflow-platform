"""The live-cluster Vault verification harness (plan 05-01).

Every module here carries `pytestmark = pytest.mark.cluster` and requires a
running `kind-airflow-platform` cluster with Vault unsealed and bootstrapped
-- run `make cluster-up && make vault-unseal && make vault-bootstrap` first,
or run `make vault-verify`, which requires those same prerequisites.
"""
