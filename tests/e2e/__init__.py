"""Live-service test suites — never collected by the offline gate.

`tests/e2e/cluster/` is the only subtree today (D-16). It is reachable
through `make cluster-verify` alone; see that target's comment in the
Makefile and `tests/policy/test_offline_gate_stays_offline.py` for the gate
that keeps it that way.
"""
