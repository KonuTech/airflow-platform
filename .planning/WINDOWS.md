---
schema_version: 1
open_count: 7
waived_count: 0
fixed_count: 2
total_count: 9
last_updated: 2026-08-12T05:14:19.705Z
---

# Broken Windows Ledger

> Cross-phase defect register. With `workflow.windows_enforce` enabled, `/gsd-ship` blocks while `open_count > 0`.
> Waive with `gsd-tools windows waive <id> "<reason>"` (reason required).
> Mark fixed with `gsd-tools windows fixed <id>`.

| id | phase | kind | file | line | description | status | reason | recorded_at | resolved_at |
|----|-------|------|------|------|-------------|--------|--------|-------------|-------------|
| 1 | 01 | unrun-verify | tools/security/install_gitleaks.sh |  | Installer fail-closed path has only static ordering coverage (verify-before-extract); the corrupted-download behaviour is still uncovered by an automated test | open |  | 2026-08-11T17:46:09.436Z |  |
| 2 | 01 | unrun-verify | docs/ci-branch-protection.md |  | Branch protection rule specified but NOT applied; no CI run ever observed. Environment denied git push and gh run list. CICD-01/CICD-02/SEC-02/SEC-10 remain open. | fixed |  | 2026-08-11T19:55:18.744Z | 2026-08-11T20:11:19.647Z |
| 3 | 01 | unrun-verify | tools/security/install_gitleaks.sh |  | CR-03: gitleaks tarball and checksums.txt are fetched from the same base_url, so the digest catches transit corruption but not the source tampering threat T-01-09 names. Also an existing binary in gitignored tools/bin/ is EXECUTED to read its version before any verification. Fix: commit the per-platform SHA-256 as trust anchor; verify before executing. | fixed |  | 2026-08-11T20:28:22.993Z | 2026-08-11T21:01:17.971Z |
| 4 | 01 | unrun-verify | tests/regression/ |  | CR-review WR: tests/regression/ is run by NO Makefile target, so 01-04's QUAL-07 provenance-enforcing collection hook never executes in make check or make ci. Hook verified by hand but not wired into any gate. | open |  | 2026-08-11T20:28:31.390Z |  |
| 5 | 01 | unmet-truth | tests/policy/test_ci_invokes_make_only.py |  | CR-review WR: regex for 'make gitleaks' also matches 'make gitleaks-selftest', so test_a_scanning_job_exists_at_all is satisfied by a workflow that runs only the self-test and never the real full-history scan. | open |  | 2026-08-11T20:28:31.466Z |  |
| 6 | 01 | fixme | tools/corpus/generators.py |  | CR-review WR: _decimal_renderer renders negative decimals incorrectly (-1234 -> -13.66), affecting generated numeric fixture content. | open |  | 2026-08-11T20:28:31.541Z |  |
| 7 | 01 | todo | .gitignore |  | CR-review WR-04: .gsd/ is untracked and unignored; it holds a run-scoped dispatch sentinel that will be swept into a commit during phase 2. Add .gsd/ to .gitignore. | open |  | 2026-08-11T20:28:31.616Z |  |
| 8 | 01 | todo | Makefile |  | Phase 3 must wire tests/property, tests/integration and tests/e2e into a target that can provide testcontainers or a live cluster. make check names test paths explicitly, so a new test directory is silently uncollected until named — the defect that made QUAL-07 partial. Do not assume make check collects them. | open |  | 2026-08-11T20:40:04.033Z |  |
| 9 | 02 | todo | Makefile |  | CONTEXT.md D-Claude's-discretion: 'make clean-images' pruning each kind node's containerd image store was left unplanned for Phase 2 (orthogonal to all five success criteria). PITFALLS A3 flags this as the cleanup step everyone forgets and the reason WSL2 disk keeps climbing after a 'cleanup' — pruning the host Docker daemon does NOT touch images already loaded into node containerd stores. Build it when disk pressure first bites, via: docker exec <node> crictl rmi --prune across all nodes. | open |  | 2026-08-12T05:14:19.705Z |  |

````json
[
  {
    "id": 1,
    "kind": "unrun-verify",
    "phase": "01",
    "file": "tools/security/install_gitleaks.sh",
    "line": null,
    "description": "Installer fail-closed path has only static ordering coverage (verify-before-extract); the corrupted-download behaviour is still uncovered by an automated test",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-11T17:46:09.436Z",
    "resolved_at": null
  },
  {
    "id": 2,
    "kind": "unrun-verify",
    "phase": "01",
    "file": "docs/ci-branch-protection.md",
    "line": null,
    "description": "Branch protection rule specified but NOT applied; no CI run ever observed. Environment denied git push and gh run list. CICD-01/CICD-02/SEC-02/SEC-10 remain open.",
    "status": "fixed",
    "reason": "",
    "recorded_at": "2026-08-11T19:55:18.744Z",
    "resolved_at": "2026-08-11T20:11:19.647Z"
  },
  {
    "id": 3,
    "kind": "unrun-verify",
    "phase": "01",
    "file": "tools/security/install_gitleaks.sh",
    "line": null,
    "description": "CR-03: gitleaks tarball and checksums.txt are fetched from the same base_url, so the digest catches transit corruption but not the source tampering threat T-01-09 names. Also an existing binary in gitignored tools/bin/ is EXECUTED to read its version before any verification. Fix: commit the per-platform SHA-256 as trust anchor; verify before executing.",
    "status": "fixed",
    "reason": "",
    "recorded_at": "2026-08-11T20:28:22.993Z",
    "resolved_at": "2026-08-11T21:01:17.971Z"
  },
  {
    "id": 4,
    "kind": "unrun-verify",
    "phase": "01",
    "file": "tests/regression/",
    "line": null,
    "description": "CR-review WR: tests/regression/ is run by NO Makefile target, so 01-04's QUAL-07 provenance-enforcing collection hook never executes in make check or make ci. Hook verified by hand but not wired into any gate.",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-11T20:28:31.390Z",
    "resolved_at": null
  },
  {
    "id": 5,
    "kind": "unmet-truth",
    "phase": "01",
    "file": "tests/policy/test_ci_invokes_make_only.py",
    "line": null,
    "description": "CR-review WR: regex for 'make gitleaks' also matches 'make gitleaks-selftest', so test_a_scanning_job_exists_at_all is satisfied by a workflow that runs only the self-test and never the real full-history scan.",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-11T20:28:31.466Z",
    "resolved_at": null
  },
  {
    "id": 6,
    "kind": "fixme",
    "phase": "01",
    "file": "tools/corpus/generators.py",
    "line": null,
    "description": "CR-review WR: _decimal_renderer renders negative decimals incorrectly (-1234 -> -13.66), affecting generated numeric fixture content.",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-11T20:28:31.541Z",
    "resolved_at": null
  },
  {
    "id": 7,
    "kind": "todo",
    "phase": "01",
    "file": ".gitignore",
    "line": null,
    "description": "CR-review WR-04: .gsd/ is untracked and unignored; it holds a run-scoped dispatch sentinel that will be swept into a commit during phase 2. Add .gsd/ to .gitignore.",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-11T20:28:31.616Z",
    "resolved_at": null
  },
  {
    "id": 8,
    "kind": "todo",
    "phase": "01",
    "file": "Makefile",
    "line": null,
    "description": "Phase 3 must wire tests/property, tests/integration and tests/e2e into a target that can provide testcontainers or a live cluster. make check names test paths explicitly, so a new test directory is silently uncollected until named — the defect that made QUAL-07 partial. Do not assume make check collects them.",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-11T20:40:04.033Z",
    "resolved_at": null
  },
  {
    "id": 9,
    "kind": "todo",
    "phase": "02",
    "file": "Makefile",
    "line": null,
    "description": "CONTEXT.md D-Claude's-discretion: 'make clean-images' pruning each kind node's containerd image store was left unplanned for Phase 2 (orthogonal to all five success criteria). PITFALLS A3 flags this as the cleanup step everyone forgets and the reason WSL2 disk keeps climbing after a 'cleanup' — pruning the host Docker daemon does NOT touch images already loaded into node containerd stores. Build it when disk pressure first bites, via: docker exec <node> crictl rmi --prune across all nodes.",
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-12T05:14:19.705Z",
    "resolved_at": null
  }
]
````
