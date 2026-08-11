---
schema_version: 1
open_count: 2
waived_count: 0
fixed_count: 0
total_count: 2
last_updated: 2026-08-11T19:55:18.744Z
---

# Broken Windows Ledger

> Cross-phase defect register. With `workflow.windows_enforce` enabled, `/gsd-ship` blocks while `open_count > 0`.
> Waive with `gsd-tools windows waive <id> "<reason>"` (reason required).
> Mark fixed with `gsd-tools windows fixed <id>`.

| id | phase | kind | file | line | description | status | reason | recorded_at | resolved_at |
|----|-------|------|------|------|-------------|--------|--------|-------------|-------------|
| 1 | 01 | unrun-verify | tools/security/install_gitleaks.sh |  | Installer fail-closed path has only static ordering coverage (verify-before-extract); the corrupted-download behaviour is still uncovered by an automated test | open |  | 2026-08-11T17:46:09.436Z |  |
| 2 | 01 | unrun-verify | docs/ci-branch-protection.md |  | Branch protection rule specified but NOT applied; no CI run ever observed. Environment denied git push and gh run list. CICD-01/CICD-02/SEC-02/SEC-10 remain open. | open |  | 2026-08-11T19:55:18.744Z |  |

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
    "status": "open",
    "reason": "",
    "recorded_at": "2026-08-11T19:55:18.744Z",
    "resolved_at": null
  }
]
````
