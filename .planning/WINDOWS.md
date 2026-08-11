---
schema_version: 1
open_count: 1
waived_count: 0
fixed_count: 0
total_count: 1
last_updated: 2026-08-11T17:46:09.436Z
---

# Broken Windows Ledger

> Cross-phase defect register. With `workflow.windows_enforce` enabled, `/gsd-ship` blocks while `open_count > 0`.
> Waive with `gsd-tools windows waive <id> "<reason>"` (reason required).
> Mark fixed with `gsd-tools windows fixed <id>`.

| id | phase | kind | file | line | description | status | reason | recorded_at | resolved_at |
|----|-------|------|------|------|-------------|--------|--------|-------------|-------------|
| 1 | 01 | unrun-verify | tools/security/install_gitleaks.sh |  | Installer fail-closed path has only static ordering coverage (verify-before-extract); the corrupted-download behaviour is still uncovered by an automated test | open |  | 2026-08-11T17:46:09.436Z |  |

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
  }
]
````
