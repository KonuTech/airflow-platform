# Phase 5: Vault Secrets & Workload Identity - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-08-14
**Phase:** 05-vault-secrets-workload-identity
**Areas discussed:** Credential Migration Scope, Unseal Ceremony, Rotation Proof Depth, Audit Visibility Tooling

---

## Credential Migration Scope

| Option | Description | Selected |
|--------|-------------|----------|
| Delete/empty them | Once a credential is served from Vault, remove or blank the old K8s Secret so it can't silently be a fallback — matches SEC-01's literal "only source" wording and makes the migration provable, not just additive. | ✓ |
| Leave them in place, unused | Vault becomes the path pods actually read from, but the old Secrets stay as inert leftovers (e.g. for an emergency rollback). Weaker proof that Vault is truly the ONLY source. | |
| Let Claude decide per-secret | Some may be worth keeping briefly for rollback safety, others removed immediately — Claude judges case by case during planning. | |

**User's choice:** Delete/empty them (Recommended)
**Notes:** Question raised as a single batched round covering all four areas at once; user selected the Recommended option for every question without follow-up discussion.

---

## Unseal Ceremony

| Option | Description | Selected |
|--------|-------------|----------|
| Scripted single-command unseal | A make target (e.g. `make vault-unseal`) reads locally-stored, gitignored unseal key(s) and unseals automatically. Real persistent storage + real seal/unseal mechanics, but zero manual ceremony friction for daily WSL2 restarts. | ✓ |
| Manual multi-key-share ceremony | The production-realistic Shamir's Secret Sharing unseal (multiple key holders, threshold to unseal) — most rigorous, but a real manual step every restart, which on WSL2 means most mornings. | |
| Let Claude decide | Claude picks the approach that best balances rigor and daily usability during planning. | |

**User's choice:** Scripted single-command unseal (Recommended)
**Notes:** Explicitly framed as a local-dev-only convenience in CONTEXT.md — production substitution story (SEC-14) should still describe the real ceremony/auto-unseal alternative.

---

## Rotation Proof Depth

| Option | Description | Selected |
|--------|-------------|----------|
| Live-demonstrated rotation | An automated test actually rotates a Vault-stored credential and asserts a running workload observes the new value without a pod restart — proof, not just prose. | ✓ |
| Documentation only | Write down which credentials are read once at pod-start (need a restart to rotate) vs. read per-use (rotate live), without building an automated proof. Smaller scope for this phase. | |
| Let Claude decide | Claude judges the right depth during planning based on how much the mechanism naturally falls out of the Vault delivery design. | |

**User's choice:** Live-demonstrated rotation (Recommended)
**Notes:** Scoped in CONTEXT.md to one credential path demonstrated end-to-end, not an exhaustive rotation test of every credential.

---

## Audit Visibility Tooling

| Option | Description | Selected |
|--------|-------------|----------|
| Convenience script | A small script/make target (e.g. `make vault-audit-tail`) parses and tails the audit log in a readable form — matches the developer-experience bar Phase 4 set with `make ingest-demo`. | ✓ |
| Raw log file is enough | The audit log exists on disk in Vault's standard JSON format; a human greps/jqs it directly when needed. No dedicated tooling built. | |
| Let Claude decide | Claude decides based on how much the audit log's raw format actually needs translating to be usable. | |

**User's choice:** Convenience script (Recommended)
**Notes:** Command name is a suggestion, not locked — should match Phase 4's `make image-csv-processor`/`make ingest-demo`/`make cluster-verify` naming convention.

---

## Claude's Discretion

- Secret delivery mechanism at the `SecretsResolver` level — `vault://` direct SA-token login via `hvac`, per STACK.md's HIGH-confidence recommendation, not the Agent Injector pattern the resolver's own docstring example superficially suggests.
- Exact Vault deployment topology, unseal-key storage format for the local convenience script, and whether a distinct bootstrap/root-token auth tier is needed alongside the workload-facing Kubernetes-auth tier.
- Whether SEC-12's negative test needs a third identity beyond `default`, or whether `default` alone satisfies the requirement.

## Deferred Ideas

None — discussion stayed within phase scope. Observability (Prometheus/Grafana/OTel) was not re-raised; it remains Phase 7's explicitly scoped territory per ROADMAP.md.
