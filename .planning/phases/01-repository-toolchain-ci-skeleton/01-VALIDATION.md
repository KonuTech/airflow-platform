---
phase: 1
slug: repository-toolchain-ci-skeleton
# status lifecycle: draft (seeded by plan-phase) → validated (set by validate-phase §6)
# audit-milestone §5.5 distinguishes NOT-VALIDATED (draft) from PARTIAL (validated + nyquist_compliant: false) (#2117)
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-08-11
---

# Phase 1 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: `01-RESEARCH.md` § Validation Architecture (line 1180), which maps all 12 requirements
> to observable signal, mechanism and cadence.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest `9.1.1` (+ pytest-cov `7.1.0`, hypothesis `6.165.3`) — **Wave 0 installs** |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` — none exists yet, Wave 0 creates |
| **Quick run command** | `uv run pytest -q` |
| **Full suite command** | `make check` (ruff → mypy → import-linter → pytest → policy greps) |
| **Estimated runtime** | ~15–30 s (no cluster, no containers, no network this phase) |

**Parity requirement:** CI and local MUST invoke the same entry point, so they cannot drift.
`make check` is the single definition; the GitHub Actions job calls it rather than re-listing steps.

---

## Sampling Rate

- **After every task commit:** `uv run pytest -q`
- **After every plan wave:** `make check`
- **Before `/gsd-verify-work`:** full suite green on a clean checkout (`uv sync && make check`)
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

Populated by the planner. Each of the 12 phase requirements must appear against at least one task.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| *(pending planner)* | — | — | — | — | — | — | — | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Requirement → Mechanism Summary

Condensed from `01-RESEARCH.md` § Validation Architecture. Full detail lives there.

| Requirement | Mechanism | Cadence |
|-------------|-----------|---------|
| QUAL-01 type hints | mypy strict (flags enumerated individually — `strict=false` in `[[tool.mypy.overrides]]` is silently ignored by mypy 2.3.0) | every PR |
| QUAL-02 docstrings | ruff `D` pydocstyle, scoped to public API | every PR — **partial**, presence not quality |
| QUAL-07 regression tests | `tests/regression/` exists + policy documented | phase acceptance |
| QUAL-08 fixture corpus | `make fixtures` + committed SHA-256 digest manifest | every PR |
| CICD-01 GitHub Actions | workflow file present and running | every PR |
| CICD-02 PR quality gate | required checks on PR | every PR |
| CICD-03 ruff | `ruff check` non-zero on violation | every PR |
| CICD-04 mypy | `mypy` non-zero on violation | every PR |
| SEC-02 no secrets in history | `gitleaks detect` over full history | every PR + phase acceptance |
| SEC-10 no secret echoed in CI | Phase-1 form ("workflow references no secret") is checkable; general form is undecidable | phase acceptance — **partial** |
| SEC-11 secret scanning in CI | negative test: a planted synthetic credential fails the build | every PR |
| OBS-03 no `print()` | ruff `T20`, scoped to library packages | every PR |

**Honest limitations carried forward from research — do not paper over:**
- **SEC-10** is partly undecidable in general (a future `curl -v` carrying a bearer token matches no grep). Its Phase-1 form is fully checkable and that is what this phase asserts.
- **QUAL-02** verifies a docstring *exists*, not that it is meaningful.
- The **gitleaks negative test must not use `AKIAIOSFODNN7EXAMPLE`** — gitleaks 8.30.1 stopword-lists it and does not flag it, so a test built on that string asserts the opposite of what it claims.

---

## Wave 0 Requirements

- [ ] `pyproject.toml` — pytest config, ruff config, mypy config for both workspace members
- [ ] `tests/conftest.py` — shared fixtures
- [ ] `tests/unit/`, `tests/regression/` — tree established while empty (QUAL-07 policy)
- [ ] pytest + pytest-cov + hypothesis installed via `uv sync`
- [ ] `make check` target defined as the single CI/local entry point

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Branch protection requires the CI checks | CICD-02, success criterion 1 | GitHub repository setting, not a repo file — CI is advisory without it | Repo → Settings → Branches → require `check` status before merge; confirm a failing PR cannot be merged |
| CI log contains no secret values | SEC-10 | General form undecidable; needs human read of a real run's logs | Open the most recent Actions run, confirm no job echoes a credential |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
