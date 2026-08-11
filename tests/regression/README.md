# Regression tests (QUAL-07)

**Every important discovered bug gains a permanent test here, and that test names
the bug it prevents.**

This directory is **intentionally empty of tests** at the end of Phase 1, and that
emptiness is correct: no bug has been discovered yet. The policy is written now,
while the tree is empty, so that the first bug fix in this project meets an
existing convention instead of inventing one under the pressure of a live defect.

## The rule

A test module in this directory must carry a **bug-provenance line**:

```python
# BUG: <issue reference or short slug> - <what used to go wrong>
```

For example:

```python
# BUG: #128 - ragged rows were silently truncated instead of rejected
```

The reference must let a reader find the bug — an issue number, a commit hash, or
a slug used consistently in the test names. The description must state the
**wrong behaviour** the test prevents, not the right one. "Rows are validated" is
not provenance; "ragged rows were silently truncated" is, because it tells the
next reader what breaking this test would mean.

A module without a provenance line **fails collection**. It is not skipped, not
warned about, and its tests are not run — `tests/regression/conftest.py`
substitutes a collector that raises, so the file cannot contribute a passing
test. Failing rather than warning is deliberate: a warning printed in a directory
nobody reads is a note, not a policy.

## Naming

| Thing | Convention |
|---|---|
| File | `test_<slug>.py`, where the slug names the *bug*, not the module it was found in — `test_ragged_row_truncation.py`, not `test_parser.py` |
| Test function | `test_<the_wrong_thing>_is_rejected` / `..._is_preserved` — phrased so the failure message reads as the bug returning |
| Marker | `pytestmark = pytest.mark.regression`, registered in `pyproject.toml`, so `-m regression` selects the whole class of tests |

A complete minimal example:

```python
# BUG: #128 - ragged rows were silently truncated instead of rejected

import pytest

pytestmark = pytest.mark.regression


def test_ragged_row_is_rejected_not_truncated():
    ...
```

The marker is a **convention, not a gate**. The collection hook enforces the
provenance line only. Enforcing the marker too would be easy and was deliberately
not done: one mechanical rule that is always true is worth more than two that
invite exceptions, and the marker's only purpose is selection.

## What a regression test is for

It pins a specific past failure so it cannot return. That makes it different from
the other test directories, and the difference governs how it should be written:

- **Reproduce the bug, not the area.** The test should fail against the code as it
  was before the fix, and pass after. If it would have passed before the fix, it
  is a unit test that belongs in `tests/unit/`.
- **Prefer the smallest input that reproduces.** A regression test that needs the
  whole corpus is one that will be deleted the next time it is inconvenient.
- **Do not refactor it later for elegance.** Its value is that it encodes a real
  failure exactly. Tidying it into the surrounding style is how coverage is
  silently lost.

## The honest limitation

**Presence is mechanical; importance is a review judgement.**

The collection hook proves that a test in this directory names *a* bug. It cannot
prove that a given bug warranted a regression test in the first place — README's
wording is "every *important* discovered bug", and no linter can evaluate
"important". Nothing here detects a bug fixed in some other pull request with no
test added at all; this directory can only police what arrives in it.

That residual half is a **review** rule, which is why
`.github/pull_request_template.md` carries the matching checkbox:

> **Regression test** — a test was added under `tests/regression/` naming the bug
> it pins, **or** N/A because: …

The two halves are deliberately different in kind. The conftest is the mechanical
half and runs on every collection; the checkbox is the review half and runs on
every pull request. Neither is claimed to do the other's job.

## Running them

```sh
uv run --frozen pytest tests/regression      # this directory
uv run --frozen pytest -m regression         # the marked tests, wherever they live
```

`make check` runs `tests/unit` and `tests/policy` explicitly and does not name
this directory, so an empty regression tree cannot make the gate exit with
pytest's "no tests collected" status. A bare `uv run --frozen pytest` collects
`tests/` as a whole and is likewise unaffected.

Running `pytest tests/regression` **on its own** while the directory is empty does
report `no tests ran` and exits 5. That is pytest reporting the truth, and it is
not papered over: an exit-code override in this conftest would have to mask the
same status in the runs where it means something real.
