## What this changes

<!-- One or two sentences. What is true after this merges that was not true before? -->

## Why

<!-- Link the requirement ID, phase plan, ADR or issue this serves. -->

## Checklist

- [ ] **Regression test** — a test was added under `tests/regression/` naming the
      bug it pins, **or** N/A because: <!-- state the reason; "no bug fixed here" is a valid reason -->
- [ ] Public functions, classes and methods changed here document their purpose,
      parameters, returns, assumptions, exceptions and side effects.
      *(ruff proves a docstring exists and that a declared `Args:` section is
      complete; assumptions, exceptions and side effects are not mechanically
      checkable and are what this box is for.)*
- [ ] No credential, token or connection string appears in the diff, in a
      fixture, or in a workflow file.
- [ ] `make check` passes locally.

## Notes for the reviewer

<!-- Anything that is not obvious from the diff: a deviation, a trade-off, a follow-up. -->
