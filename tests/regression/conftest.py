"""Collection-time enforcement of the QUAL-07 bug-provenance rule.

Every test module in this directory must name the bug it pins, on a line of the
form ``# BUG: <reference> - <description>``. A module without one fails
collection.

Failing collection, rather than warning, is the point. A warning printed in a
directory nobody reads is not a policy; it is a note. The hook below turns the
convention into something a pull request cannot merge past.

The check is scoped by pytest's own conftest semantics: these hooks run only for
nodes collected under ``tests/regression/``, so nothing elsewhere in the test
tree is affected.

See ``tests/regression/README.md`` for the policy and its honest limits.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

# A top-level comment line: `# BUG:` followed by at least one non-space
# character. Leading whitespace is tolerated so the line may sit inside a
# module docstring block or beside an import.
PROVENANCE_PATTERN = re.compile(r"^[ \t]*#\s*BUG:\s*\S", re.MULTILINE)

_HOW_TO_FIX = """\
{path} has no bug-provenance line.

Every module in tests/regression/ must name the bug it pins. Add a line of the
form:

    # BUG: <issue reference or short slug> - <what used to go wrong>

for example:

    # BUG: #128 - ragged rows were silently truncated instead of rejected

The reference should let a reader find the bug (an issue number, a commit, or a
slug used consistently in the test names). The description should state the
WRONG behaviour the test now prevents, not the right one.

See tests/regression/README.md for the full policy.\
"""


class MissingBugProvenance(pytest.Module):
    """A regression module that does not name the bug it pins.

    Substituted for the ordinary module collector so that the file's tests are
    never collected: a regression test with no provenance is not a test that
    passes, it is a test that must not land.
    """

    def collect(self) -> None:
        """Fail collection, naming the file and the line it is missing."""
        raise pytest.Collector.CollectError(_HOW_TO_FIX.format(path=self.path))


def _has_provenance(module_path: Path) -> bool:
    """Return whether the module declares the bug it pins.

    The file is read as text rather than imported: the rule must hold for a
    module that does not import cleanly, and reading is cheaper than importing.
    """
    try:
        source = module_path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        # An unreadable module is a real problem, but it is not this hook's
        # problem to diagnose. Let the ordinary collector produce the error.
        return True
    return PROVENANCE_PATTERN.search(source) is not None


def pytest_pycollect_makemodule(
    module_path: Path, parent: pytest.Collector
) -> pytest.Module | None:
    """Substitute a failing collector for any regression module lacking provenance.

    Returning ``None`` defers to pytest's ordinary module collection, which is
    what every compliant file gets.
    """
    if not module_path.name.startswith("test_"):
        return None
    if _has_provenance(module_path):
        return None
    return MissingBugProvenance.from_parent(parent, path=module_path)
