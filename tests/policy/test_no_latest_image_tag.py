"""INFRA-08: the csv-processor image is always tagged by git SHA, never `:latest`.

Mirrors `tests/policy/test_pinned_tool_versions_agree.py`'s Makefile-recipe-body
regex-scanning idiom (the same idiom `test_supply_chain_guards.py`'s `gitleaks`
target check already uses): read the `image-csv-processor` target's recipe
body out of the real `Makefile` and assert on its text, rather than running
`docker build` here. A static, offline check runs inside `make check` (T-03-16)
and fails a PR the moment someone hardcodes a tag or drops the git-SHA
computation -- not only when someone next runs a build by hand.

Comments are stripped before every search below, and that is load-bearing
rather than tidiness -- the exact same reasoning `test_supply_chain_guards.
py::test_the_installer_verifies_before_it_extracts` documents at length: this
target's own explanatory comments are free to say the word "latest" in prose
(e.g. "never :latest") without that prose satisfying or tripping a check that
is supposed to read only executable recipe content.

`MUTABLE_TAG_VALUES` is imported, not redefined, from `test_supply_chain_
guards.py` (which already imports the same cross-module pattern from
`test_ci_calls_make_ci.py`) -- one frozenset, so a value added to the values-
file guard is automatically covered here too.
"""

from __future__ import annotations

import re
from pathlib import Path

from tests.policy.test_supply_chain_guards import MUTABLE_TAG_VALUES

REPO_ROOT = Path(__file__).resolve().parents[2]
MAKEFILE = REPO_ROOT / "Makefile"

TARGET = "image-csv-processor"
GIT_SHA_EXPR = "git rev-parse --short HEAD"
PINNED_TAG_SUBSTRING = "csv-processor:$$(git rev-parse --short HEAD)"


def _strip_comments(body: str) -> str:
    """Blank comment text while preserving line count/offsets.

    Two comment shapes appear in this Makefile: a whole recipe line that is
    only a `#`-prefixed comment, and the `##`-delimited help suffix on the
    target's own declaration line (this repo's `make help` convention, see
    Makefile line 52-53). Both are blanked from their `#`/`##` onward; the
    executable text before either is preserved untouched.
    """
    lines = []
    for line in body.splitlines():
        if line.lstrip().startswith("#"):
            lines.append("")
        elif "##" in line:
            lines.append(line.split("##", 1)[0])
        else:
            lines.append(line)
    return "\n".join(lines)


def _target_recipe_body() -> str:
    text = MAKEFILE.read_text(encoding="utf-8")
    match = re.search(rf"^{TARGET}:.*?(?=^\S|\Z)", text, re.MULTILINE | re.DOTALL)
    assert match, f"Makefile no longer defines a `{TARGET}` target"
    return _strip_comments(match.group(0))


def mutable_tag_problems(body: str) -> list[str]:
    """Report any `MUTABLE_TAG_VALUES` member hardcoded as `csv-processor:<value>`."""
    return [value for value in MUTABLE_TAG_VALUES if value and f"csv-processor:{value}" in body]


def test_the_recipe_computes_the_git_sha_at_least_twice() -> None:
    """Once for the build arg (the image's own OCI labels), once for the tag."""
    body = _target_recipe_body()
    occurrences = body.count(GIT_SHA_EXPR)
    assert occurrences >= 2, (
        f"`{TARGET}` computes the git SHA {occurrences} time(s), expected at least "
        f"2 (build arg + tag):\n{body}"
    )


def test_the_recipe_never_selects_the_literal_latest_tag() -> None:
    body = _target_recipe_body()
    assert ":latest" not in body, (
        f"`{TARGET}` selects the literal `:latest` tag -- INFRA-08 forbids it:\n{body}"
    )


def test_the_recipe_never_hardcodes_a_mutable_tag_value() -> None:
    body = _target_recipe_body()
    assert not mutable_tag_problems(body), (
        f"`{TARGET}` hardcodes a mutable tag value {mutable_tag_problems(body)}:\n{body}"
    )


def test_dropping_a_git_sha_computation_is_reported() -> None:
    """Non-vacuity: a recipe computing the SHA only once must fail the >= 2 check."""
    body = _target_recipe_body()
    first = body.find(GIT_SHA_EXPR)
    second = body.find(GIT_SHA_EXPR, first + 1)
    assert second != -1, "fixture recipe has fewer than 2 occurrences -- test setup is wrong"

    mutated = body[:second] + "0.0.0-scratch" + body[second + len(GIT_SHA_EXPR) :]

    assert mutated.count(GIT_SHA_EXPR) < 2


def test_replacing_the_tag_with_latest_is_reported() -> None:
    """Non-vacuity: selecting `:latest` must be caught by the literal-substring check."""
    body = _target_recipe_body()
    mutated = body.replace(PINNED_TAG_SUBSTRING, "csv-processor:latest")

    assert mutated != body, "the scratch mutation did not apply -- this test proves nothing"
    assert ":latest" in mutated


def test_replacing_the_tag_with_a_mutable_branch_name_is_reported() -> None:
    """Non-vacuity: a value other than `:latest` in MUTABLE_TAG_VALUES must also be caught."""
    body = _target_recipe_body()
    mutated = body.replace(PINNED_TAG_SUBSTRING, "csv-processor:main")

    assert mutated != body, "the scratch mutation did not apply -- this test proves nothing"
    assert mutable_tag_problems(mutated), "replacing the tag with 'main' was not reported"


def test_the_real_recipe_produces_no_problems() -> None:
    """False-positive control: the committed recipe, unmodified, is clean."""
    body = _target_recipe_body()
    assert not mutable_tag_problems(body)
    assert ":latest" not in body
    assert body.count(GIT_SHA_EXPR) >= 2
