r"""Filename mask compiler/matcher — CSV-01's token-to-regex engine (D-07/D-08/D-09/D-11).

Mask syntax is strptime-style named tokens (D-07), e.g.
``{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv`` — not raw
regex, not a token+regex-escape-hatch hybrid. Individual facets can be
marked optional within one mask using bracket syntax (D-08), e.g.
``[_{seq:03d}]``. A filename that does not match its dataset's configured
mask at all is rejected with a named diagnostic (D-09) — never processed
with the unmatched facets left null.

Implements 06-RESEARCH.md's Architecture Patterns Pattern 3 two-pass
*re-parse* design (a compiled regex extracts substrings, then ``strptime``
re-parses date-typed ones using the SAME format string the regex character
class was built from — the format-to-regex mapping and the
format-to-strptime mapping are never two independently-maintained tables),
combined with a single left-to-right *scan* over the mask text itself:
``compile_mask`` walks the mask exactly once via one combined regex
(``_SCAN_RE``) that alternates between "a token", "a bracket", and "a run
of literal characters", rather than chaining two independent ``re.sub``
passes over the whole mask. A chained-``re.sub`` design was tried and
rejected during this plan's own implementation: expanding bracket-optional
segments first (e.g. ``[_{seq:03d}]`` -> ``(?:_(?P<seq>\\d{3}))?``) and then
running a second, unscoped token substitution over the *entire* resulting
string re-matches the just-generated ``{3}`` quantifier as if it were an
unexpanded ``{name}`` token, corrupting the pattern (verified live: raises
``re.error: bad character in group name '03'``). A single pass has no such
self-interference. Literal mask text (delimiters like ``_``, the ``.csv``
suffix) is ``re.escape``-d as it is scanned, so a mask's own regex
metacharacters (a literal ``.`` in ``.csv`` is the concrete case in every
example mask this phase uses) can never accidentally widen what the
compiled pattern matches.

``parse`` (PyPI) was evaluated and rejected (06-RESEARCH.md Pattern 3): it
supports strptime tokens but has no concept of D-08's bracket-optional
segments, so adopting it would still require hand-writing the optional-
segment layer this module provides anyway.

The compiled regex is whole-string-anchored (``^...$``) per the Claude's
Discretion recommendation 06-RESEARCH.md's Pattern 3 makes explicitly: a
prefix match would silently accept a truncated or extra-suffix filename,
contradicting D-09's "fail with a named diagnostic" framing. Every
generated character class is fixed-width and bounded (``\\d{4}``, ``\\d{2}``,
a bounded ``[^_./]+`` free-form segment) — never ``.*`` or a nested/
backtracking quantifier, so an attacker-controlled filename cannot trigger
catastrophic regex backtracking regardless of length (T-06-03).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING

from dataplat.errors import FilenameParsingError

if TYPE_CHECKING:
    from dataplat.config.model import FilenameMaskConfig

# The one pattern compile_mask scans a mask with, in a single left-to-right
# pass (finditer). Alternatives, tried in order at each position:
#   1. a full "{name}" / "{name:spec}" token (groups 1/2 = name/spec)
#   2. a literal "[" (opens an optional segment)
#   3. a literal "]" (closes an optional segment)
#   4. a run of one-or-more characters that are none of "{", "}", "[", "]"
#      (literal mask text, re.escape-d before use -- see module docstring)
# Because alternation always tries earlier branches first and a token's own
# "{...}" syntax can never appear inside branch 4's exclusion set, a token
# is always recognized as a whole unit and never mistaken for literal text
# or vice versa.
_SCAN_RE = re.compile(r"\{(\w+)(?::([^}]+))?\}|\[|\]|[^{}\[\]]+")

# strptime directive -> fixed-width digit character class. Every entry is a
# bounded quantifier (never ``.*``/an unbounded repeat) so a mask's compiled
# regex can never backtrack catastrophically on attacker-influenced input
# (T-06-03) -- see this module's docstring and the plan's threat model.
# Extend this table, never invent a second one, when a new mask needs a
# strptime directive not yet listed here.
_STRPTIME_CHAR_CLASSES: dict[str, str] = {
    "%Y": r"\d{4}",
    "%m": r"\d{2}",
    "%d": r"\d{2}",
    "%H": r"\d{2}",
    "%M": r"\d{2}",
    "%S": r"\d{2}",
}

# Width, in source characters, of every supported strptime directive (all
# two characters: "%" + one letter). Used to walk a multi-directive format
# spec like "%Y%m%d" two characters at a time when building its regex.
_DIRECTIVE_WIDTH = 2


@dataclass(frozen=True, slots=True)
class CompiledMask:
    """A filename mask compiled to a single anchored regex plus its facet re-parse rules.

    Attributes:
        pattern: The whole-string-anchored (``^...$``) compiled regex. Every
            named group corresponds to one facet declared in the source
            mask; an optional bracket-wrapped segment's groups may be
            ``None`` in a successful match.
        formats: Facet name -> strptime format string, populated only for
            facets whose token declared a ``%``-prefixed format spec (e.g.
            ``business_date`` in ``{business_date:%Y%m%d}``). Consulted by
            ``match_filename`` to re-parse the captured substring into a
            ``datetime.date`` using the exact same format string the
            regex's character class was built from.
        int_facets: Names of facets whose token declared a zero-padded-
            integer spec (e.g. ``seq`` in ``{seq:03d}``). Consulted by
            ``match_filename`` to re-parse the captured substring into
            ``int``. Disjoint from ``formats`` — a facet is in at most one
            of the two.
    """

    pattern: re.Pattern[str]
    formats: dict[str, str]
    int_facets: frozenset[str] = field(default_factory=frozenset)


def _expand_strptime_format(fmt: str) -> str:
    r"""Expand a strptime format spec into its fixed-width regex character classes.

    Args:
        fmt: A strptime format string, e.g. ``"%Y%m%d"``. Walked two
            characters at a time since every supported directive is exactly
            two characters (``%`` + one letter).

    Returns:
        The concatenated regex fragment, e.g. ``r"\\d{4}\\d{2}\\d{2}"``.

    Raises:
        ValueError: ``fmt`` contains a directive not present in
            ``_STRPTIME_CHAR_CLASSES`` — a malformed or unsupported mask
            token must raise at ``compile_mask`` time, not silently produce
            a regex that matches nothing.
    """
    pieces: list[str] = []
    for i in range(0, len(fmt), _DIRECTIVE_WIDTH):
        directive = fmt[i : i + _DIRECTIVE_WIDTH]
        char_class = _STRPTIME_CHAR_CLASSES.get(directive)
        if char_class is None:
            msg = (
                f"unsupported strptime directive {directive!r} in format {fmt!r}; "
                f"supported directives are {sorted(_STRPTIME_CHAR_CLASSES)}"
            )
            raise ValueError(msg)
        pieces.append(char_class)
    return "".join(pieces)


def _zero_padded_int_width(spec: str) -> str | None:
    """Return a zero-padded-integer facet's declared digit width, or ``None`` if not one.

    Args:
        spec: The format spec text after a token's colon, e.g. ``"03d"``.

    Returns:
        The declared digit width as text (e.g. ``"03"``), or ``None`` when
        ``spec`` does not match the ``<digits>d`` zero-padded-integer shape.
    """
    if not spec.endswith("d"):
        return None
    width_text = spec[:-1]
    if not width_text.isdigit():
        return None
    return width_text


@dataclass
class _CompileState:
    """Mutable accumulator threaded through one ``compile_mask`` call.

    Not part of this module's public surface. ``body_parts`` accumulates
    the top-level regex fragments in scan order; ``optional_buffer`` is
    ``None`` while scanning outside any bracket segment, or the in-progress
    fragment list for the bracket segment currently open. ``formats``/
    ``int_facets`` record each facet's re-parse rule, mirroring
    ``CompiledMask``'s own fields.
    """

    body_parts: list[str]
    optional_buffer: list[str] | None
    formats: dict[str, str]
    int_facets: set[str]


def _expand_field(match: re.Match[str], state: _CompileState) -> str:
    """Expand one ``{name}``/``{name:spec}`` token match into a named regex group.

    Args:
        match: A regex match against ``_SCAN_RE`` whose first alternative
            (the token branch) matched — group 1 is the facet name, group 2
            (optional) is the format spec.
        state: Mutated in place: a ``%``-prefixed spec records
            ``name -> spec`` in ``state.formats``; a zero-padded-integer
            spec adds ``name`` to ``state.int_facets`` — so
            ``match_filename`` can later re-parse the captured substring
            with the exact rule this facet was compiled under.

    Returns:
        The regex fragment for this one facet, wrapped in a named capture
        group ``(?P<name>...)``.

    Raises:
        ValueError: The format spec is neither a recognized strptime format
            nor a zero-padded-integer width — a malformed mask must raise
            at compile time (D-07's requirement, applied to unsupported
            tokens).
    """
    name, spec = match.group(1), match.group(2)
    if spec is None:
        # Free-form segment: bounded, no ".", "/" or "_" (the mask's own
        # literal separators), so it can never swallow an adjacent literal
        # or path separator. Never ".*" -- see T-06-03 in this module's
        # docstring.
        return f"(?P<{name}>[^_./]+)"
    if spec.startswith("%"):
        state.formats[name] = spec
        pattern = _expand_strptime_format(spec)
        return f"(?P<{name}>{pattern})"
    width = _zero_padded_int_width(spec)
    if width is not None:
        state.int_facets.add(name)
        return f"(?P<{name}>\\d{{{width}}})"
    msg = (
        f"mask token {{{name}:{spec}}} has an unrecognized format spec {spec!r}; "
        "expected a strptime format (e.g. '%Y%m%d') or a zero-padded-integer "
        "width (e.g. '03d')"
    )
    raise ValueError(msg)


def compile_mask(mask: str) -> CompiledMask:
    """Compile a strptime-style filename mask into a single anchored regex.

    A single left-to-right scan (``_SCAN_RE.finditer``, see module
    docstring for why a single pass replaced an earlier chained-``re.sub``
    attempt) walks ``mask`` once: a token becomes a named regex group
    (recording, alongside it, whether ``match_filename`` must later
    re-parse the captured substring via ``strptime`` or ``int()``); ``[``
    opens an accumulator for an optional segment; ``]`` closes it, wrapping
    everything accumulated since the matching ``[`` in a non-capturing
    ``(?:...)?`` group (D-08); any other run of characters is literal mask
    text, escaped via ``re.escape`` so a mask's own regex metacharacters
    (e.g. the ``.`` in a literal ``.csv`` suffix) can never widen what the
    compiled pattern matches beyond that literal character.

    The final pattern is whole-string-anchored (``^...$``): a filename
    matching only a prefix of the mask does not match at all (D-09;
    06-RESEARCH.md Pattern 3's explicit recommendation on the
    whole-string-vs-prefix discretion point).

    Args:
        mask: The strptime-style mask pattern, e.g.
            ``"{dataset}_{country}_{business_date:%Y%m%d}[_{seq:03d}].csv"``.

    Returns:
        The compiled mask, ready for repeated use with ``match_filename``.

    Raises:
        ValueError: ``mask`` is malformed — an unclosed ``[``, an unmatched
            ``]``, a nested ``[`` inside an already-open optional segment,
            an unrecognized token format spec, or an otherwise invalid
            regex once expanded. Raised at compile time, never deferred to
            a silently-wrong regex that matches nothing (per this task's
            ``<behavior>`` contract).
    """
    state = _CompileState(body_parts=[], optional_buffer=None, formats={}, int_facets=set())

    for match in _SCAN_RE.finditer(mask):
        text = match.group(0)
        target = state.body_parts if state.optional_buffer is None else state.optional_buffer
        if text == "[":
            if state.optional_buffer is not None:
                msg = f"mask {mask!r} has a nested '[' — nested optional segments are not supported"
                raise ValueError(msg)
            state.optional_buffer = []
        elif text == "]":
            if state.optional_buffer is None:
                msg = f"mask {mask!r} has an unmatched ']' with no preceding '['"
                raise ValueError(msg)
            state.body_parts.append(f"(?:{''.join(state.optional_buffer)})?")
            state.optional_buffer = None
        elif match.group(1) is not None:
            # The token branch of _SCAN_RE matched -- group 1 is always
            # present for a token match and always None for "[", "]", or a
            # literal-text run (unmatched alternation groups are None).
            target.append(_expand_field(match, state))
        else:
            target.append(re.escape(text))

    if state.optional_buffer is not None:
        msg = f"mask {mask!r} has an unclosed '[' with no matching ']'"
        raise ValueError(msg)

    body = "".join(state.body_parts)
    try:
        pattern = re.compile(f"^{body}$")
    except re.error as exc:
        msg = f"mask {mask!r} compiled to an invalid regex: {exc}"
        raise ValueError(msg) from exc

    return CompiledMask(
        pattern=pattern,
        formats=state.formats,
        int_facets=frozenset(state.int_facets),
    )


def match_filename(compiled: CompiledMask, filename: str) -> dict[str, str | int | date] | None:
    """Match ``filename`` against a compiled mask, returning every declared facet.

    Runs the compiled regex, then re-parses each captured group: a facet
    listed in ``compiled.formats`` is re-parsed via
    ``datetime.strptime(captured, fmt).date()``; a facet listed in
    ``compiled.int_facets`` becomes ``int(captured)``; every other facet
    stays ``str``. An optional bracket segment (D-08) that did not
    participate in the match has its inner groups captured as ``None`` by
    the regex engine — such facets are OMITTED from the returned dict
    entirely, never present with a ``None`` value, matching D-09's "not
    processed with the unmatched facets left null" framing applied to
    genuinely-optional facets.

    Args:
        compiled: A mask compiled by ``compile_mask``.
        filename: The filename to match, with no path prefix stripping
            performed here — pass the bare filename the caller wants
            matched.

    Returns:
        A dict of facet name -> typed value for every facet that
        participated in the match, or ``None`` if ``filename`` does not
        match ``compiled.pattern`` at all (never raises here — the caller,
        e.g. ``parse_filename``, decides what "no match" means).
    """
    match = compiled.pattern.match(filename)
    if match is None:
        return None

    result: dict[str, str | int | date] = {}
    for name, captured in match.groupdict().items():
        if captured is None:
            # Absent optional facet (D-08): omitted entirely, never present
            # with a None value.
            continue
        fmt = compiled.formats.get(name)
        if fmt is not None:
            # A filename-mask facet is never timezone-aware -- it is either
            # a bare calendar date or, per the plan's own instruction, a
            # timestamp-shaped token immediately truncated to .date() (the
            # time-of-day component, if any, is intentionally discarded;
            # only the date is a meaningful lineage/fallback facet, D-11).
            result[name] = datetime.strptime(captured, fmt).date()  # noqa: DTZ007
        elif name in compiled.int_facets:
            result[name] = int(captured)
        else:
            result[name] = captured
    return result


def parse_filename(mask_config: FilenameMaskConfig, filename: str) -> dict[str, object]:
    """Match ``filename`` against ``mask_config``, raising a named diagnostic on no match.

    The real entry point a future ``Source.inspect()`` caller uses;
    ``match_filename`` stays the low-level primitive this function wraps
    with D-09's reject-on-no-match contract. Compiles the mask fresh on
    every call — a caller processing many filenames against the same mask
    may prefer to call ``compile_mask``/``match_filename`` directly and
    cache the ``CompiledMask`` itself.

    D-11's priority-order rule: the returned ``business_date`` facet (when
    ``mask_config.mask`` declares one) is intended as a FALLBACK ONLY,
    consulted by a future business-date-derivation caller strictly when a
    file's data carries no derivable date. It must never be treated by any
    caller as authoritative over a data-derived date. This function itself
    makes no decision about precedence — it only extracts the facet. This
    phase does not build a general business-date-resolution pipeline; no
    such mechanism exists anywhere in this codebase yet. That remains a
    future phase's integration point.

    Args:
        mask_config: The dataset's filename mask configuration.
        filename: The filename to match, with no path prefix stripping
            performed here.

    Returns:
        Every facet ``match_filename`` extracted, as a plain
        ``dict[str, object]`` (widened from ``match_filename``'s more
        precise return type for caller convenience).

    Raises:
        FilenameParsingError: ``filename`` does not match
            ``mask_config.mask`` at all (D-09). ``context`` carries
            ``diagnostic_code="filename-does-not-match-mask"``, the
            offending ``filename``, and the configured ``mask``.
    """
    compiled = compile_mask(mask_config.mask)
    result = match_filename(compiled, filename)
    if result is None:
        # "filename-does-not-match-mask" is declared in
        # dataplat.diagnostics.DIAGNOSTIC_CODES (D-23/D-24) -- this raise
        # site is that code's first consumer this phase; this plan's
        # <interfaces> block specifies the literal directly rather than a
        # runtime catalog lookup, and test_filename.py cross-checks the
        # literal against the catalog so the two can never quietly drift
        # apart.
        msg = f"filename {filename!r} does not match mask {mask_config.mask!r}"
        raise FilenameParsingError(
            msg,
            context={
                "diagnostic_code": "filename-does-not-match-mask",
                "filename": filename,
                "mask": mask_config.mask,
            },
        )
    return dict(result)
