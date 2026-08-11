---
phase: 01-repository-toolchain-ci-skeleton
reviewed: 2026-08-11T00:00:00Z
depth: standard
files_reviewed: 26
files_reviewed_list:
  - Makefile
  - pyproject.toml
  - setup.cfg
  - .gitignore
  - .gitleaks.toml
  - .pre-commit-config.yaml
  - .dockerignore
  - .github/workflows/ci.yml
  - .github/dependabot.yml
  - tools/corpus/__main__.py
  - tools/corpus/generators.py
  - tools/corpus/manifest.py
  - tools/corpus/digests.py
  - tools/security/install_gitleaks.sh
  - tools/security/gitleaks_selftest.py
  - tests/conftest.py
  - tests/regression/conftest.py
  - tests/policy/test_gates_actually_fail.py
  - tests/policy/test_ci_calls_make_ci.py
  - tests/policy/test_ci_invokes_make_only.py
  - tests/policy/test_secret_scan_depth.py
  - tests/policy/test_workflow_secrets.py
  - tests/policy/test_print_ban_scope.py
  - tests/policy/test_pinned_tool_versions_agree.py
  - tests/policy/test_generator_determinism_rules.py
  - tests/policy/test_no_postgres_csv_parsing.py
findings:
  critical: 3
  warning: 10
  info: 11
  total: 24
status: issues_found
---

# Phase 01: Code Review Report

**Reviewed:** 2026-08-11
**Depth:** standard
**Files Reviewed:** 26 (of 59 in scope; the badsamples pairs, unit-test modules, package `__init__` files and `corpus.yaml` were read but produced no findings)
**Status:** issues_found

## Summary

The phase is unusually well-defended for a repository skeleton: gates carry paired
negative/positive controls, meta-tests mutate scratch copies rather than the real
files, and most controls state their own limits in prose. The defects below are
therefore not "missing checks" — they are **checks that do not check what their
comment claims**, which is the failure mode this phase is most exposed to.

Three findings are Critical. Two of them are gates that pass on a broken
implementation (`lock-check` after `uv sync`; the installer-ordering assertion
matching a comment), and one is a supply-chain control whose trust anchor is
co-located with the artifact it is supposed to authenticate.

Explicitly *not* reported, per the review brief: the deliberate violations in
`tests/policy/badsamples/`, the malformed CSV declarations in `corpus.yaml`, the
`SYNTH_`-prefixed synthetic credentials, and `enforce_admins: false`. All
`good_*.py` counterparts were checked and none accidentally violates the rule its
partner is meant to trip.

## Critical Issues

### CR-01: `make install` rewrites `uv.lock`, so `lock-check` can never fail in CI

**File:** `Makefile:39-43`, `Makefile:86`; `.github/workflows/ci.yml:43,48,87`
**Issue:** `install` runs a bare `uv sync`. `uv sync` **updates the lockfile** when
it is out of date with the `pyproject.toml` files. `check` then runs `lock-check`
(`uv lock --check`) as its second prerequisite — *after* `install` has already
brought the lock up to date. Make evaluates prerequisites left to right, and CI
runs `make install` then `make check` in that order, so by the time the staleness
gate looks at `uv.lock` there is nothing stale left to find.

The consequence is not cosmetic. A pull request that edits any `pyproject.toml`
without regenerating the lock will be resolved *fresh on the runner*, install
dependency versions nobody reviewed, pass a green `lock-check`, and leave no
trace: the modified `uv.lock` lives only in the runner's workspace. The
`test_pinned_tool_versions_agree` module's own docstring asserts "CI installs from
`uv.lock`" — that claim is currently false whenever the lock is stale.

**Fix:**
```makefile
install: uv-guard              ## Create the venv from the lockfile, exactly
	$(UV) sync --locked
```
`--locked` fails rather than updating. Use `--frozen` if you want the lock used
verbatim without any consistency check. Keep `lock-check` in `check` regardless —
with `--locked` in place it becomes a genuine second signal instead of a no-op.

### CR-02: The installer-ordering gate matches a comment, not the code

**File:** `tests/policy/test_secret_scan_depth.py:157-165`
**Issue:** The test proves the checksum is verified before extraction by comparing
first-occurrence offsets:

```python
verify  = text.find("sha256sum -c")
extract = text.find("tar -xzf")
assert verify < extract
```

The first occurrence of the literal `sha256sum -c` in `install_gitleaks.sh` is not
the verification — it is the **comment on line 63**:

> ``# here rather than silently verifying nothing: `sha256sum -c` over an empty``

That comment sits above `tar -xzf` (line 81) unconditionally. Move the real
verification block (lines 74-79) to *after* the extraction and this assertion
still passes, because line 63 never moved. The test's own docstring calls the
reordering "a plausible, innocent-looking edit" — and it is precisely the edit
this test cannot detect.

This is the sole automated guard on the phase's supply-chain ordering claim
(T-01-09); the fail-closed behaviour itself is documented as having no committed
coverage.

**Fix:** Strip comments before searching, and require the extraction to follow the
*last* verification, not the first mention.
```python
code = "\n".join(
    line for line in text.splitlines() if not line.lstrip().startswith("#")
)
verify  = code.rfind("sha256sum -c")
extract = code.find("tar -xzf")
assert verify != -1 and extract != -1
assert verify < extract, "the archive is extracted before its checksum is verified"
```
Better still, add the behavioural case the docstring admits is missing: point the
script at a fixture whose tarball bytes do not match the checksums file and assert
exit 1 with `tools/bin/gitleaks` absent afterwards.

### CR-03: The gitleaks checksum is fetched from the same origin as the binary, and an already-installed binary is executed before any verification

**File:** `tools/security/install_gitleaks.sh:26-35`, `:49-79`
**Issue:** Two related gaps in the phase's only network-fetch-and-execute path.

1. **The digest is not an independent trust anchor.** Lines 58-59 download the
   tarball *and* `gitleaks_<v>_checksums.txt` from the same
   `github.com/gitleaks/gitleaks/releases/download/v${VERSION}` prefix. Any
   adversary positioned to alter the tarball — a compromised release, a hijacked
   CDN/DNS path, a corporate TLS-intercepting proxy that the runner trusts — alters
   the checksums file in the same operation, and `sha256sum -c` passes. The header
   comment claims this defeats *tampering* ("A tampered binary would report clean
   forever"); as written it defeats only *corruption*. There is no signature check
   (no cosign/minisign verification of the checksums file), so the verification
   adds no integrity beyond what TLS already provides.

2. **The idempotency short-circuit executes an unverified binary.** Line 32 runs
   `"${dest}" version` on whatever is already at `tools/bin/gitleaks` and, if the
   output contains the pinned version string, exits 0 without downloading or
   verifying anything. `tools/bin/` is gitignored and never re-verified, so a
   binary planted there once — including one installed by an earlier run that
   predates a pin bump-and-revert — is executed on every subsequent `make gitleaks`
   and `make ci` forever. The check that decides whether verification is needed is
   itself the execution of the untrusted artifact.

**Fix:** Pin the expected digest in the repository so the trust anchor is
reviewed in a diff, and verify the installed binary rather than interrogating it:

```bash
# Committed, reviewed, per-platform. Bumping the version means bumping these,
# in the same commit, visible in code review.
declare -A GITLEAKS_SHA256=(
  ["8.30.1_linux_x64"]="<digest from the signed release, recorded at pin time>"
  ["8.30.1_darwin_arm64"]="..."
)
expected="${GITLEAKS_SHA256[${GITLEAKS_VERSION}_${os}_${arch}]:?unpinned platform}"

# Idempotency without trusting the binary: hash it, do not run it.
if [ -f "${dest}" ] && [ "$(sha256sum < "${dest}" | cut -d' ' -f1)" = "${installed_expected}" ]; then
  exit 0
fi
...
printf '%s  %s\n' "${expected}" "${tarball}" > expected.sha256
sha256sum -c expected.sha256 || { echo "ERROR: digest mismatch" >&2; exit 1; }
```
Keep the downloaded `checksums.txt` cross-check as a secondary signal if you like,
but the committed digest must be the one that decides. Add
`test_pinned_tool_versions_agree` coverage so the committed digest and
`GITLEAKS_VERSION` cannot drift apart.

## Warnings

### WR-01: `_decimal_renderer` renders negative values incorrectly

**File:** `tools/corpus/generators.py:512-515`
**Issue:** The fractional part is derived with Python's floor-semantics `//` and
`%`, which are correct only for non-negative `units`:

```python
units = low + min(int(rng.random() * span), span - 1)
return f"{units // power}{separator}{units % power:0{scale}d}"
```

With `scale=2`: `units=-1234` renders `-13.66` (should be `-12.34`); `units=-34`
renders `-1.66` (should be `-0.34`). The integral branch (line 508) is fine.
Currently latent — every `decimal` column in `corpus.yaml` declares
`min: "100.00"` — but this is a corpus whose stated purpose is numeric edge cases,
it already ships `57_negative_parentheses_and_trailing_minus.csv`, and a negative
`min:` is a one-line manifest edit away. The failure would be silent: the digest
oracle would happily bless the wrong bytes.

**Fix:**
```python
def _render(rng: random.Random, row_index: int) -> str:
    del row_index
    units = low + min(int(rng.random() * span), span - 1)
    sign = "-" if units < 0 else ""
    magnitude = abs(units)
    return f"{sign}{magnitude // power}{separator}{magnitude % power:0{scale}d}"
```
Add a manifest fixture with a negative `min:` so the case is covered rather than
merely fixed.

### WR-02: `scale` is never validated, and a negative scale silently reaches `float`

**File:** `tools/corpus/manifest.py:1013-1018`
**Issue:** `DecimalColumn.scale` is read with a bare `_require_int` — no bound is
imposed. A manifest declaring `scale: -1` produces `power = 10**-1`, which is a
**Python float (0.1)**, directly violating determinism rule R10 ("No value ever
passes through `float`"), and then crashes with an opaque `ValueError` from the
format spec `f"{...:0-1d}"` deep inside generation. Every other numeric field in
this module is range-checked (`length < 1`, `parts < 2`, `after_record < 1`,
`bom_after_record < 0`); this one is not.

**Fix:**
```python
scale = _require_int(data, "scale", where)
if not 0 <= scale <= _MAX_DECIMAL_SCALE:      # e.g. 18
    msg = f"{where}: scale must be between 0 and {_MAX_DECIMAL_SCALE}, got {scale}"
    raise ManifestError(msg)
```

### WR-03: `tests/regression/` is not run by any Makefile target

**File:** `Makefile:57-61`, `Makefile:86-87`
**Issue:** `test` runs `pytest tests/unit`; `policy` runs `pytest tests/policy`.
Nothing runs `pytest tests/regression`. The entire QUAL-07 mechanism — the
`pytest_pycollect_makemodule` provenance hook in `tests/regression/conftest.py`
and every future regression test — sits outside `make check` and outside `make ci`.
The first regression test written for a real bug will be committed, appear to be
protected, and never execute. The conftest's own claim that a missing `# BUG:`
line makes a pull request unmergeable is false today: collection never happens.

The gap is invisible because `pyproject.toml` sets `testpaths = ["tests"]`, so a
bare `pytest` *does* collect the directory — only the Makefile targets do not.

**Fix:**
```makefile
test:                          ## unit + regression tests, with a coverage report
	$(RUN) pytest tests/unit tests/regression -q --cov --cov-report=term-missing
```
Then add a policy assertion that the union of directories named across the
Makefile's pytest invocations covers every `tests/*/` directory holding an
`__init__.py`, so a future `tests/contract/` cannot be orphaned the same way.

### WR-04: `.gsd/` is untracked and not ignored

**File:** `.gitignore` (absent entry)
**Issue:** `git status --porcelain` reports `?? .gsd/`, and the directory holds
`dispatch-isolation-sentinel.json` — GSD runtime state, not source. It is one
`git add -A` away from entering history, at which point it is scanned by the
full-history gitleaks job on every subsequent run and published when the
repository goes public. Runtime sentinels are exactly the class of file that
accretes machine paths and process identifiers over time.

**Fix:** Add to `.gitignore` alongside the existing tooling block, and to
`.dockerignore`:
```gitignore
# GSD runtime state (ephemeral, machine-local)
.gsd/
```

### WR-05: `make gitleaks`'s working-tree scan has no path scope

**File:** `Makefile:76`
**Issue:** `./tools/bin/gitleaks dir --redact --no-banner --exit-code 1 .` scans the
repository root with no exclusions. That directory contains `.venv/` (thousands of
third-party files, many with test fixtures full of credential-shaped strings),
`tools/bin/gitleaks` (the scanner binary itself), the tool caches, and — after
`make fixtures` — the ~293 MB generated corpus. `.gitleaks.toml` declares no
`[allowlist] paths` for any of them.

Two consequences. A third-party package's test data trips the gate for a reason
that has nothing to do with this repository, and the cheapest way to green that
build is to widen `.gitleaks.toml` — which is precisely the "six months later a
real key ships past a control everyone believes is on" failure the config's own
header warns about. And a developer running `make gitleaks` after `make fixtures`
scans hundreds of megabytes of synthetic high-entropy data, creating standing
pressure to skip the target locally.

**Fix:** Scope the working-tree scan to tracked files, or exclude the
non-repository trees explicitly:
```toml
[[allowlists]]
description = "Not repository content: virtualenv, downloaded tooling, caches."
paths = [
  '''^\.venv/''',
  '''^tools/bin/''',
  '''^\.(mypy|pytest|ruff)_cache/''',
]
```
Note that the fixture-tree allowlists must stay `condition = "AND"`; these are
path-only by nature and should be justified in the description as such.

### WR-06: The history-scan regex also matches `make gitleaks-selftest`

**File:** `tests/policy/test_secret_scan_depth.py:60`
**Issue:** `re.search(rf"\bmake\s+{HISTORY_SCAN_TARGET}\b", run)` with
`HISTORY_SCAN_TARGET = "gitleaks"` matches `make gitleaks-selftest`, because `-`
is a non-word character and therefore satisfies the trailing `\b`. Two failure
modes follow:

* `test_a_scanning_job_exists_at_all` — the vacuity guard — is satisfied by a
  workflow that runs **only** the self-test and never the real full-history scan.
  SEC-02 would then be entirely unenforced with every test green.
* Conversely, a future job that runs only the self-test is required to carry
  `fetch-depth: 0` and is reported as a shallow-checkout violation if it does not,
  which reads as a false positive and invites someone to loosen the pattern.

**Fix:**
```python
if re.search(rf"\bmake\s+{HISTORY_SCAN_TARGET}(?![\w-])", run):
```
and add a sensitivity case asserting that a workflow whose only make step is
`make gitleaks-selftest` is reported by `test_a_scanning_job_exists_at_all`.

### WR-07: `actions/checkout` persists the job token into `.git/config`

**File:** `.github/workflows/ci.yml:37`, `:70`
**Issue:** Neither checkout sets `persist-credentials: false`, which is the
action's default-on behaviour: the `GITHUB_TOKEN` is written into
`.git/config` as an `http.extraheader`. Both jobs then execute repository-supplied
code with that token readable on disk — `make install` runs `uv sync`, which
executes build backends from the lockfile, and `make check` runs the pull
request's own test suite (`tests/policy/test_gates_actually_fail.py` even shells
out to `make`). The workflow-level `permissions: contents: read` and the read-only
fork token bound the damage, but the workflow's own comment claims "this workflow
holds no credential", and `test_the_workflow_token_stays_read_only` exists
specifically because that claim needed qualifying. Handing the token to executed
code is unnecessary here: neither job pushes anything.

**Fix:**
```yaml
      - uses: actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1
        with:
          persist-credentials: false
```
(and on the secrets job alongside `fetch-depth: 0`). Extend
`permission_problems` in `test_workflow_secrets.py` to assert it, so the
structural SEC-10 claim covers the token that is always present.

### WR-08: `_write_records` re-encodes per record, so an endian-less codec would emit one BOM per line

**File:** `tools/corpus/generators.py:288-298`
**Issue:** `_write_records` calls `_encode_text(record + terminator, fixture)` once
per record, and `_encode_text` uses `str.encode(encoding)` — a *stateless* call.
For `utf-16` or `utf-32` (no explicit endianness), every such call emits a fresh
byte-order mark, so a `literal_unicode` fixture declared with `encoding: utf-16`
silently produces a file with a BOM before every row. `_write_tabular` does not
have this bug because it threads a single `codecs.IncrementalEncoder` through the
whole file (line 209).

Nothing in `manifest.py` rejects the endian-less codecs: `codecs.lookup("utf-16")`
succeeds. The `bom: true` path happens to fail (`_bom_for` raises because `_BOMS`
holds only the explicit-endian names), but `bom: false` + `utf-16` is accepted and
generates wrong bytes with a perfectly stable digest.

**Fix:** Use one incremental encoder for the whole record sequence, mirroring
`_write_tabular`:
```python
encoder = codecs.getincrementalencoder(fixture.encoding)("strict")
...
chunks.append(_encode_incremental(encoder, record + fixture.terminator_for(index), fixture))
...
chunks.append(encoder.encode("", final=True))
```
and/or reject endian-less codecs at load time, since a fixture whose byte order is
not declared cannot be a specification of anything.

### WR-09: `--fast` aborts instead of skipping when a wrapper's target was skipped

**File:** `tools/corpus/generators.py:181-187`, `:391-403`; `tools/corpus/manifest.py:456-463`
**Issue:** `generate_corpus(fast=True)` skips `profile: large` fixtures, but a
`wrapper` or `multipart` whose target was skipped is still attempted and raises
`GeneratorError: wraps '…', which has not been generated`. `_check_multipart_target`
forbids a `large` target for **multipart** only — `_validate_wrapper` has no
equivalent check, so `generator: wrapper` over `29_large_file.csv` is a valid
manifest that makes `make fixtures FAST=1` and `make fixtures-verify FAST=1` fail
outright. Compressing the large fixture is an obvious next move (it is the natural
`61_gzipped` companion), and the failure would surface as an unexplained crash in
the fast development loop.

**Fix:** Either extend the load-time rejection to wrappers —
```python
def _validate_wrapper(fixture, where):
    ...
    # plus, in _check_wrapper_target, once by_name is available:
    if by_name[fixture.wraps].profile == "large":
        raise ManifestError(f"{where}: a wrapper over a large-profile target breaks --fast")
```
— or make `generate_corpus` skip transitively: track skipped names and `continue`
for any fixture whose `wraps` is in that set, so `--fast` degrades rather than
breaks.

### WR-10: Coverage is measured over the two packages that contain almost no code

**File:** `pyproject.toml:119-122`; `Makefile:58`
**Issue:** `[tool.coverage.run] source = ["dataplat", "csv_processor"]`. Those two
packages ship a version resolver and two `__init__` modules. The phase's actual
executable code — `tools/corpus/` (~800 lines of parsing, validation and byte
generation) and `tools/security/` — is **outside the measured source**, and
`make test` only runs `tests/unit` so the policy suite's exercise of those modules
is not counted either. The `--cov --cov-report=term-missing` output will report a
near-perfect number over ~50 lines of trivial code while the risky code is
unmeasured. No threshold is enforced today, so this is a misleading signal rather
than a broken gate — but it is the signal a reader will use when Phase 11 sets
`fail_under`.

**Fix:**
```toml
[tool.coverage.run]
branch = true
source = ["dataplat", "csv_processor", "tools"]
```
and run the suites that actually exercise `tools/` under coverage (see WR-03's fix
for the target).

## Info

### IN-01: The second `.gitleaks.toml` allowlist is entirely subsumed by the first

**File:** `.gitleaks.toml:44-55`
**Issue:** `paths = ['''^tests/fixtures/corpus\.yaml$''']` is a strict subset of the
first allowlist's `^tests/fixtures/.*`, with an identical regex and identical
`condition = "AND"`. It silences nothing the first entry does not already silence.
Dead configuration in a security control file is worse than dead code elsewhere: a
future reader trying to work out why it exists is likely to conclude one of the two
is "not working" and broaden it.
**Fix:** Delete the second block and fold its (genuinely useful) explanation of the
tracked-vs-generated distinction into the first block's description.

### IN-02: The `.gitignore` negations on lines 9-10 are no-ops

**File:** `.gitignore:8-10`
**Issue:** `tests/fixtures/csv/` ignores only that subdirectory;
`tests/fixtures/corpus.yaml` and `tests/fixtures/CORPUS.sha256` were never matched
by it, so `!` re-inclusion does nothing. Harmless, but it implies a protection that
is not present — and `test_corpus_not_committed.py` already asserts the real
property.
**Fix:** Drop the two `!` lines; keep the comment.

### IN-03: `parse_digests` accepts non-hex digests and silently deduplicates names

**File:** `tools/corpus/digests.py:96-109`
**Issue:** The only validation is `len(digest) != 64` — 64 arbitrary characters
pass. Duplicate names overwrite silently (`parsed[name] = digest`), so an oracle
containing the same path twice with different digests is accepted and only the last
is compared. `name.lstrip(" *")` also strips *all* leading spaces and asterisks, so
a path legitimately beginning with one cannot round-trip.
**Fix:** `if not re.fullmatch(r"[0-9a-f]{64}", digest): raise DigestFormatError(...)`,
and raise on a repeated name rather than overwriting.

### IN-04: `permission_problems` reports a *narrowed* permission block as "widens"

**File:** `tests/policy/test_workflow_secrets.py:125-127`
**Issue:** The check is `job["permissions"] != READ_ONLY`, so a job declaring
`permissions: {}` — strictly more restrictive than `contents: read` — is reported
as `widens permissions to {}`. The message points the author in the wrong
direction.
**Fix:** Compare against a permitted set of scopes rather than one exact mapping,
e.g. treat `{}` and any subset of `{"contents": "read"}` as acceptable.

### IN-05: `regexTarget = "line"` silences whole lines, not just the matched value

**File:** `.gitleaks.toml:40`, `:53`
**Issue:** With `regexTarget = "line"`, a line under `tests/fixtures/` that
contains a `SYNTH_`-prefixed token *and* a genuine credential is allowlisted
wholesale. Impact is low because the tree is generated and synthetic by
construction, but `regexTarget = "match"` expresses the intent ("this value is
synthetic") more narrowly than "this line is synthetic".
**Fix:** Evaluate `regexTarget = "match"`; if it changes behaviour, record why the
line form is required.

### IN-06: The `--redact` assertion only inspects the Makefile

**File:** `tests/policy/test_workflow_secrets.py:66`, `:157-165`
**Issue:** `SCANNER_INVOCATION` matches `^\s*\.?/?tools/bin/gitleaks\s+…` in the
Makefile text only. The self-test invokes the scanner from Python
(`tools/security/gitleaks_selftest.py:213-230`); that invocation *does* carry
`--redact`, but nothing asserts it, so SEC-10b is enforced for one of the two call
sites. `install_gitleaks.sh:32` and `:85` also invoke the binary, uncovered.
**Fix:** Extend the scan to `tools/**/*.py` and `tools/**/*.sh`, matching the
binary path in argv lists as well as in shell lines.

### IN-07: The gitleaks "pin" is a substring match over an env-overridable default

**File:** `tools/security/install_gitleaks.sh:24`, `:32`
**Issue:** `grep -qF "${GITLEAKS_VERSION}"` is a substring test, so a pin of `8.3`
would be satisfied by an installed `8.30.1`. And `GITLEAKS_VERSION="${GITLEAKS_VERSION:-8.30.1}"`
means any caller's environment silently redirects the download to a different
release — `test_pinned_tool_versions_agree` reads the *default*, not what actually
ran.
**Fix:** Anchor the match (`grep -qE "^gitleaks v?${GITLEAKS_VERSION}$"` against the
parsed version line) and, if the override is intended to stay, echo the effective
version so the CI log records which binary was installed. Largely subsumed by
CR-03's committed-digest fix.

### IN-08: `tests/` is never type-checked, so the mypy `tests.*` overrides are inert

**File:** `Makefile:13`, `:52`; `pyproject.toml:97-104`
**Issue:** `TYPECHECK_PATHS` is `packages/*/src` plus `tools`. `tests/` is not in
it, so the carefully enumerated `[[tool.mypy.overrides]] module = ["tests.*"]`
block (with its correct warning about `strict = false` being silently ignored)
never applies to anything. Test code — including the policy suite that *is* the
phase's deliverable — is unchecked.
**Fix:** Either add `tests` to `TYPECHECK_PATHS` (the overrides then start earning
their keep) or delete the override block and note that tests are out of scope.

### IN-09: `_reject_extra_keys` permits fields irrelevant to the declared generator

**File:** `tools/corpus/manifest.py:503`, `:74-98`
**Issue:** The allowed-key set is the union across all five generator kinds, so a
`literal` fixture may declare `rows: 500`, `row_spec: {...}` or `header: [...]` and
they are parsed, stored and then never used. That is the same class of failure the
"a manifest that silently ignores a misspelled field generates a corpus that
silently means something else" rationale exists to prevent — the field is not
misspelled, it is simply inapplicable, and the silence is identical.
`_validate_parts_scope` already does exactly this check for one field (`parts`).
**Fix:** Generalise `_validate_parts_scope` into a per-kind applicable-key table
and reject any declared key outside it.

### IN-10: `.dockerignore` omits the caches and the new runtime directory

**File:** `.dockerignore:1-9`
**Issue:** `.mypy_cache/`, `.pytest_cache/`, `.ruff_cache/`, `__pycache__/`,
`.coverage`, `.import_linter_cache/` and `.gsd/` all reach the build context. They
are gitignored, so a clean clone is unaffected, but a local `docker build` (the
stated motivation for writing this file early) ships them and busts layer caches
on every test run.
**Fix:** Mirror the tool-cache block from `.gitignore`.

### IN-11: `wrapper` and `multipart` silently ignore a declared `bom:`

**File:** `tools/corpus/generators.py:301-330`, `:391-425`; `manifest.py:804-806`
**Issue:** `_validate_byte_order_mark` returns immediately when
`bom_after_record == 0`, so `bom: true` on a `wrapper` or `multipart` fixture
passes validation — and both writers ignore the flag entirely. The declaration says
the file carries a mark; the bytes do not. Same failure shape as IN-09.
**Fix:** Reject `bom: true` on generator kinds that do not place a mark, in
`_validate_byte_order_mark` before the `== 0` early return.

---

_Reviewed: 2026-08-11_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
