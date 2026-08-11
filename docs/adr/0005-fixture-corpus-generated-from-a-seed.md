---
status: accepted
date: 2026-08-11
---

# ADR-0005: The CSV fixture corpus is generated from a seed and never committed; the digest file is the oracle

## Context and Problem Statement

README §73 calls for a comprehensive corpus of CSV fixtures — encodings, BOMs,
dialects, embedded newlines, ragged rows, NUL bytes, NFC/NFD pairs, scientific
notation, DST edges, compressed archives — and requires that it grow as new cases
are discovered. QUAL-08 makes the corpus *the specification* of the CSV engine:
every detector and validator in Phases 6 and 7 is measured against it.

A corpus that is the specification must be **reproducible byte-for-byte**, because
a fixture whose bytes drift silently re-baselines every test that reads it. It must
also be large enough to include a fixture bigger than the process's address space,
because README §39's bounded-memory guarantee is only provable against one.

Committing that corpus creates two concrete problems, from the same cause — a large
body of realistic-looking synthetic data:

1. **It drowns the secret scanner.** gitleaks' generic high-entropy and card/IBAN-
   shaped rules fire on synthetic account numbers, UUIDs and long identifiers. The
   tempting fix is a global allowlist regex, which disables the control README
   §81.11 exists to provide.
2. **It bloats every Docker build context.** Fixtures anywhere under the build
   context are uploaded on every `docker build`, and a change to any one of them
   invalidates the layer cache.

And the large-file fixture cannot be committed at all at a useful size: the
bounded-memory test wants roughly 241 MB.

## Considered Options

* **A — Commit the corpus.** Hand-craft or generate once, check the files into git,
  read them directly in tests.
* **B — Generate at test time, verify nothing.** A generator produces the fixtures
  into a gitignored directory on demand; tests trust whatever it produced.
* **C — Generate from a seed, commit the specification and a digest oracle.**
  `tests/fixtures/corpus.yaml` (the manifest — committed) and
  `tests/fixtures/CORPUS.sha256` (the digests — committed, ~5 KB) are the only
  artifacts in git. `tests/fixtures/csv/**` is gitignored in its entirety.

## Decision Outcome

Chosen option: **C — generated from a seed, with a committed specification and a
committed digest oracle.**

`make fixtures` materialises the corpus and rewrites the digest file;
`make fixtures-verify` regenerates to a temporary directory and compares SHA-256
against the committed oracle, exiting non-zero on any mismatch. The generator lives
in `tools/corpus/` — as a package, not a script, so it stays under mypy strict and
the `print()` ban that governs library code.

Option **A** was rejected on the two problems above, either of which is enough, and
on the size ceiling for the large fixture.

Option **B** is the one that looks equivalent and is not. Without a committed
oracle, a change to the generator silently changes the corpus, and every downstream
detector test quietly re-baselines against the new bytes — which is the exact
failure the corpus exists to prevent, arriving through the back door. The digest
file makes a generator change a **reviewable diff**: `CORPUS.sha256` moves in the
same pull request as `tools/corpus/`, and a reviewer sees which fixtures changed.
That is the entire point of "the corpus is the specification".

### The ten determinism rules

Byte-identity is not a property the generator has by default; it is a property that
ten specific mechanisms would otherwise break. Each rule names the mechanism it
defeats. These live here, in a decision record, rather than in a test docstring,
because they constrain every future addition to the generator.

| # | Rule | Mechanism it defeats |
|---|---|---|
| **R1** | Derive a **per-fixture** RNG: `Random(int.from_bytes(sha256(f"{MASTER_SEED}\|{name}").digest(), "big"))` | A single shared stream makes fixture *N*'s bytes depend on how many values fixtures 1..*N*−1 consumed. Adding, removing or reordering any fixture would silently rewrite every later one. |
| **R2** | Consume randomness **only** through `.random()` and `.getrandbits()`. Derive integers as `lo + int(r.random() * (hi-lo+1))` and choices as index arithmetic on `.random()` | `choice`, `shuffle`, `sample`, `randrange` and `randint` are documented as subject to change across Python versions. Only `random()`'s sequence for a given seed is guaranteed stable. |
| **R3** | Build a `str`, call `.encode(declared_encoding)`, write with `open(path, "wb")`. Never `open(path, "w")`, never rely on the locale | Text-mode `open` uses `locale.getpreferredencoding(False)`, which differs between the WSL development box and a CI runner. |
| **R4** | The line terminator is an explicit manifest field, joined by hand. Never `csv.writer`'s default, never a translated `"\n"` | `csv.writer` defaults to `\r\n`; text-mode writes translate `\n` per platform. A fixture that exists to test CRLF handling must not have its terminators chosen by the platform. |
| **R5** | `gzip.GzipFile(..., mtime=0, filename="")`; `zipfile.ZipInfo(name, date_time=(1980,1,1,0,0,0))` with an explicit `external_attr` | Both container formats embed wall-clock timestamps in their headers, and gzip additionally embeds the source filename. Two runs 1.1 s apart produced different digests until `mtime=0` was set. |
| **R6** | No `datetime.now()`, `uuid4()`, `os.urandom()`, `time.time()` or `os.getpid()` anywhere in `tools/corpus/`. Every timestamp is a literal in the manifest | Obvious once stated and easy to reintroduce through a helpful `generated_at` header comment. Enforceable as a policy test that greps the generator package. |
| **R7** | Iterate the manifest in **declared order**. Never iterate a `set`, never `sorted()` a heterogeneous key, never rely on `dict` ordering derived from a set | `str.__hash__` is `PYTHONHASHSEED`-salted, so set iteration order varies per process. |
| **R8** | Never read `os.listdir()` or `glob` ordering as an input to generation | Filesystem ordering differs between ext4, overlayfs and tmpfs. |
| **R9** | Build NFC/NFD variants with explicit `unicodedata.normalize("NFC"/"NFD", s)` calls — **never** by pasting two visually identical strings into the generator source | Editors, git filters and some terminals silently renormalise source files, collapsing the very distinction the fixture exists to test. |
| **R10** | Format numbers with explicit format strings or `Decimal`; never `str(float)` | `repr(float)` is stable in CPython ≥3.1, but the values produced by float arithmetic inside the generator are not worth the risk in a corpus whose purpose is numeric fidelity. |

A prototype implementing R1–R10 across ten fixtures produced identical SHA-256
digests on three consecutive runs, including one under
`TZ=America/New_York LC_ALL=C.UTF-8 PYTHONHASHSEED=99`.

**Residual risk, stated rather than hidden.** R2 rests on a documented CPython
guarantee, not on a cross-version experiment — only Python 3.12.3 was available to
test against. The insurance is a policy test asserting that `tools/corpus/`
contains no reference to `random.choice|shuffle|sample|randrange|randint`, so the
rule cannot erode between versions.

### What this buys that a committed corpus cannot

* **A size-parameterised large fixture.** The bounded-memory assertion needs a file
  around 241 MB. Generated, its size is a manifest field; committed, it is 241 MB
  of git history forever. Better still, the assertion needs no file larger than the
  machine's RAM at all: `resource.setrlimit(RLIMIT_AS, 128 MiB)` in a subprocess
  makes the streaming reader pass and a buffering reader die with `MemoryError` on
  that file — which turns README §39 from an E2E-only claim into a unit test.
* **A secret scanner that is not drowned.** With no fixture files in git, gitleaks'
  full-history scan has nothing synthetic to trip over, and the `.gitleaks.toml`
  allowlist stays narrow: path-scoped to `tests/fixtures/**` and keyed to a
  documented `SYNTH_` prefix, never a global pattern. An allowlist that narrow is
  auditable; a global one is a disabled control.

### Consequences

* Good, because a generator change is a reviewable diff (`CORPUS.sha256`) instead
  of a silent re-baseline.
* Good, because `.dockerignore` and the build context stay small, and no fixture
  change can invalidate an image layer cache.
* Good, because the manifest's `expect:` fields make the corpus *machine-readable
  specification* — each becomes an assertion in Phase 6 — rather than a pile of
  bytes with a naming convention.
* Bad, because a fixture cannot be inspected by browsing the repository; you must
  run `make fixtures` first. This is the real ergonomic cost and it is paid on
  every fresh clone.
* Bad, because the generator is now load-bearing code with its own correctness
  requirements — hence mypy strict, the `print()` ban, and a unit test over the
  manifest model so that a corrupt manifest fails loudly instead of generating a
  corrupt corpus.
* Neutral, because README §73's "grown as cases are found" requirement is served
  either way; here growth is a manifest entry plus a digest line.

## Migration trigger

**If corpus generation time ever exceeds the pull-request gate budget.**

The measured headroom today is large. Generation runs at ~67 MB/s, so the 241 MB
fixture costs ~3.6 s to build; SHA-256 runs at ~2 379 MB/s, so verification of the
whole corpus is well under a second. The `make check` target budget is < 90 s and
the pull-request gate budget is ~4 minutes — beyond which, per the research, a gate
gets routed around rather than fixed.

So the trigger is concrete: **if `make fixtures-verify` alone approaches ~30 s**,
roughly an order of magnitude above today's cost and a third of the `make check`
budget, the corpus has outgrown regenerate-every-run. The response at that point is
to move `fixtures-verify` from `make check` to the merge gate — **not** to start
committing fixtures, and **not** to add a cache. A cache would reintroduce a
staleness hole in the one artifact whose entire purpose is reproducibility, to save
four seconds.

## References

* README §73 — CSV Test Corpus; README §39 — bounded memory
* README §67 — Deterministic Processing; README §81.11 — Secret Scanning
* `.planning/research/PITFALLS.md` G3 — the corpus fights both the secret scanner
  and the Docker build context
* `.planning/phases/01-repository-toolchain-ci-skeleton/01-RESEARCH.md`
  § Fixture Corpus → "The ten determinism rules", the measured throughput figures,
  and Pattern 3 "Generated corpus, committed oracle"
* `docs.python.org/3/library/random.html` — the `random()`-only stability guarantee
  behind R2
* `tests/fixtures/corpus.yaml` — the manifest; the machine-readable specification
* `tests/fixtures/CORPUS.sha256` — the oracle
* `tools/corpus/` — the generator (plan 01-03)
