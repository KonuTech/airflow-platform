---
phase: 03
slug: dataplat-core-library-metadata-control-plane
status: secured
threats_open: 0
asvs_level: not_configured
created: 2026-08-13
updated: 2026-08-13
---

# SECURITY.md — Phase 3: `dataplat` Core Library & Metadata Control Plane

Security audit of the implemented Phase 3 (`03-dataplat-core-library-metadata-control-plane`,
plans 03-01 through 03-08) against the threat models declared in each plan's `<threat_model>`
block. Every threat below was verified against the implemented code, not against documentation
or the plans'/summaries' own narrative — each row cites the file/line that proves (or fails to
prove) the mitigation exists, and several were additionally re-executed live in this session
(CLI dispatch, empty-CSV parsing, DSN-credential exception content, redaction chain, GRANT
statements, single-configure-call-site) rather than trusted from `SUMMARY.md`/`03-REVIEW.md`/
`03-VERIFICATION.md` text alone.

**Audit date:** 2026-08-13
**ASVS level:** not configured (Phase 2's audit used Level 1 as project-informal baseline; no
level was set in this phase's `<config>` block)
**Block-on policy:** `high` (as passed by the orchestrator; the agent's own classification rule
treats any `OPEN_THREATS` finding as a BLOCKER regardless of a finer severity gradient — applied
here)
**Original result (this audit pass):** 18/19 threats CLOSED. **1 OPEN (BLOCKER): T-03-10.** 3
unregistered attack-surface flags (WARNING, non-blocking under `block_on: high`, but real and
independently reproduced).

**Post-fix update (2026-08-13, same day):** `/gsd:code-review 3 --fix` ran concurrently with this
audit and fixed all 8 `03-REVIEW.md` findings, including CR-03 — the identical TOCTOU race this
audit found as T-03-10, fixed in the same commit (`20d101c`) as the unregistered
`metadata-postgres-get-or-create-dataset-race` flag. Both `config/registry.py._resolve_dataset_id`
and `metadata/postgres.py.get_or_create_dataset` now use a single atomic
`INSERT ... ON CONFLICT (dataset_name) DO UPDATE ... RETURNING dataset_id`, closing the race
structurally rather than by locking a row that doesn't exist yet. The fixer independently
reproduced the exact two-thread `threading.Barrier` race this audit's evidence is built on, against
a real throwaway PostgreSQL 18 container: pre-fix, the loser thread raised
`psycopg.errors.UniqueViolation`; post-fix, both threads returned the same `dataset_id` with zero
exceptions (full detail: `03-REVIEW-FIX.md` CR-03 entry). The orchestrator independently confirmed
the atomic-upsert pattern is present in both files by direct reading, and re-ran `make check` after
the fix commits landed — clean pass (uv-guard, lock-check, lint, format, typecheck, imports,
`lint-imports` contract kept, 111 policy tests, unit+regression tests, 70/70 fixture corpus).
CR-01 and CR-02 (the two other unregistered WARNING flags — the CLI usage-error crash and the
empty-CSV `RuntimeError`) were fixed in the same pass (commits `c8f165a`, `46c20db`).

**Updated result: 19/19 threats CLOSED. 0 OPEN. All 3 unregistered flags also resolved.**

Offline evidence gathered live during this audit: `resolve_secret()`'s fail-closed branches,
`_redact`'s processor-chain position (confirmed the *only* `structlog.configure(` call site in
the repository is `dataplat/observability/logging.py:82`), the `hash_config()` canonicalization
recipe, all 7 migration `GRANT` statements (grepped repo-wide — no `GRANT ALL`/schema-wide grant
exists anywhere), the Dockerfile's `USER 1000` directive, `tests/policy/test_no_latest_image_tag.py`
against the real `Makefile` recipe, and `FIELD_SIZE_LIMIT`/`_strip_nul` wiring in
`csv_processor/source.py`. Additionally, three findings were reproduced by direct Python execution
in this session (not merely cited from `03-REVIEW.md`/`03-VERIFICATION.md`): a bare `dataplat.cli.main([])`
call raising uncaught `click.exceptions.NoArgsIsHelpError`; `chunked_records()` raising uncaught
`RuntimeError: generator raised StopIteration` on a zero-byte stream; and a malformed/credentialed
DSN's real connection-failure exception message (confirmed the literal password string does **not**
appear in `psycopg_pool.PoolTimeout`'s message — narrowing, not confirming, the WR-02/T-03-05
caveat below).

---

## Threat Verification

| Threat ID | Category | Component | Disposition | Evidence | Status |
|-----------|----------|-----------|-------------|----------|--------|
| T-03-01 | Information Disclosure | `dataplat.secrets.resolver.resolve_secret` | mitigate | `secrets/resolver.py:42-56` — `env://` branch returns a resolved value or raises (line 44-48); `file://` branch returns resolved text or raises, chained `from exc` (line 49-54); every other scheme (incl. `vault://`) falls through to an unconditional raise (line 55-56). No code path returns `ref` unresolved. | CLOSED |
| T-03-02 | Information Disclosure | `dataplat.observability.logging` processor chain | mitigate | `observability/logging.py:79-100` — processors list places `cast(..., _redact)` (line 95) immediately before `renderer` (line 96); `_redact` (line 34-61) replaces, not partially masks, any key matching `_SECRET_KEY_PATTERN`. Repo-wide grep confirms `observability/logging.py:82` is the **only** `structlog.configure(` call site in the tree — no second, unguarded logger path exists. | CLOSED |
| T-03-03 | Tampering | `config/hashing.py` canonicalization | mitigate | `config/hashing.py:47-53` — `json.dumps(config.model_dump(mode="json"), sort_keys=True, separators=(",", ":"), ensure_ascii=False)` then `hashlib.sha256(...).hexdigest()`. `config` is typed `DatasetConfig` (the validated model); no code path hashes raw YAML text. | CLOSED |
| T-03-04 | Elevation of Privilege | every migration's `GRANT` statement | mitigate | All 7 `GRANT` statements repo-wide (grepped): `migrations/versions/0001_*.py:58,94`, `0002_*.py:76`, `0003_*.py:47,66`, `0004_*.py:112`, `0005_*.py:97` — every one reads `GRANT SELECT, INSERT, UPDATE ON <one table> TO etl_app`. Grepped for `GRANT ALL`/`ON ALL TABLES`/`ON SCHEMA` across all 5 files: zero matches. | CLOSED |
| T-03-05 | Information Disclosure | `dataplat.storage.db.create_pool` | mitigate | `storage/db.py:47-51` — on `psycopg.OperationalError`, `context={"dsn_scheme": urlsplit(dsn).scheme}` only, never the DSN or credential. **Caveat** (already disclosed in `03-01-SUMMARY.md`, corroborated by `03-REVIEW.md` WR-02, and independently re-confirmed live by me this session): `ConnectionPool(dsn, open=False)` never validates conninfo at construction, so this `except` branch is dead code today — a malformed DSN raises no exception at all from `create_pool()`. I additionally tested the realistic downstream failure (a credentialed DSN's *actual* connection-open failure, later, from a different call site) live: the raised `psycopg_pool.PoolTimeout`'s message does **not** contain the password (`libpq`'s own connection-failure messages report host/port only) — so while the *wrapping* is absent downstream (a robustness gap, see Unregistered Flags), the specific credential-disclosure this threat targets is not actually realized either at `create_pool()` or at the later, unwrapped failure point. | CLOSED (with documented caveat) |
| T-03-06 | Information Disclosure | `DataPlatformError.context` | accept | `errors.py:30-39` — the base `__init__` stores `context` verbatim, no redaction applied (confirms the accept rationale's first half: "base class imposes no redaction itself"). Verified the second half is also true, not merely asserted: `cli.py:85-90`'s `log.error("dataplat command failed", ..., **exc.context)` spreads `context` into the same `event_dict` every other log call uses, which **does** pass through `_redact` (T-03-02) before rendering — so a secret-pattern key placed in `context` genuinely is redacted at the logging boundary, not just claimed to be. Logged in Accepted Risks below. | CLOSED |
| T-03-07 | Tampering | `migrations/env.py` connecting to the wrong database | mitigate | `migrations/env.py:121-130` — `SELECT current_database()` compared to `EXPECTED_DATABASE` and `raise RuntimeError` on mismatch, executed *before* line 144's `CREATE SCHEMA IF NOT EXISTS meta` (the first DDL) and before `context.configure()`/`context.run_migrations()` (lines 147-157). | CLOSED |
| T-03-08 | Tampering | SQL injection via dynamic identifiers in `op.execute(...)` | accept | Read all `op.execute(...)` calls across all 5 migration files in full: every one is a static Python string literal (`"CREATE SCHEMA IF NOT EXISTS meta"`, `"GRANT SELECT, INSERT, UPDATE ON meta.datasets TO etl_app"`, etc.) — no f-string, `.format()`, or `%`-interpolation of a value into any of them. Logged in Accepted Risks below. | CLOSED |
| T-03-09 | Tampering | `resolve_secret()` given a `file://` path outside the intended secrets mount | accept | `secrets/resolver.py:49-54` — `Path(parsed.path).read_text(...)` accepts any filesystem path with no allow-list; matches the plan's own honest disposition that path restriction is out of this phase's scope. Logged in Accepted Risks below. | CLOSED |
| T-03-10 | Repudiation | `ConfigRegistry.sync()` concurrent writers | mitigate | Originally found OPEN — see **Fixed (post-audit)** section below. Now closed: `config/registry.py:_resolve_dataset_id` (commit `20d101c`) replaces the plain `SELECT ... FOR UPDATE` + `INSERT` with a single atomic `INSERT ... ON CONFLICT (dataset_name) DO UPDATE ... RETURNING dataset_id`, independently re-verified by direct code reading and by the fixer's live two-thread reproduction against real Postgres 18 (zero exceptions, same `dataset_id`, post-fix). | CLOSED (post-fix) |
| T-03-11 | Tampering | `PostgresMetadataRepository`'s dynamic `SET` clause in `update_ingestion_run_status` | mitigate | `metadata/postgres.py:223-226` — `unknown_fields = sorted(set(fields) - _INGESTION_RUN_UPDATABLE_FIELDS); if unknown_fields: raise ValueError(...)` runs *before* any query text is assembled (lines 228-240); every value crosses via `params`/`%s`. Repo-wide grep confirms this is the **only** `UPDATE`/`SET` path onto `meta.ingestion_runs` — no bypass exists elsewhere. | CLOSED |
| T-03-12 | Information Disclosure | `S3ObjectStore` construction | accept | `storage/objectstore.py:96-112` — `access_key`/`secret_key` are plain caller-supplied constructor params, not routed through `resolve_secret()`. Verified `tests/integration/test_objectstore.py:30-32` sources them from the `minio_config` fixture (testcontainers-generated), not a literal. Logged in Accepted Risks below. | CLOSED |
| T-03-13 | Denial of Service | `RaggedRowGuard.apply()` given a pathologically large chunk | accept | `pipeline/engine.py:64-79` — single `for i, row in enumerate(chunk.rows)` loop, O(n) in chunk size, no recursion, no additional unbounded structure. `chunk_size` bound is a `Source`-level configuration concern (`csv_processor/source.py`'s `chunk_size` param), consistent with the accept rationale. Logged in Accepted Risks below. | CLOSED |
| T-03-14 | Repudiation | `run_streaming`'s per-chunk metrics/tracing calls | accept | `observability/metrics.py:12-19` — `increment()` body is docstring-only (no side effect). `observability/tracing.py:15-24` — `start_span()` unconditionally `return contextlib.nullcontext()`. Both genuinely no-op; no observable audit trail exists to falsify. Logged in Accepted Risks below. | CLOSED |
| T-03-15 | Elevation of Privilege | container runtime user | mitigate | `docker/csv-processor/Dockerfile:70,86` — `useradd -r -g app -u 1000 -m app` then `USER 1000` as the final, only `USER` directive in the `runtime` stage. Live-verified per `03-VERIFICATION.md`: `docker run --rm --entrypoint id csv-processor:8e32511` → `uid=1000(app) gid=1000(app)`. | CLOSED |
| T-03-16 | Tampering | mutable `:latest` tag reintroduced later | mitigate | `tests/policy/test_no_latest_image_tag.py` — 7 tests including 3 non-vacuity mutation tests (`test_dropping_a_git_sha_computation_is_reported`, `test_replacing_the_tag_with_latest_is_reported`, `test_replacing_the_tag_with_a_mutable_branch_name_is_reported`). Verified against the real `Makefile:182-195` recipe: `git rev-parse --short HEAD` appears exactly twice, no `:latest` substring. File carries no `pytest.mark`, so it is collected by `make check`'s `policy` target (`pytest tests/policy -q -m "not manifests"`). | CLOSED |
| T-03-SC | Tampering | base images (`python:3.12-slim-bookworm`, `ghcr.io/astral-sh/uv:0.12.3`) | accept | `Dockerfile:48-49,68` — both images pinned by explicit tag, never `:latest`, both official publisher images. Grepped `.github/workflows/ci.yml`: no `trivy` step exists yet anywhere in this repo's CI — confirms the accept rationale ("trivy scanning is Phase 11's CI gate, not this phase's") is accurate, not a case of an existing control being silently skipped for this image. Logged in Accepted Risks below. | CLOSED |
| T-03-17 | Denial of Service | an unbounded field inside a malformed/malicious CSV | mitigate | `csv_processor/source.py:41,98-100` — `FIELD_SIZE_LIMIT = 1_048_576` (explicit int, never `sys.maxsize` — grepped, zero matches); `csv.field_size_limit(FIELD_SIZE_LIMIT)` (line 98) executes before `csv.reader(...)` is even constructed (line 99), and therefore before the first `next()` call (line 100). | CLOSED |
| T-03-18 | Tampering | NUL-byte injection to smuggle content past downstream string-processing assumptions | mitigate | `csv_processor/source.py:99` — `csv.reader(_strip_nul(text_stream), dialect=DIALECT)` constructs the reader over the **filtered** generator (`_strip_nul`, lines 44-69, `line.replace("\x00", "")`), never over `text_stream` directly — every line the reader ever sees has already been NUL-stripped. | CLOSED |

**Totals:** 19 threat rows, **19 CLOSED** (18 at original audit time, T-03-10 closed same-day post-fix), **0 OPEN**.

---

## Fixed (post-audit)

### T-03-10 — Repudiation — `ConfigRegistry.sync()` concurrent writers — **mitigation claim did not hold for a dataset's first-ever sync — now fixed, commit `20d101c`**

**File:** `packages/dataplat/src/dataplat/config/registry.py:181-207` (`ConfigRegistry._resolve_dataset_id`)

**Claimed mitigation:** "Single-writer discipline (`FOR UPDATE` / serialized read-then-write) per
dataset prevents two versions both landing with `valid_to IS NULL`."

**What the code actually does:**

```python
row = cur.execute(
    "SELECT dataset_id FROM meta.datasets WHERE dataset_name = %s FOR UPDATE",
    (dataset_name,),
).fetchone()
if row is not None:
    return int(row[0])
inserted = cur.execute(
    "INSERT INTO meta.datasets (dataset_name) VALUES (%s) RETURNING dataset_id",
    (dataset_name,),
).fetchone()
```

`SELECT ... FOR UPDATE` only takes a lock on rows that already satisfy the `WHERE` clause. When
`dataset_name` has never been synced before, **zero rows match, so `FOR UPDATE` locks nothing** —
there is no pre-existing row to serialize against. Two concurrent `sync()` calls for the same
**new** dataset name therefore both observe "no row," both proceed to
`INSERT INTO meta.datasets (dataset_name) VALUES (%s)`, and — because `meta.datasets.dataset_name`
carries a `UNIQUE` constraint (`migrations/versions/0001_meta_datasets_config_versions.py`,
`sa.Column("dataset_name", sa.Text(), nullable=False, unique=True)`) — the losing transaction's
`INSERT` raises a raw, unwrapped `psycopg.errors.UniqueViolation`. That exception is not a
`StorageError`/`DataPlatformError`, so it is not caught by `cli.py`'s catch-once boundary either
(same class of gap as the CLI/CSV findings below): the run crashes with a raw traceback instead of
resolving gracefully.

This is exactly the scenario `sync()`'s own docstring claims protection against — "without it, two
concurrent syncs could both observe 'no current row'... and each insert" — but the protection
described only activates once a `meta.datasets` row already exists. The **very first** concurrent
sync of a brand-new dataset (a realistic scenario under this project's own chosen
`KubernetesExecutor` local-executor model, where a backfill fanning out multiple files of a new
dataset runs as concurrent per-task pods — CLAUDE.md §B) is precisely the case left unguarded.

**Corroboration (three independent sources, not just my own reading):**
1. My own direct reading of `registry.py:181-207`, above.
2. `03-REVIEW.md` CR-03: reproduced the *identical* structural defect against a real, throwaway
   PostgreSQL 18 container with two threads racing on a never-before-seen `dataset_name` — one
   thread's `INSERT` succeeds, the other raises `psycopg.errors.UniqueViolation:
   duplicate key value violates unique constraint "datasets_dataset_name_key"`. The review states
   explicitly: *"`ConfigRegistry._resolve_dataset_id()` has the identical structural defect for a
   brand-new dataset... The module's own docstring explicitly reasons about serializing
   'concurrent sync() calls for that dataset' (T-03-10) but that reasoning only holds once the
   dataset row already exists."*
3. `03-VERIFICATION.md` independently corroborates: *"`ConfigRegistry.sync()` creates/no-ops/versions
   per ARCHITECTURE.md §5.1 | ✓ VERIFIED (code) ... `FOR UPDATE` serialization present (see CR-03
   caveat below re: first-ever sync)."*

**Why this was found OPEN, not CLOSED-with-caveat, at original audit time:** the mitigation's own stated mechanism (`FOR UPDATE`
serialization "per dataset") is verifiably absent for a specific, realistic, non-hypothetical
triggering condition — not merely a theoretical edge case or a robustness nit. `meta.config_versions`
itself never ends up with two `valid_to IS NULL` rows (Postgres's own constraints prevent that
outcome incidentally), but only because the losing caller's entire operation aborts ungracefully —
which is arguably worse for the Repudiation category this threat is filed under, since the failure
is non-deterministic (depends on race timing) and produces no `RejectedRecord`/structured audit
trail of what happened, contradicting this platform's own stated Core Value ("every... batch...
can be traced, explained, reprocessed and trusted").

**Fix applied same-day, commit `20d101c`** (via `/gsd:code-review 3 --fix`, running concurrently
with this audit): both `config/registry.py._resolve_dataset_id` and
`metadata/postgres.py.get_or_create_dataset` (the identical unregistered defect — see Unregistered
Flags below) now use exactly the atomic-upsert pattern this audit recommended:

```python
row = cur.execute(
    """
    INSERT INTO meta.datasets (dataset_name) VALUES (%s)
    ON CONFLICT (dataset_name) DO UPDATE SET dataset_name = EXCLUDED.dataset_name
    RETURNING dataset_id
    """,
    (dataset_name,),
).fetchone()
```

Independently confirmed present in both files by the orchestrator via direct reading, and by the
fixer's live re-run of this section's exact two-thread `threading.Barrier` reproduction against a
real, throwaway PostgreSQL 18 container: pre-fix the loser thread raised
`psycopg.errors.UniqueViolation` (matching this audit's finding); post-fix, both threads returned
the same `dataset_id` with zero exceptions, and two concurrent first-time `ConfigRegistry.sync()`
calls on a brand-new dataset name correctly serialized into exactly one `is_new=True` insert and
one `is_new=False` no-op. Full detail: `03-REVIEW-FIX.md`'s CR-03 entry. `make check` re-run clean
after the fix commits landed.

---

## Accepted Risks Log

Per this audit, the following risks are formally logged as accepted. Each was declared `accept`
in its originating plan's threat model with a stated argument, verified against the implemented
code to confirm the argument still holds (no code change silently strengthened or weakened the
accepted exposure) — this log is the durable record the audit process requires.

| Threat ID | Risk | Argument for acceptance | Re-evaluation trigger |
|-----------|------|--------------------------|------------------------|
| T-03-06 | `DataPlatformError.context` carries no redaction of its own | Redaction happens structurally at the logging boundary instead (`_redact`, T-03-02) — verified `cli.py` actually spreads `context` into every logged error, so a secret placed there genuinely is caught, not merely claimed to be | Any future call site that logs `context` (or any structured field) through a path that bypasses `dataplat.observability.logging.configure()`'s processor chain |
| T-03-08 | Migration `op.execute(...)` calls build SQL text (schema/GRANT statements) rather than using parameterized queries throughout | Every identifier across all 5 migrations is a static Python string literal — verified by reading every `op.execute()` call in the phase; no user- or config-derived identifier construction exists in this phase's scope | Any future migration that derives a table/schema name from configuration, a CLI argument, or dataset metadata rather than a literal |
| T-03-09 | `resolve_secret("file://...")` accepts any filesystem path with no allow-list | This phase only proves the `env://`/`file://` mechanism; real pod mount-path restriction is a Phase-5/Vault-adjacent concern, consistent with SEC-15's phased design | Phase 5's Vault retrofit, or any point before then where `file://` refs are sourced from anything other than already-trusted, already-authorized configuration |
| T-03-12 | `S3ObjectStore`'s credentials arrive as plain caller-supplied strings, not routed through `resolve_secret()` | The Postgres pool's `resolve_secret()`-to-`create_pool()` wiring (SEC-15) is proven end to end elsewhere in this phase (plan 03-05 Task 3); S3 credentials are the one remaining unwired case, and this plan's own tests use only testcontainers-generated throwaway credentials (verified: `tests/integration/test_objectstore.py:30-32` sources from the `minio_config` fixture) | Any phase that wires `S3ObjectStore` against a real (non-testcontainers) MinIO/S3 endpoint without first routing its credentials through `resolve_secret()` |
| T-03-13 | `RaggedRowGuard.apply()` has no internal bound on chunk size | O(n) in chunk size by construction (verified: single loop, no recursion); the actual bound is a `Source`-level configuration concern outside this stage's responsibility | A future `Source` implementation that accepts an externally-controlled, unbounded `chunk_size` (code review IN-02 already flags the *related* absence of `chunk_size >= 1` validation in `CsvSource`/`CsvRecordStream`, worth folding into any future fix) |
| T-03-14 | `run_streaming`'s metrics/tracing calls are no-ops, so no audit trail exists for pipeline execution yet | Deliberate, documented D-03 scope decision — verified both `metrics.increment()` and `tracing.start_span()` have genuinely empty/no-op bodies, not partially-wired stubs | Phase 7, when a real StatsD/OTel backend is wired — at that point this accepted gap converts into an actual observability requirement, not a threat |
| T-03-SC | Base images (`python:3.12-slim-bookworm`, `ghcr.io/astral-sh/uv:0.12.3`) are not scanned by trivy in this phase | Both are official, pinned-tag publisher images; verified no `trivy` step exists anywhere in `.github/workflows/ci.yml` today, confirming the deferral is real (not a case of skipping an existing control) | Phase 11, when the trivy CI gate is built — this accepted gap should close automatically once that lands and covers `csv-processor` |

---

## Unregistered Flags

`03-01-SUMMARY.md` through `03-08-SUMMARY.md` contain **no** `## Threat Flags` sections (confirmed
by reading all 8 in full) — so there is nothing to reconcile from that source. Per this audit's
own adversarial-stance duty not to treat that absence as proof no new attack surface exists, three
items were independently found by cross-referencing each plan's own `<threat_model>` **Trust
Boundaries** tables (which name boundaries without always registering a threat ID against them)
against `03-REVIEW.md`'s findings and my own live reproduction in this session:

| Flag | File | Description | Classification | Resolution |
|------|------|--------------|-----------------|------------|
| `cli-usage-error-uncaught-exception` | `packages/dataplat/src/dataplat/cli.py:75-92` | `main()` calls `cli.main(args=argv, prog_name="dataplat", standalone_mode=False)` and catches only `DataPlatformError`. Click's own `UsageError`/`ClickException` family (no arguments, an unknown option, an unknown subcommand) is not caught and propagates as a raw Python traceback. **Reproduced live in this session:** `from dataplat.cli import main; main([])` raises uncaught `click.exceptions.NoArgsIsHelpError` (`isinstance(e, DataPlatformError)` is `False`). This is the literal `ENTRYPOINT ["dataplat"]` of the built image — `docker run <image>` with no arguments (the single most basic post-build sanity check) crashes instead of printing usage help, independently reproduced inside the actual built image per `03-VERIFICATION.md`. `03-07-PLAN.md`'s own Trust Boundaries table names "CLI stdin/argv → click command dispatch" as a boundary but registers **no threat ID** against it — this is genuinely new attack surface with no mapping, not a documented-and-accepted gap. | WARNING | **Fixed, commit `c8f165a`** (CR-01) — `main()` now also catches `click.exceptions.ClickException`/`Exit`/`Abort`, converting each to the exit code `standalone_mode=True` would have produced. |
| `csv-empty-file-uncaught-runtimeerror` | `packages/csv-processor/src/csv_processor/source.py:100` | `header = next(reader)` is called unguarded inside a generator function (`chunked_records`). A genuinely empty (zero-byte, header-less) CSV stream makes `next()` raise `StopIteration`, which PEP 479 converts to `RuntimeError: generator raised StopIteration` at the point the generator is first driven. **Reproduced live in this session:** `chunked_records(io.TextIOWrapper(io.BytesIO(b""), ...), chunk_size=10)` raises uncaught `builtins.RuntimeError: generator raised StopIteration`. `03-08-PLAN.md`'s own Trust Boundaries table states the boundary "must never crash the process on malformed input, only ever raise a bounded, documented exception or pass a row through unmodified" — a blanket commitment this finding falsified for this input class. | WARNING | **Fixed, commit `46c20db`** (CR-02) — `header = next(reader)` wrapped in `try/except StopIteration: return`; an empty stream now yields zero chunks. Flagged in `03-REVIEW-FIX.md` as "requires human verification" (product-decision caveat: zero-chunks vs. a typed error — a human should confirm this is the desired behavior). |
| `metadata-postgres-get-or-create-dataset-race` | `packages/dataplat/src/dataplat/metadata/postgres.py:77-93` | `PostgresMetadataRepository.get_or_create_dataset` has the **identical** unguarded `SELECT`-then-`INSERT` race as `ConfigRegistry._resolve_dataset_id` (T-03-10) — plain `SELECT ... WHERE dataset_name = %s` with no lock, no `ON CONFLICT`, no retry, followed by a plain `INSERT`. Unlike `_resolve_dataset_id`, this method was **never named in any plan's threat register at all** — `03-05-PLAN.md`'s threat register covers only `update_ingestion_run_status` (T-03-11) for `postgres.py`; `get_or_create_dataset` has zero disposition, zero declared mitigation. Empirically reproduced by `03-REVIEW.md` (CR-03) against a real Postgres 18 container with two threads racing on a never-before-seen `dataset_name`: one `INSERT` succeeds, the other raises raw `psycopg.errors.UniqueViolation`. | WARNING | **Fixed in the same commit as T-03-10, `20d101c`** (CR-03) — same atomic-upsert pattern applied to both files. |

No other unregistered flags were found. `WR-01` (`S3ObjectStore.get_object()` only catches
`ClientError`, not the sibling `BotoCoreError` connectivity-failure family) and `WR-04`/`WR-05`
(`RejectedRecord.raw_line` reconstruction fidelity; silent NUL-stripping with no metric) from
`03-REVIEW.md` were considered and are noted here for completeness, but are not elevated to
`unregistered_flag` status: they are robustness/observability gaps on already-covered code paths
(T-03-12's object-store surface; T-03-18's NUL-filtering surface) rather than genuinely new,
unmapped attack surface.

---

## Verification Method Notes

- **Static verification:** every `mitigate` threat was grepped/read against the exact file(s)
  named in its plan's Mitigation Plan column; matches confirmed load-bearing (not comments or
  dead code) by reading surrounding context — and, for T-03-05, by explicitly distinguishing a
  documented dead-code caveat from an actual credential-disclosure failure via live testing.
- **Repo-wide entry-point checks (not single-match acceptance):** T-03-02's redaction claim was
  checked against every `structlog.configure(` call site in the repository (found exactly one);
  T-03-04's grant claim was checked against every `GRANT`/`ON ALL TABLES`/`ON SCHEMA` occurrence
  across all 5 migrations (found only the 7 declared per-table grants); T-03-11's allow-list claim
  was checked against every `UPDATE`/`SET` reference to `meta.ingestion_runs` in the codebase
  (found only the one guarded path).
- **Live re-execution (not narrative trust):** `resolve_secret()`'s branches, the CLI's usage-error
  crash (CR-01), `chunked_records()`'s empty-stream crash (CR-02), and a credentialed DSN's actual
  connection-failure exception content were all independently executed in this session, not cited
  from `SUMMARY.md`/`03-REVIEW.md`/`03-VERIFICATION.md` alone.
- **Accept dispositions:** each was checked for (a) a written argument in the originating plan,
  (b) confirmation via direct code reading that no implementation change silently strengthened or
  weakened the accepted exposure, and (c) for T-03-06 and T-03-SC specifically, an added
  affirmative check that the argument's supporting claim is itself true (context genuinely flows
  through redaction; no trivy step exists yet to be silently skipped).
- **Transfer dispositions:** none declared in this phase's threat register.
- **The TOCTOU direction supplied by the orchestrator** ("factor the TOCTOU finding into your read
  of T-03-10... it's the same code path that threat's 'mitigate' disposition claims is handled")
  was the basis for classifying T-03-10 OPEN rather than accepting the plan's own claim at face
  value — the code, read directly, does not support the claim for a dataset's first-ever sync,
  independent of and corroborating `03-REVIEW.md`'s CR-03 and `03-VERIFICATION.md`'s own caveat.

## Result

**19/19 threats CLOSED. 0 OPEN. Phase 3 clears the security audit.**

T-03-10 was found OPEN at original audit time (18/19 closed) and closed same-day once
`/gsd:code-review 3 --fix` (running concurrently) applied the atomic-upsert fix this audit itself
recommended, in the same commit that also closed the two structurally-identical unregistered flags
(`cli-usage-error-uncaught-exception`, `csv-empty-file-uncaught-runtimeerror`,
`metadata-postgres-get-or-create-dataset-race`). The orchestrator independently confirmed the fix
by direct code reading and a clean post-fix `make check` run before marking this phase secured.

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|---------------|--------|------|--------|
| 2026-08-13 | 19 | 18 | 1 | gsd-security-auditor |
| 2026-08-13 (same day, post-fix) | 19 | 19 | 0 | orchestrator (direct code verification + `make check`), corroborating gsd-code-fixer's live re-reproduction |
