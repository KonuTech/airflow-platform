# Feature Research

**Domain:** Production-like data-ingestion platform (Kubernetes + Airflow) with a metadata-driven universal CSV ingestion engine
**Researched:** 2026-08-11
**Confidence:** MEDIUM-HIGH (taxonomy and dependency analysis derived from the README itself = HIGH; comparative-tool claims verified against vendor docs = MEDIUM)

> **Framing.** The README already specifies the feature set exhaustively (95 sections, 114 DoD items) and the user has committed to all of it. This document does **not** propose a feature list. It (1) organizes the 114 DoD items into a REQ-ID taxonomy, (2) validates them against how Airbyte / dbt / dlt / Debezium / Delta Lake / Singer actually solve these problems, (3) calibrates effort, (4) maps dependencies, and (5) flags anti-features — including ones the README misses.

---

## 1. Proposed Category Taxonomy (REQ-ID prefixes)

Fourteen categories. **Every one of the 114 DoD items lands in exactly one.** Item counts sum to 114 (verified).

| # | Prefix | Category | DoD items | Count | Effort |
|---|--------|----------|-----------|-------|--------|
| 1 | `INFRA` | Cluster, deployed services, IaC, container build | 1–8 | 8 | L |
| 2 | `SEC` | Secrets management, workload identity, security testing | 90–102, 111 | 14 | L |
| 3 | `ORCH` | Airflow orchestration, TaskFlow, K8s execution, dataset deps | 9–14, 58 | 7 | M |
| 4 | `CSV` | Filename/encoding/dialect/header/footer detection + normalization | 15–22, 28, 29 | 10 | XL |
| 5 | `SCHEMA` | Inference, contracts, versioning, compatibility, drift, historical resolution | 23–27, 51 | 6 | L |
| 6 | `VALID` | Structural + quality validation, quarantine, reports, reconciliation, RI | 30–33, 55–57 | 7 | L |
| 7 | `LOAD` | Identity, idempotency, transactional/atomic loading, recovery, streaming | 34–37, 52–54 | 7 | L |
| 8 | `DEDUP` | Deduplication strategies and audit | 38–41 | 4 | M |
| 9 | `INCR` | Watermarks, late/out-of-order data, backfills, rebuild-from-raw | 42, 43, 47–50, 114 | 7 | L |
| 10 | `CDC` | CDC event model, ordering, delivery semantics | 44–46 | 3 | M |
| 11 | `SCD` | SCD 0/1/2, keys, change detection, effective dating, late arrivals, CDC↔SCD | 60–70 | 11 | XL |
| 12 | `OBS` | Freshness, structured logging, metrics/lineage, runbooks | 59, 74–77, 112 | 6 | L |
| 13 | `QUAL` | Python code standards + the full test pyramid + fixture corpus | 71–73, 78–89 | 15 | L |
| 14 | `CICD` | GitHub Actions pipeline, image build, manifest validation, rebuildability | 103–110, 113 | 9 | M |

**Sum:** 8+14+7+10+6+7+7+4+7+3+11+6+15+9 = **114** ✓

### Full DoD → category mapping

| Category | DoD items (verbatim numbers from §94) |
|----------|----------------------------------------|
| `INFRA` | 1 kind cluster · 2 Airflow in K8s · 3 Airflow PG · 4 analytical PG · 5 MinIO · 6 Vault · 7 IaC · 8 containers versioned |
| `SEC` | 90 solution deployed · 91 not in Git · 92 not in Python · 93 not in images · 94 Airflow backend · 95 pod least-cred · 96 least-privilege identity · 97 auditable · 98 rotation documented · 99 CI no long-lived creds · 100 secret scanning · 101 unauthorized access rejected · 102 dev/prod isolation · 111 secrets managed securely |
| `ORCH` | 9 TaskFlow · 10 K8s task pods · 11 Dynamic Task Mapping · 12 retries/backfills · 13 logical dates · 14 thin DAGs · 58 dataset dependencies in Airflow |
| `CSV` | 15 filename masks · 16 encodings · 17 encoding detection · 18 dialects · 19 delimiter detection · 20 quote/escape · 21 header detection · 22 metadata/footer · 28 invalid dates detected · 29 numeric/boolean/NULL normalization |
| `SCHEMA` | 23 inference · 24 explicit contracts · 25 versioning · 26 compatibility validation · 27 drift detection · 51 historical schemas for backfills |
| `VALID` | 30 structural · 31 data quality · 32 quarantine · 33 machine-readable reports · 55 reconciliation · 56 control totals · 57 referential integrity |
| `LOAD` | 34 idempotent ETL · 35 retries no dupes · 36 reprocessing safe · 37 file vs record identity · 52 transactional/atomic load · 53 partial-failure recovery · 54 large files without full RAM load |
| `DEDUP` | 38 dupes within file · 39 dupes across batches · 40 dataset-specific strategies · 41 auditable |
| `INCR` | 42 incremental processing · 43 watermarks persisted safely · 47 late-arriving data · 48 out-of-order data · 49 backfills correct · 50 backfills idempotent · 114 rebuild analytics from raw |
| `CDC` | 44 CDC architecturally supported · 45 CDC ordering · 46 delivery semantics documented |
| `SCD` | 60 Type 0 · 61 Type 1 · 62 Type 2 · 63 business vs surrogate keys · 64 deterministic change detection · 65 effective dating · 66 late-arriving changes · 67 CDC feeds SCD · 68 replayed CDC ≠ duplicate versions · 69 SCD idempotent · 70 SCD backfills |
| `OBS` | 59 data freshness · 74 proper logging · 75 no `print()` · 76 contextual logs · 77 no secrets in logs · 112 runbooks |
| `QUAL` | 71 type hints · 72 docstrings · 73 error handling · 78 unit · 79 integration · 80 E2E · 81 regression · 82 fixture corpus · 83 idempotency tested · 84 dedup tested · 85 backfills tested · 86 schema evolution tested · 87 CDC tested · 88 SCD tested · 89 failure/recovery tested |
| `CICD` | 103 GH Actions · 104 PR checks · 105 lint · 106 type check · 107 tests · 108 image build · 109 K8s/Helm validation · 110 security scanning · 113 environment recreatable from repo |

**Optional finer split:** `QUAL` (15 items) is the largest bucket and cleanly bisects into code standards (71–73) and testing (78–89). If the requirements author wants tighter IDs, use `PYENG-01..03` + `TEST-01..12`. That yields 15 prefixes and no other change.

**Ambiguity calls made (record these so they aren't re-litigated):**

- **51 (historical schemas for backfills)** → `SCHEMA`, not `INCR`. The mechanism is "resolve schema version by business date"; that is a schema-registry capability that backfill merely consumes.
- **58 (dataset dependencies)** → `ORCH`, not `VALID`. It is an Airflow-asset/sensor concern, explicitly "do not hide in Python" (§48).
- **59 (freshness)** → `OBS`, not `VALID`. Freshness is a monitored property of the platform, not a per-batch validation.
- **77 (no secrets in logs)** → `OBS`, not `SEC`. The control is implemented in the logging layer (redaction filter); `SEC` owns storage/access, `OBS` owns emission. Cross-reference required in both.
- **111 (secrets managed securely)** → `SEC`. It is a verbatim restatement of 90–102 living in the DevOps list; folding it in avoids a duplicate requirement.
- **114 (rebuild from raw)** → `INCR`, not `CICD`. Rebuildability is replay, and replay is the same machinery as backfill. `CICD` keeps 113 (rebuild the *environment*); `INCR` keeps 114 (rebuild the *data*).
- **28 / 29 (dates, numerics, booleans, NULLs)** → `CSV`. They sit under §94's own "CSV Processing" heading and belong to the same module boundary (`csv_processor/normalization/`). If the roadmap wants normalization as its own plan, split `NORM-01..05` out of `CSV`.

---

## 2. Feature Landscape

### Table Stakes (any production ingestion platform has these)

Missing any of these means the platform is not production-like. Validated against Airbyte, dlt, Singer/Meltano, dbt, Debezium.

| Feature | Why Expected | Complexity | Notes |
|---------|--------------|------------|-------|
| RFC 4180-compliant streaming parser (quotes, escapes, embedded newlines) | Universal. Naive splitting corrupts data silently. | MEDIUM | `CSV`. Never `split(",")` (§10). Confirmed: most real CSVs violate RFC 4180. |
| Encoding detection with confidence + per-dataset override | Non-UTF-8 exports (Windows-1250/1252) are the norm in EU sources. | MEDIUM | `CSV`. Detection is probabilistic — must be recorded and pinnable. |
| Delimiter/dialect detection with override | Every ETL tool ships this. | MEDIUM | `CSV`. Must fail loudly when undecidable, not guess. |
| Explicit schema/data contract + conservative inference | dlt, Airbyte, Singer all have a schema layer. Inference alone is unusable in production. | MEDIUM | `SCHEMA`. Contract is the default; inference is bootstrap-only. |
| Schema drift **detection and reporting** (not auto-adaptation) | Reference behaviour in dlt's contract modes. | MEDIUM | `SCHEMA`. See §4 anti-features. |
| Content-hash file identity, not filename | Filenames are re-used, re-dated and re-sent. | LOW | `LOAD`. Cheap; foundational. |
| Batch identity + load ledger with UNIQUE constraint | Delta Lake's `(txnAppId, txnVersion)`; the universal idempotency token. | LOW | `LOAD`. **Highest ROI item in the project.** |
| Idempotent merge/upsert into a keyed target | Airbyte "append + dedup", dlt `write_disposition=merge`. | MEDIUM | `LOAD`. Requires a real PK constraint, not application-side checking. |
| Staging table + atomic publication | Consumers never see half-loaded data. | MEDIUM | `LOAD`. |
| Watermark/bookmark advanced **only after commit** | Singer STATE contract; identical semantics. | LOW | `INCR`. Commit watermark in the same transaction as data. |
| Incremental extraction with a cursor column | Airbyte cursor field / Singer replication key. | MEDIUM | `INCR`. Use `>=` + dedup, never `>`. |
| Structural validation with row/column diagnostics | Errors must be locatable. | MEDIUM | `VALID`. |
| Quarantine of bad records with structured reason codes | Neither GX nor Soda gives this — it is genuinely pipeline work. | MEDIUM | `VALID`. Must be queryable and re-drivable. |
| Persisted machine-readable validation results per run | GX Validation Results / Data Docs retain per-run history for trend analysis. | MEDIUM | `VALID` + `OBS`. Rows in Postgres **and** JSON in MinIO. |
| Configurable quality thresholds → PASS / WARN / FAIL / QUARANTINE | Every DQ framework has severity tiers. | LOW | `VALID`. |
| Deduplication by configurable business key (not `DISTINCT`) | Airbyte primary key, dlt `primary_key`. | MEDIUM | `DEDUP`. |
| Streaming/chunked processing under bounded memory | Files exceed pod memory routinely. | MEDIUM | `LOAD`. |
| Backfill through the same pipeline, driven by logical date | Airflow-native. §34 forbids a bypass path. | MEDIUM | `INCR` + `ORCH`. |
| SCD2 with surrogate keys, validity intervals, `is_current` | dbt snapshots are the reference implementation. | HIGH | `SCD`. |
| Documented delivery semantics, honestly stated | Debezium documents at-least-once explicitly. | LOW | `CDC`. Cheap to write, expensive to get wrong. |
| Structured contextual logging, no `print()`, no secrets | Baseline. | LOW | `OBS`. |
| Secrets from an external manager, never in Git/images | Baseline. | HIGH | `SEC`. |
| CI: lint, type check, unit + integration tests, image build | Baseline. | MEDIUM | `CICD`. |

### Differentiators (above the commodity line)

These are where this platform beats an off-the-shelf Airbyte/dlt deployment. They align with PROJECT.md's Core Value ("every file, batch and record can be traced, explained, reprocessed and trusted").

| Feature | Value Proposition | Complexity | Notes |
|---------|-------------------|------------|-------|
| **Header/metadata/footer detection** (report titles, blank lines, totals rows) | Airbyte/dlt/Fivetran essentially assume row 1 is the header. Real bank/SAP/Excel exports do not. This is the genuinely novel capability. | HIGH | `CSV` §11. |
| **Filename mask parsing into business metadata** (dataset, country, business date, batch, sequence) | Commodity tools ignore filenames entirely; file-drop integrations depend on them. | MEDIUM | `CSV` §8. Note §8's own warning: a date in a filename is *not* automatically the business date. |
| **Per-dataset schema-evolution policy** (compatible vs breaking, per change type) | dlt gets close with `{tables, columns, data_type} × {evolve, freeze, discard_row, discard_value}` — copy this shape, not a boolean. | HIGH | `SCHEMA` §13. |
| **Historical schema resolution for backfills** | Almost nothing does this. Reprocessing a 2024 file under the 2026 schema is a classic silent-corruption source. | HIGH | `SCHEMA` (51). Requires schema registry keyed by validity window. |
| **Full lineage queryable in SQL** ("where did this row come from?") | Commercial tools show run history, not row provenance. | HIGH | `OBS` §83. Requires the control-plane schema (see gaps). |
| **Deduplication auditability** (why was this record removed?) | Airbyte/dlt dedup is opaque — dropped rows vanish. Explaining removals is rare and valuable. | MEDIUM | `DEDUP` §27. |
| **Late-arriving SCD2 corrections** (split and re-close historical intervals) | dbt snapshots explicitly cannot do this — they only append forward. Genuinely differentiating. | HIGH | `SCD` §58. |
| **Source→target reconciliation + control totals** | Standard in banking ETL, absent from modern OSS tooling. | MEDIUM | `VALID` §45–46. |
| **Record-level quarantine with a documented re-drive path** | GX/Soda produce verdicts, not recoverable rejects. | MEDIUM | `VALID`. Re-drive is a **gap** — see §5. |
| **Deterministic replay** (same input + config + version ⇒ same output) | Testable as a property; almost never guaranteed in practice. | HIGH | §67. **Gap** — no DoD item. |
| **Metadata-driven single engine, many datasets** | The architectural thesis (§65, §95). | HIGH | Cross-cutting. One parameterized DAG factory, not N DAGs. |
| Prometheus + Grafana + OpenTelemetry tracing across Airflow→pod→processor→PG | Above what most in-house platforms achieve. | HIGH | `OBS`. PROJECT.md addition; sequence last. |

### Anti-Features

**A. Already correctly warned about in the README** (keep these; they are load-bearing):

| Anti-feature | README |
|---|---|
| `string.split(",")` CSV parsing | §10 |
| Assuming row 1 is the header | §11 |
| Over-eager type inference (`001234` → `1234`) | §12 |
| `DISTINCT` as a deduplication strategy | §26 |
| Filenames as identity | §24 |
| Claiming exactly-once | §30 |
| Auto-adapting to schema drift instead of reporting it | §52 |
| Silently discarding records | §27, §51 |
| Ingestion time as SCD effective date | §57 |
| A simplified bypass pipeline for backfills | §34 |
| ML-based anomaly detection before simple thresholds | §53 |
| Blindly trimming identifiers / auto-treating `N/A` as NULL / auto-treating `1/0` as boolean | §18, §17, §16 |
| Overwriting raw files on failure | §63 |

**B. Anti-features the README MISSES** — these are the additions:

| Anti-feature | Why Requested | Why Problematic | Alternative |
|---|---|---|---|
| **Auto-creating / auto-ALTERing target tables from inferred schema** | Feels like the natural payoff of a metadata-driven engine; dlt does it. | DDL becomes an uncontrolled side effect of arriving data. A single bad file permanently widens the warehouse. Rollback is manual. | Engine **validates** against the target, never migrates. DDL is a versioned, reviewed migration artifact. Schema evolution produces a *proposal + alert*, not an `ALTER`. |
| **A single global `strict_mode: true/false`** | Simple config surface. | Collapses genuinely distinct outcomes. dlt needs a 3×4 matrix (`tables`/`columns`/`data_type` × `evolve`/`freeze`/`discard_row`/`discard_value`) because "drop the bad value but keep the row" ≠ "drop the row" ≠ "fail the load". Retrofitting the matrix later re-plumbs every call site. | Per-entity, per-dataset enum from day one. Copy dlt's shape. |
| **Quarantine as log lines or opaque blobs** | Fastest thing to build. | Quarantine becomes a data graveyard. Nobody can count errors by type, threshold on them, or reprocess. | Structured `quarantine_record` table: `batch_id, file_id, source_row_number, raw_line (bounded), parsed JSONB, error_code (enum), column_name, stage, resolution`. Error codes are a **stable enum**, never free text. |
| **`WHERE cursor > last_watermark`** | The obvious query. | Records sharing the boundary cursor value are silently lost. Airbyte documents this exact failure and the coarse-granularity variant (daily cursor, hourly syncs). | `>=` plus idempotent merge. Accept deliberate re-delivery; make the merge absorb it. Optionally add dlt-style `lag` for attribution windows. |
| **Surrogate key derived as `hash(business_key, valid_from)`** | Deterministic, no sequence needed, reproducible across environments. | A late-arriving correction changes `valid_from` ⇒ the SK changes ⇒ facts already joined to that version dangle. Silent referential corruption. | Stable identity/sequence SK. Keep the change hash as a **separate** column. dbt does exactly this: `dbt_scd_id` is independent of the check-cols hash. |
| **`valid_to = NULL` for current rows, with no policy** | Looks clean. | Breaks `BETWEEN`/range predicates; every consumer must `COALESCE`, and one that forgets returns wrong history. | Pick one convention and enforce it centrally. dbt added `dbt_valid_to_current` (e.g. `9999-12-31`) precisely for this. Prefer a sentinel + half-open intervals `[valid_from, valid_to)`. |
| **"Absent from this batch ⇒ deleted"** as SCD/merge default | Feels like the honest full-snapshot reading. | Catastrophic on incremental extracts — the first delta file retires the entire dimension. dlt makes this configurable via `merge_key` semantics *because* it is a footgun. | Explicit per-dataset `delivery_mode: full_snapshot | incremental | partition_snapshot`. Retirement only in snapshot modes, scoped to the loaded partition. |
| **Change hash computed over raw (un-normalized) values** | Simpler; hash the row as parsed. | `PL` vs `pl `, NFC vs NFD, `1.50` vs `1.5`, `2026-08-11` vs `11/08/2026` all produce phantom SCD2 versions. The dimension grows without any real change. | **Normalization must run before hashing.** Hard ordering constraint: `CSV`(norm) → `SCD`(hash). Also exclude volatile columns (ingestion timestamp, row number, filename) from the hash — including them means *every* load creates a version. |
| **XCom as a data channel** | TaskFlow makes `return df` look natural. | XCom is metadata-sized and lives in the Airflow metadata DB — the one DB §4 forbids using for data. Fills it, then fails at scale. | Pass S3 URIs, `batch_id`s and small manifests. Never rows. |
| **One hand-written DAG per dataset** | Fastest path to "it works for customers". | Directly defeats §65/§95. N copies of retry logic, N places to fix a bug. | One parameterized DAG factory reading the dataset registry. Adding a dataset is a YAML file, not a Python file. |
| **Aggressive `retries=5` on every task from the start** | Makes the pipeline "resilient". | Masks non-idempotent bugs — the pipeline looks green while duplicating data. Also retries non-retryable errors (contract violation, breaking schema change) pointlessly. | Retries enabled **only after** the batch ledger proves idempotency. Classify exceptions: transient (retry) vs terminal (fail fast, no retry). The §71 exception hierarchy should encode this. |
| **Treating detected encoding as ground truth** | Detection returns a value; use it. | `charset-normalizer`/`chardet` confidence is probabilistic; Windows-1250 vs 1252 differ in a handful of code points and misdetection corrupts exactly the Polish characters this project cares about. §9 says "don't pretend it's deterministic" but stops short. | Detection is a **suggestion**. Resolved encoding is persisted per batch in lineage, pinnable per dataset, and a confidence below threshold is a validation finding, not a silent choice. |
| **Full-table SCD2 recompute as the "simple" implementation** | Much easier to write and verify. | Fine at fixture scale, wrong at real scale, and — worse — regenerates surrogate keys, breaking fact joins. Teams rarely migrate off it. | Do the incremental merge properly from the start. Full recompute is acceptable only as a *test oracle* to assert the incremental path against. |
| **A bespoke expectation/rule DSL** | Contracts want expressiveness. | An in-house expression language is a permanent maintenance tax and untestable in isolation. | Fixed, small rule vocabulary in YAML (`not_null`, `unique`, `in_set`, `range`, `regex`, `foreign_key`, `row_count_between`). Add rule types by adding code, not grammar. |
| **A "defer" outcome for referential-integrity failures** (§47) | Listed in the README alongside fail/quarantine/warn. | Implies a deferred-orphan queue with re-evaluation, expiry and ordering — a whole subsystem. | Ship `fail` / `quarantine` / `warn` first. `defer` only if a fixture demands it. |

---

## 3. Deep Dives (specific questions)

### 3.1 File discovery and identity — what "good" looks like

**Identity is content, not path.** Three distinct identities, each with its own table:

- `ingest_object` — one row per observed `(bucket, key, etag, size, last_modified)`. Physical.
- `ingest_file` — one row per distinct `(dataset, content_sha256)`. Logical. UNIQUE on the hash.
- `ingest_batch` — one row per processing unit, UNIQUE on `(dataset, batch_key)`. A batch may span many files (manifest) or one file may be re-batched (deliberate reprocess).

This separation is what lets discovery classify precisely rather than binarily:

| Classification | Detection |
|---|---|
| `NEW` | hash not in `ingest_file` |
| `ALREADY_PROCESSED` | hash present, batch committed |
| `MODIFIED` | same key, different etag/hash |
| `DUPLICATE_CONTENT` | different key, same hash (re-send under a new name) |
| `LATE` | parsed business date < current data interval |
| `MISSING` | expected by the dataset's frequency contract, not observed (§44) |

§40 already mandates "use ingestion metadata rather than object-storage listings" — this is the schema that makes that possible. Note `DUPLICATE_CONTENT` ≠ `ALREADY_PROCESSED`: the same content legitimately re-sent as a *new* batch is a business decision, and conflating them is exactly the §25 error.

**Still-uploading files.** S3/MinIO `PUT` is atomic so partial objects are rarer than on POSIX, but multipart uploads and staged writers still expose them. Two acceptable mechanisms: (a) require a control file (preferred), or (b) ETag/size stability check across a settle interval. §42 mandates this; there is **no DoD item** for it.

**Manifest / control-file / batch-completion patterns.** Three industry patterns, in increasing strength:

1. **Marker file** — `_SUCCESS` / `_BATCH_COMPLETE` (Hadoop lineage, §43). Cheap, says only "done".
2. **JSON manifest** (§41) — enumerates files with checksums, sizes, expected row counts. Strongest.
3. **Per-file sidecar** — `data_001.csv.ctl` carrying record count and control totals. Common in banking feeds; pairs naturally with §46.

The design rule that matters: **when a manifest exists it is authoritative**. Files present on disk but absent from the manifest are ignored; files in the manifest but absent from storage are an error, not a shrug. Crucially, the manifest — not a live listing — should be the input to Dynamic Task Mapping. Mapping over a live `list_objects` makes the run non-deterministic and unreplayable, violating §67; mapping over a frozen manifest makes the same DAG run reproduce exactly.

### 3.2 Quarantine and bad-record handling

Two levels, and they behave differently:

- **File-level** — the raw object is immutable (§63), so quarantine *references or copies*, never moves out of `raw/`. A `quarantine/` pointer plus a `ingest_file.status = QUARANTINED` row.
- **Record-level** — structured rows, schema given in the anti-features table above.

What separates a good implementation from a graveyard is the **re-drive path**: correct the contract or the data, then reprocess the quarantined rows for batch X *through the same pipeline* (the §34 principle applied to quarantine). The README specifies quarantine (§51) and forbids silent discard, but never specifies how data gets out. **Gap.**

The `resolution` lifecycle (`PENDING → REDRIVEN | DISCARDED | ACCEPTED`) is what makes quarantine depth a monitorable metric rather than an ever-growing number.

### 3.3 Validation reporting and DQ observability

Persist **results as rows, not just artifacts**:

- `validation_run` — `batch_id, status, started_at, ended_at, rules_evaluated, rules_failed`
- `validation_result` — `run_id, rule_id, rule_type, column_name, observed_value, threshold, status`

This is the lesson from Great Expectations: Validation Results are retained per Checkpoint run specifically so quality trends over time are queryable, and Data Docs render from that history. The JSON report in `metadata/` (§23) is the replay/audit artifact; the rows are the query surface.

Two consequences worth stating as requirements:

1. **§53 anomaly detection needs this history to exist first.** "Normal NULL rate 0.2%, today 38%" is only computable against a persisted baseline. Anomaly detection therefore depends on validation-result persistence, not the reverse.
2. **§82 metrics should be derived from these same tables**, so dashboards and DAG branching decisions can never disagree.

### 3.4 CSV fixture gaps — additions to §73's 29 fixtures

The existing 29 cover the common axes well. These are the cases production systems actually hit that the list misses. Grouped; `[V]` marks cases corroborated by upstream bug reports rather than recalled.

**Structural / parser-breaking**
- `30_crlf_lf_mixed` — both line endings in one file **[V]**
- `31_cr_only` — bare `\r` (classic Mac / some mainframe exports)
- `32_nul_bytes` — embedded NUL. Python's stdlib `csv` raises `_csv.Error: line contains NULL byte` and **cannot** handle it without pre-filtering the byte stream (cpython #71767 / bpo-27580) **[V]**
- `33_ragged_rows` — rows with both too few and too many fields. Highest-severity case: parsers commonly pad or truncate *silently* (polars #10585) **[V]**
- `34_unclosed_quote_eof` — an open quote consuming the remainder of the file
- `35_quote_in_unquoted_field` — `ab"cd`
- `36_doubled_vs_backslash_escape` — `""` and `\"` conventions
- `37_delimiter_frequency_differs_header_vs_body` — classic dialect-sniffer trap
- `38_single_column_no_delimiter` — sniffer has nothing to detect; must decline, not guess
- `45_trailing_delimiter_every_row` — phantom empty final column
- `46_no_trailing_newline`
- `47_blank_lines_interspersed` — blanks *inside* data, not only before the header
- `63_repeated_header_mid_file` — concatenated exports
- `64_footer_totals_with_different_column_count`
- `65_very_wide` — ~2,000 columns (column limits, DB limits)
- `67_row_exceeding_field_size_limit` — trips `csv.field_size_limit`

**Encoding / Unicode**
- `39_utf8_invalid_sequences` — truncated multibyte; forces an explicit `errors=` policy (strict / replace / surrogateescape)
- `40_utf16_no_bom` — genuinely ambiguous; the confidence-and-override path
- `41_bom_mid_file` — BOM after concatenation
- `42_zero_width_and_bidi` — ZWSP/RLM inside business keys **[V]**
- `43_nbsp_thousands_separator` — U+00A0 as a thousands separator (ubiquitous in PL/FR exports)
- `44_unicode_nfc_vs_nfd` — the same business key in both forms. **Dedup and SCD hashing will not match unless normalization includes Unicode normalization.** High-value fixture.
- `68_utf8_bom_semicolon_pl_excel` — the single most common real Polish Excel export shape

**Type / semantic damage**
- `50_excel_scientific_notation_ids` — a 15-digit ID rendered `1.23457E+14`. **Unrecoverable** — must be detected and rejected, not coerced
- `51_excel_leading_zero_stripped` — postcode `01234` → `1234`
- `52_date_ambiguous_dm_vs_md` — all days ≤ 12, so the format is undecidable; must not guess
- `53_two_digit_year` — `11/08/26`
- `54_excel_serial_dates` — `45880`
- `55_dst_gap_and_overlap` — timestamps inside the Europe/Warsaw spring-forward gap and autumn overlap. §14 mandates DST handling; nothing tests it
- `56_mixed_timezone_offsets` — `+02:00`, `Z` and naive in one column
- `57_negative_parentheses_and_trailing_minus` — `(123.45)` and SAP-style `123.45-`
- `58_currency_and_percent` — `€1.234,56`, `12,5 %`
- `59_numeric_null_sentinels` — `-1`, `9999-12-31`, `0000-00-00`
- `60_boolean_localized` — `Tak/Nie`, `Ja/Nein`, `O/N`
- `70_empty_last_field_vs_null` — distinguishing `''` from NULL

**Header hygiene**
- `48_duplicate_header_names_case_variant` — `Name` and `name`
- `49_header_with_leading_trailing_spaces` — `" customer_id "`

**Delivery shape — flags a genuine feature gap**
- `61_gzipped.csv.gz` — **compression is never mentioned anywhere in the README's 95 sections.** Gzipped CSV is ubiquitous in real feeds. This is a missing *capability*, not just a missing fixture.
- `62_multipart_split` — `part-00000` / `part-00001` with the header only in the first (Spark-style output). Interacts with manifest handling and with "one logical dataset spans N objects".
- `66_triple_nasty` — a field containing delimiter **and** newline **and** escaped quote simultaneously

### 3.5 SCD2 correctness — table stakes and common failures

**Table stakes**

1. Surrogate key **stable and independent** of the change hash and of `valid_from`.
2. Half-open intervals `[valid_from, valid_to)` — version N's `valid_to` equals version N+1's `valid_from`. Closed intervals produce either gaps or double-counting on point-in-time joins.
3. **Database-enforced invariants**, not just tests. PostgreSQL gives this for free and most implementations skip it:
   - No overlapping versions per business key: `EXCLUDE USING gist (business_key WITH =, tstzrange(valid_from, valid_to) WITH &&)`
   - Exactly one current row: partial unique index `ON (business_key) WHERE is_current`

   This converts "SCD2 correctness" from a property you hope for into one the database refuses to violate. Strong recommendation.
4. Explicit tracked-attribute list for change detection (dbt's `check_cols`), with untracked attributes handled Type 1 in place.
5. `valid_from` from **business/source effective time**, never ingestion time (§57).
6. Idempotent re-application: replaying the same batch yields zero new versions, because the hash matches the current row.
7. Explicit delete semantics. dbt's three-way vocabulary is the right one: `ignore` | `invalidate` (close the interval) | `new_record` (insert a tombstone version carrying an `is_deleted` flag). Adopt these names.
8. Current-row `valid_to` convention chosen once and enforced centrally (sentinel high-date preferred over NULL — dbt added `dbt_valid_to_current` for exactly this reason).

**What commonly gets it wrong**

| Failure | Consequence |
|---|---|
| Ingestion time used as `valid_from` | History is wrong whenever the pipeline runs late; backfills actively rewrite reality |
| Closed intervals (`valid_to = next valid_from` exactly, inclusive) | Overlapping ranges; rows counted twice in point-in-time joins |
| Hash includes volatile columns (ingest timestamp, row number, filename) | **Every** load creates a new version. The most common SCD2 bug in the wild |
| Hash over un-normalized values | Phantom versions from `PL` vs `pl `, NFC vs NFD, `1.50` vs `1.5` |
| Late change appended as a new *current* version | The correction lands at the end of history instead of in the middle; the true current state is now wrong |
| SK derived from `hash(bk, valid_from)` | Late corrections mutate SKs; existing facts dangle |
| "Absent from batch ⇒ retire" on incremental extracts | First delta file retires the whole dimension |
| No idempotency token on the SCD load | A retry after partial commit creates duplicate versions |
| Consecutive identical events not pre-deduplicated | Version churn (§60 catches this) |

**Late-arriving correction algorithm** (worth writing as an explicit requirement, since it is the part dbt cannot do). Given a late change for key `K` at effective time `T` with hash `H`:

1. Locate version `V` whose interval contains `T`.
2. If `V.hash == H` → no-op (this is what makes re-application idempotent).
3. Otherwise split: close `V` at `T`; insert a new version `[T, V.valid_to)` with `H`.
4. Walk forward, collapsing adjacent versions with identical hashes.

Because every step is a pure function of `(K, T, H)` and existing state, the operation is naturally idempotent — re-running it converges. That property is what should be asserted in tests (`SCD` + `QUAL`).

### 3.6 Realistic delivery-semantics claims

§30 forbids overclaiming. Here is the honest, defensible statement — recommend this becomes a literal document in `docs/`:

| Boundary | Guarantee | Why |
|---|---|---|
| Source → platform | **At-least-once** | Object re-sends, Airflow task retries, pod restarts. Same posture Debezium documents for CDC: no changes missed, but an event may be delivered more than once on failure/restart, and duplicates are visible downstream. |
| Platform → analytical target tables | **Effectively-once (idempotent merge)** | Final row state equals the state produced by applying each source record once. |
| Platform → side effects (metrics, `processed/` object writes, notifications, logs) | **At-least-once** | They are outside the database transaction and cannot be made atomic with it. |
| Append-only audit tables without a natural key | **At-least-once** | Deliberately so — duplicates there are evidence, not corruption. |
| Backfill / replay of an interval | **Convergent** | Re-running an interval drives the target to the same state. Stronger and more useful than a delivery guarantee. |

Effectively-once holds only under four stated preconditions, and the docs must say so:

1. Batch identity is content-derived, not filename-derived (§24).
2. The merge key set is complete **and enforced by a database UNIQUE constraint** — not checked in application code.
3. The merge and the watermark advance commit in a **single transaction** (Singer's STATE contract: records preceding an acknowledged state are durably written).
4. Processing is deterministic (§67) — no wall-clock, randomness or listing-order dependence.

**Never claim exactly-once.** Debezium's own documentation reserves exactly-once for the narrow case of running as a Kafka Connect source connector using Connect's EOS support (KIP-618, built on Kafka transactions) — i.e. it is a property of the *transport*, not of CDC or of ingestion. With no broker in scope (PROJECT.md excludes streaming), exactly-once is not available and claiming it would be false.

The deliverable artifact should be **a table listing each target table and its guarantee**, because the guarantee genuinely differs per table. That is more honest and more useful than a single platform-wide adjective.

---

## 4. Feature Dependencies

```
INFRA (cluster + PG + MinIO + Vault)
  ├──requires──> SEC (Vault wiring, K8s auth, Airflow secrets backend)
  ├──requires──> ORCH (Airflow, TaskFlow, KubernetesPodOperator)
  └──requires──> META* (control-plane schema in analytical PG)   <-- see gaps

META* ──enables──> LOAD(ledger) ──enables──> DEDUP(audit)
                                └──enables──> INCR(watermarks)
                                └──enables──> OBS(lineage, metrics)
                                └──enables──> VALID(persisted results)
                                └──enables──> SCHEMA(registry)

CSV (parse) ──> SCHEMA (infer/validate) ──> CSV(normalize) ──> VALID ──> DEDUP ──> LOAD
                                                    │
                                                    └──must precede──> SCD(hashing)

SCHEMA(versioning) ──requires-for──> SCHEMA(drift) ──requires-for──> SCHEMA(evolution policy)
SCHEMA(versioning) ──requires-for──> INCR(backfill w/ historical schema, DoD 51)

LOAD(identity+ledger) ──requires-for──> LOAD(idempotency) ──requires-for──> ORCH(retries enabled)
                                                          ──requires-for──> INCR(backfills)
LOAD(staging+atomic publish) ──requires-for──> DEDUP(cross-batch) ──requires-for──> SCD(merge)

INCR(event vs processing time) ──requires-for──> SCD(effective dating)
INCR(late/out-of-order)        ──requires-for──> SCD(late-arriving corrections)
CDC(event model + ordering)    ──requires-for──> SCD(CDC-fed, DoD 67/68)
DEDUP ──enhances──> SCD (replayed events must not create versions, DoD 68)

VALID(persisted results) ──requires-for──> anomaly detection (§53 baselines)
INFRA(CI profile parameterization) ──requires-for──> CICD(ephemeral kind E2E, DoD 113)
everything ──precedes──> OBS(runbooks, DoD 112)

QUAL(fixture corpus) ──should-precede──> CSV   [TDD; corpus is the spec]
```

### Dependency notes

- **`META` is the critical serialization point.** Watermarks, dedup audit, validation results, schema registry, batch ledger and lineage are all *writes into one control-plane schema*. Build it once, early, coherently. Accreting it capability-by-capability guarantees six migrations and inconsistent foreign keys. This is the single strongest structural recommendation in this document.
- **Normalization must precede hashing.** Both `DEDUP` (exact-row hash) and `SCD` (change hash) hash normalized content. If normalization lands after either, both produce phantom differences. Hard ordering edge.
- **Retries depend on idempotency, not the reverse.** §92 places idempotency in Phase 8, but Airflow retries are on by default. Enabling retries before the batch ledger exists means the platform is *silently duplicating data* during every earlier phase, and the tests that would catch it (DoD 83) do not exist yet.
- **`SCHEMA` versioning gates two later things**, drift detection and historical backfill resolution. It is a prerequisite, not a nice-to-have.
- **`CDC` before `SCD`-fed-by-CDC**, but SCD 0/1/2 themselves do **not** depend on CDC. They can be built from CSV batches alone. Do not let CDC block SCD.
- **Runbooks trail everything** — they document real observed failure modes, so writing them early produces fiction.

### Parallelizable vs strictly sequential

**Genuinely independent — safe to run as concurrent plans:**

| Workstream A | Workstream B | Why independent |
|---|---|---|
| `INFRA` / `SEC` (cluster, Helm, Vault) | `CSV` engine core | The `csv_processor` package is pure Python + fixtures. It needs no cluster, no Airflow, no Postgres. **Largest parallelization win in the project.** |
| `CICD` lint/typecheck/unit workflow | everything | Can land on day one; no dependencies. |
| `QUAL` fixture corpus authoring | `CSV` implementation | Corpus is the specification; ideally leads. |
| encoding / dialect / header / filename detectors | each other | Four independent detectors, no shared state. |
| date / number / boolean / null / whitespace normalizers | each other | Independent pure functions. |
| `SCD` Type 0 and Type 1 | `SCD` Type 2 | Different mechanisms; 0/1 are trivial next to 2. |
| `VALID` rule types (completeness, uniqueness, validity, pattern) | each other | Independent. Only referential integrity needs multi-dataset load. |
| `OBS` logging standards | everything | Cross-cutting convention, adoptable immediately. |

**Strictly sequential — cannot be parallelized:**

1. `INFRA` → `ORCH` → any E2E work
2. `INFRA`(Vault) → `SEC`(K8s auth + policies) → `SEC`(Airflow secrets backend)
3. `META` schema → {watermarks, dedup audit, validation results, lineage, batch ledger}
4. `CSV`parse → `SCHEMA` → `CSV`normalize → `VALID` → `DEDUP` → `LOAD` (real data dependency along the pipeline)
5. `LOAD`(ledger) → idempotency → retries enabled → backfills
6. `LOAD`(staging+merge) → `DEDUP`(cross-batch) → `SCD`(merge)
7. `INCR`(watermark) → `INCR`(backfill correctness)
8. `VALID`(persisted results) → anomaly detection baselines
9. `INFRA`(CI-profile Helm values) → `CICD`(ephemeral-kind E2E)
10. everything → `OBS`(runbooks)

---

## 5. Gaps in the README (worth adding as requirements)

Specified in the prose but with **no corresponding DoD item**, or absent entirely. Each is a candidate REQ.

| # | Gap | Where | Severity | Proposed REQ |
|---|---|---|---|---|
| G1 | **Control-plane metadata schema as a deliverable.** §13/§23/§27/§28/§62/§82/§83 all describe metadata that must be tracked; nothing says "design one coherent schema". No DoD item. | cross-cutting | **CRITICAL** | `META-01` ingestion control-plane schema (dataset registry, object/file/batch registries, schema registry, watermarks, validation results, dedup audit, quarantine, lineage) |
| G2 | **Lineage is queryable.** §83 mandates lineage; §94 has no item for it. | §83 | **HIGH** | `OBS-xx` "where did this row come from?" answerable in SQL |
| G3 | **Metrics are exposed.** §82 lists a metric set; no DoD item. PROJECT.md adds Prometheus/Grafana. | §82 | HIGH | `OBS-xx` |
| G4 | **File integrity / still-uploading detection.** §42 mandates it; no DoD item. Classic production incident. | §42 | HIGH | `LOAD-xx` |
| G5 | **Manifest and control-file support.** §41 and §43 specify them; no DoD item. Table stakes for file ingestion. | §41,§43 | HIGH | `LOAD-xx` / `ORCH-xx` (sensor) |
| G6 | **Missing-expected-file detection.** §44 distinguishes "none available" from "expected but missing"; no DoD item. | §44 | MEDIUM | `OBS-xx` (pairs with freshness, DoD 59) |
| G7 | **Quarantine re-drive path.** Quarantine is specified; getting data *out* is not. | §51 | HIGH | `VALID-xx` |
| G8 | **Deterministic processing.** §67 mandates it; no DoD item. Highly testable (same input twice ⇒ identical output hash). | §67 | MEDIUM | `QUAL-xx` property test |
| G9 | **Configuration versioning.** §66 mandates it; no DoD item (DoD 24 covers contracts only). | §66 | MEDIUM | `SCHEMA-xx` |
| G10 | **Concurrency / race protection.** §86–§87 specify it; no DoD item. Needs advisory locks or unique constraints per `(dataset, batch)`. | §86,§87 | MEDIUM | `LOAD-xx` |
| G11 | **Data retention enforcement.** Specified *twice* (§64 and §91); no DoD item. | §64,§91 | MEDIUM | `OBS-xx` or `INFRA-xx` |
| G12 | **Anomaly detection.** §53 specifies it; no DoD item (DoD 31 only implies it). Depends on G1/G3. | §53 | MEDIUM | `VALID-xx` |
| G13 | **Compressed input (`.gz`/`.zip`) and multi-part datasets.** Never mentioned in 95 sections. Ubiquitous in real feeds. | — | **HIGH** | `CSV-xx` |
| G14 | **Timezone/DST correctness as a tested property.** §14 mandates DST handling; nothing tests it. | §14 | MEDIUM | `QUAL-xx` fixture + test |
| G15 | **Resource requests/limits per workload.** §85 specifies it; only indirectly in DoD 10. | §85 | LOW | `ORCH-xx` |
| G16 | **Unicode normalization (NFC/NFD) in the normalization layer.** §18 covers whitespace only. Breaks dedup and SCD hashing. | §18 | MEDIUM | `CSV-xx` |

## 6. Over-engineering candidates (trim or defer)

Not "remove from scope" — "reduce ambition, or get it for free as a byproduct".

| Item | README | Assessment |
|---|---|---|
| **Intra-file checkpointing at 250k-row granularity** | §38 | §38 itself says "do not add unnecessary complexity for small files". Explicit offset checkpointing is a lot of machinery. **Get it free instead:** commit in chunks and record `last_committed_chunk_ordinal` on the batch ledger row. That *is* checkpointing, delivered as a byproduct of G1 + DoD 52. Only build offset-level resume if a fixture demands it. |
| **Multi-row / hierarchical headers** | §11 | "Where practical" already hedges. Merged/multi-level headers are a genuine rabbit hole with no canonical flattening. **Detect and reject with a clear diagnostic**, do not attempt to flatten. |
| **CDC before-image / after-image handling** | §29 | With no broker in scope (PROJECT.md), full before/after image support is speculative. **Define the event model and prove it with a CSV-delivered CDC feed** (op column + sequence + key). Defer before-image until a source produces one. |
| **`defer` outcome for referential integrity** | §47 | Implies a deferred-orphan queue with re-evaluation, expiry and ordering. Ship `fail`/`quarantine`/`warn`. |
| **ML anomaly detection** | §53 | Already correctly scoped to simple thresholds. Hold that line. |
| **OpenTelemetry distributed tracing** | PROJECT.md (not README) | Genuinely valuable, genuinely expensive, and the largest optional addition. **Sequence last**; the platform is complete without it and its value depends on everything else already emitting context. |
| **Full six-strategy deduplication matrix up front** | §26 | Six strategies (exact-row, business-key, key+timestamp, latest-wins, source-priority, batch-aware) is the eventual target. Build the *strategy interface* plus exact-row and business-key first; the rest are additional implementations of a solved interface, not new architecture. |

---

## 7. Prioritization matrix

| Capability | Value | Cost | Priority | Rationale |
|---|---|---|---|---|
| `INFRA` vertical slice (cluster + PG + MinIO + Airflow) | HIGH | HIGH | **P1** | Nothing runs without it (§93) |
| `META` control-plane schema (G1) | HIGH | LOW | **P1** | Cheapest high-leverage item; everything writes into it |
| `LOAD` batch ledger + content-hash identity | HIGH | LOW | **P1** | Unlocks idempotency, retries, dedup audit, lineage. ~1 table + a UNIQUE constraint |
| `CSV` core parse (dialect, encoding, header) | HIGH | HIGH | **P1** | The product |
| `ORCH` TaskFlow + KubernetesPodOperator | HIGH | MEDIUM | **P1** | The vertical slice |
| `SCHEMA` contracts + versioning | HIGH | MEDIUM | **P1** | Gates drift, evolution, historical backfill |
| `LOAD` staging + atomic publish + merge | HIGH | MEDIUM | **P1** | Consumers must never see partial data |
| `QUAL` fixture corpus | HIGH | MEDIUM | **P1** | The corpus *is* the spec; should lead implementation |
| `SEC` Vault + K8s auth + Airflow backend | HIGH | HIGH | **P1** | §81 is a third of the DoD's security half; blocks nothing else though |
| `VALID` structural + quality + quarantine + reports | HIGH | MEDIUM | **P2** | Needs `META` and `SCHEMA` |
| `DEDUP` strategies + audit | HIGH | MEDIUM | **P2** | Needs ledger + target |
| `INCR` watermarks + backfill | HIGH | MEDIUM | **P2** | Needs ledger |
| `CICD` full pipeline + ephemeral kind | MEDIUM | MEDIUM | **P2** | Lint/typecheck subset is P1; E2E needs the CI Helm profile |
| `SCD` 0/1/2 + keys + change detection | HIGH | HIGH | **P2** | Needs merge + normalization |
| `SCHEMA` drift + evolution policy | MEDIUM | MEDIUM | **P3** | Needs versioning |
| `SCD` late-arriving corrections | HIGH | HIGH | **P3** | The hardest correctness problem; needs everything above |
| `CDC` framework + CDC→SCD | MEDIUM | MEDIUM | **P3** | Architectural demonstration; no live source in scope |
| `VALID` reconciliation + control totals + RI | MEDIUM | MEDIUM | **P3** | Needs multi-dataset load |
| `OBS` Prometheus/Grafana + OTel | MEDIUM | HIGH | **P3** | Depends on `META` emitting context |
| Anomaly detection | MEDIUM | LOW | **P3** | Cheap *after* validation history exists |
| `OBS` runbooks | HIGH | LOW | **P3** | Must trail real failure modes |

**The one sequencing opinion that contradicts the README:** §92 places idempotency, checksums and dedup in Phase 8. **Move batch identity + the load ledger into the Phase 5 vertical slice.** It costs roughly one table and one UNIQUE constraint. Deferring it means every phase between 5 and 8 is silently duplicating data on retry, and every later capability (dedup audit, watermarks, lineage, replay) retrofits into a ledger that does not yet exist. This is the single largest avoidable rework risk in the project, and Delta Lake's `(txnAppId, txnVersion)` design is the proof that the token is small.

---

## 8. Comparative tool analysis

How comparable tools solve these problems, and what this platform should take. **The README mandates a bespoke Python engine — none of these are proposed for adoption.** These are design references.

| Problem | How they solve it | What to take |
|---|---|---|
| Schema drift policy | **dlt**: 3×4 matrix — `{tables, columns, data_type}` × `{evolve, freeze, discard_row, discard_value}`, settable per-resource/per-source/per-run with run-level override; `freeze` raises `DataValidationError` | **The matrix shape.** Not a boolean. `discard_value` (drop the bad value, keep the row) vs `discard_row` are distinct outcomes worth having |
| SCD2 metadata columns | **dbt snapshots**: `dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id` (SK, generated independently), `dbt_updated_at`, `dbt_is_deleted`; all renameable | Column vocabulary, and critically that the SK (`dbt_scd_id`) is **independent** of the change hash |
| SCD2 change detection | **dbt**: `timestamp` strategy (trusts source `updated_at`) vs `check` strategy (`check_cols` list or `all`) | Ship **both**. `check` is the deterministic option when the source has no trustworthy `updated_at` — which is most CSV feeds |
| Delete handling | **dbt**: `hard_deletes = ignore \| invalidate \| new_record` | Adopt the three-way vocabulary verbatim. §59 says "DELETE semantics must be configurable" without naming the options |
| Current-row end date | **dbt**: `dbt_valid_to_current` sentinel (e.g. `9999-12-31`) instead of NULL | Adopt. NULL breaks range predicates |
| "Absent ⇒ deleted?" | **dlt** scd2: natural key as `merge_key` ⇒ absent rows **not** retired; partition column as `merge_key` ⇒ retire only within the loaded partition | Model delivery mode explicitly per dataset. This is a real correctness trap, not a preference |
| Incremental cursor | **Airbyte**: `where cursor > last_max`; documented at-least-once; coarse cursor granularity means the source cannot know what was already sent. **dlt**: `lag` parameter re-scans an attribution window and *disables* source-side dedup when set | Use `>=` + idempotent merge, never `>`. Consider dlt's `lag` for attribution windows. Both boundary failure modes become tests |
| Watermark durability | **Singer/Meltano**: STATE with `bookmarks{stream: {replication_key, replication_key_value}}`; records preceding an acknowledged STATE are guaranteed written | Exactly §28. Commit the watermark in the same transaction as the data |
| Idempotent writes | **Delta Lake**: `(txnAppId, txnVersion)` recorded in the transaction log; a re-run batch with the same pair is recognized and ignored | The batch ledger + UNIQUE `(dataset, batch_key)` is the same token. Note their documented pitfall (reusing an appId after a checkpoint reset silently drops writes) |
| Delivery semantics | **Debezium**: at-least-once by default — on failure/restart the same event may be delivered more than once and duplicates are visible downstream; exactly-once only via Kafka Connect EOS (KIP-618) | Direct citation for §30. Exactly-once is a transport property, unavailable without a broker |
| DQ result retention | **Great Expectations**: Checkpoints produce Validation Results retained per run for trend tracking; Data Docs render from that history; Actions fire on result | Persist results as **rows**, not just artifacts. Neither GX nor Soda ships record-level quarantine — that part is genuinely bespoke |
| Ragged rows | **polars** (#10585): inconsistently handled, sometimes silently truncating. **Python stdlib csv**: hard-fails on NUL bytes (cpython #71767) | Treat ragged rows as errors, never pad/truncate. Pre-filter NUL bytes before the csv reader ever sees them |

---

## Confidence Assessment

| Area | Confidence | Basis |
|---|---|---|
| Category taxonomy & DoD mapping | **HIGH** | Derived directly from the README; arithmetic verified (114/114) |
| Dependency graph | **HIGH** | Data-flow dependencies are structural, not opinion |
| dlt schema contracts, dbt snapshot semantics | **MEDIUM** | Context7 against official repos (dlt-hub/dlt, dbt-labs/docs.getdbt.com), including source code |
| Airbyte / Delta Lake mechanics | **MEDIUM** | Fetched from vendor docs directly |
| Debezium delivery semantics | **MEDIUM** | Official docs page 403'd to the fetcher; claim corroborated across multiple sources incl. the official docs title |
| CSV edge cases | **MEDIUM** | Two claims corroborated by upstream issue trackers (cpython #71767, polars #10585); the rest is domain knowledge |
| GX / Soda quarantine patterns | **LOW** | Search-result summaries only; the negative claim ("no built-in record quarantine") was not directly confirmed in vendor docs — verify before relying on it |
| Effort estimates (S/M/L/XL) | **MEDIUM** | Judgement, not measurement. Calibrate after the first phase |

## Sources

- dlt — schema contracts and SCD2 merge: https://github.com/dlt-hub/dlt (`docs/website/docs/general-usage/schema-contracts.md`, `merge-loading.md`, `common/schema/schema.py`, `extract/incremental/transform.py`) [MEDIUM]
- dbt — snapshots / SCD2: https://github.com/dbt-labs/docs.getdbt.com (`hard-deletes.md`, `dbt_valid_to_current.md`, `snapshot_meta_column_names.md`, `invalidate_hard_deletes.md`) [MEDIUM]
- Airbyte — Incremental Append + Deduped: https://docs.airbyte.com/using-airbyte/core-concepts/sync-modes/incremental-append-deduped [MEDIUM]
- Delta Lake — idempotent writes: https://docs.delta.io/latest/delta-streaming.html [MEDIUM]
- Debezium — exactly-once delivery: https://debezium.io/documentation/reference/stable/configuration/eos.html and https://debezium.io/documentation/faq/ [MEDIUM]
- Meltano Singer SDK — stream state / incremental replication: https://sdk.meltano.com/en/latest/implementation/state.html, https://sdk.meltano.com/en/latest/incremental_replication.html [MEDIUM]
- Great Expectations — Validation Results / Data Docs: https://docs.greatexpectations.io/docs/0.18/reference/learn/terms/validation_result/ [LOW]
- CPython — csv NUL byte limitation: https://github.com/python/cpython/issues/71767 [MEDIUM]
- polars — ragged CSV silent truncation: https://github.com/pola-rs/polars/issues/10585 [MEDIUM]
- CSV edge-case surveys: https://www.importcsv.com/blog/csv-parsing-errors, https://www.elysiate.com/blog/csv-file-format-specification-rfc-4180-explained [LOW]
- Project master specification: `/home/user/projects/airflow-platform/README.md` (§1–§95, DoD §94) [HIGH]
- Project scope: `/home/user/projects/airflow-platform/.planning/PROJECT.md` [HIGH]

---
*Feature research for: production-like local Kubernetes Airflow ETL platform with universal CSV ingestion*
*Researched: 2026-08-11*
