---
phase: 04
slug: vertical-slice-csv-to-analytical-postgresql
status: verified
threats_open: 0
asvs_level: 1
created: 2026-08-14
---

# Phase 04 — Security

> Per-phase security contract: threat register, accepted risks, and audit trail.
>
> Register source: all 11 `04-*-PLAN.md` `<threat_model>` blocks
> (`register_authored_at_plan_time: true` for every plan — no
> retroactive-STRIDE construction was needed). 29 distinct entries, keyed by
> `(plan, threat_id, component)` since several Threat IDs are intentionally
> reused across plans for unrelated concerns. This audit is a **gap-closure
> re-audit**: plans 04-10/04-11 landed after the initial 11-plan execution to
> close CR-01/CR-02/WR-01 found by `04-REVIEW.md`/`04-VERIFICATION.md`;
> threats T-04-15/T-04-16/T-04-17/T-04-18 map to that round and were
> independently re-verified here (code, tests, and live cluster), not merely
> cited from the prior reports.

---

## Trust Boundaries

| Boundary | Description | Data Crossing |
|----------|-------------|----------------|
| DB-write API → analytical PostgreSQL | Every `MetadataRepository`/`Publisher` method issues SQL against operator-controlled but data-bearing tables | File hashes, sizes, statuses, CSV-derived business values |
| discover/ingest pod → object store (MinIO) | `list_objects`/`get_object`/`put_object` cross into MinIO with a bucket/key built partly from config, partly from discovered filenames | Object keys, byte content, assignment-document JSON |
| Airflow worker/scheduler pod → Kubernetes API (namespace `etl`) | The identity that creates KPO pods crosses from `airflow`'s control plane into `etl`'s data plane | Pod specs, no credentials |
| Host developer kubeconfig → live cluster Secrets | `scripts/etl-secrets.sh`, `scripts/ingest-demo.py`, `scripts/repair-duplicate-file-lineage.py` run with the operator's own cluster-admin kubeconfig | DB passwords, MinIO app credentials |
| KPO pod (`csv-processor` SA) → analytical PostgreSQL / MinIO | The pod authenticates as `etl_app` (narrowly granted) and MinIO's `etl-app` credential, never the schema owner or MinIO root | DSN, S3 access/secret keys (via `secretKeyRef`, never literal) |
| MinIO-stored assignment JSON → `ingest`'s process | Written by `discover_files` (trusted, this phase), read by `ingest` running in a different pod — the boundary crossing that makes T-04-02 real | `AssignmentDocument` JSON |
| `dataplat.cli.main()` → installed entry points | A malicious/misconfigured installed package declaring a `dataplat.plugins` entry point could inject code at CLI startup | N/A — build-time package set only |
| DAG-parse time → Airflow Variable read | `Variable.get("csv_processor_image")` executes on every DAG-file parse | Image reference string (not secret) |
| E2E/demo/repair scripts → live cluster `kubectl`/`psql`/S3 | Test and operator tooling with cluster-admin-equivalent access; not production code, but must not leak credentials | DB/MinIO credentials, read fresh per call |
| Heartbeat thread → `meta.ingestion_runs` | Internal, already-trusted write path whose *correctness* (not authorization) backs the platform's audit/traceability Core Value | `lease_expires_at`, `rows_read`, `rows_parsed` |
| `ingest()`'s failure path → XCom-visible `Receipt` | Widening which exceptions produce a written Receipt must not widen what that Receipt discloses | `status`/`run_id` only |

---

## Threat Register

| Threat ID (plan) | Category | Component | Disposition | Mitigation | Status |
|---|---|---|---|---|---|
| T-04-01 (04-01) | Tampering | `postgres.py`'s 4 new/changed methods | mitigate | Every value via `%s` placeholders (`metadata/postgres.py` L100-470); zero f-string value interpolation; independently confirmed via `ruff check --select S608` → 0 findings across the file | closed |
| T-04-06 (04-01) | Tampering | `finalize_publication`'s caller-supplied `conn` | accept | `conn` is opened only inside `run_ingest`'s own `with ctx.db.connection() as conn, conn.transaction():` (`pipeline/run.py` L303-333) and passed straight through; no other call site exists; docstring states the constraint explicitly (`repository.py` L410-414) | closed |
| T-04-07 (04-01) | Information Disclosure | `ObjectSummary`/`list_objects` results | accept | `ObjectSummary` (`storage/objectstore.py` L71-90) carries only `key`/`etag`/`size_bytes`/`last_modified` — no credential or object-content field | closed |
| T-04-03 (04-02) | Elevation of Privilege | `kubernetes/rbac-etl.yaml` | mitigate | `Role etl-pod-launcher-role` scoped to `pods`/`pods/log`/`pods/exec`/`events` verbs, namespace `etl` only; `RoleBinding` names exactly 2 subjects (`airflow-worker`, `airflow-scheduler`); no `ClusterRole`, no wildcard verb/resource (`kubernetes/rbac-etl.yaml` L34-68) | closed |
| T-04-08 (04-02) | Elevation of Privilege | `etl_app`'s Postgres grants | accept | `scripts/etl-secrets.sh` contains zero `GRANT` statements (confirmed by grep — it only sets a password). Full current grant picture across migrations 0001-0008: `SELECT,INSERT,UPDATE` on 7 named tables + `USAGE` on schemas `meta`/`normalized`/`staging` (0008 fixes previously-inert table grants) + `CREATE` on `staging` only (0007, required for `StagingLoader`'s own per-run `UNLOGGED` tables). No `DROP`/`ALTER`/superuser anywhere. **Correction to the original plan-time rationale**: the literal text "only ever receives SELECT, INSERT, UPDATE" is now incomplete (migrations 0007/0008, landed in later plans, added schema-level `USAGE`/`CREATE`) — the *scope* (no DDL outside the throwaway `staging` schema, no privilege on other schemas' structure) is unchanged and still least-privilege; this entry's rationale is updated here to match ground truth | closed |
| T-04-09 (04-02) | Information Disclosure | `scripts/etl-secrets.sh` | mitigate | `_apply_secret` pipes a manifest to `kubectl apply -f -` on stdin (L79-97); `_ensure_csv_processor_db_secret` pipes `ALTER ROLE` to `kubectl exec -i ... psql` on stdin (L145-148) — no credential ever in argv or echoed | closed |
| T-04-SC (04-02) | Tampering (supply chain) | N/A — no new package installs | mitigate | No new dependency introduced by this plan; `apache-airflow-providers-amazon` already cleared and bundled in the stock image | closed |
| T-04-02 (04-03) | Tampering, Elevation of Privilege | `AssignmentDocument` | mitigate | `model_config = ConfigDict(extra="forbid", frozen=True)` on `FileAssignment`/`BatchAssignment`/`AssignmentDocument` (`models/assignment.py` L44,60,93). Read-side (04-05) verified: `csv_processor/cli.py` L245 calls `model_validate_json` before any field of `doc` is dereferenced (config resolution at L254, source construction at L255+) | closed |
| T-04-05 (04-03) | Denial of Service | `discover_files`'s cap enforcement | mitigate | `discovery.py` L310-318: `candidates[: config.batching.max_units_per_run]`; excess units are already-upserted `PENDING` rows, picked up next call, never lost | closed |
| T-04-10 (04-03) | Tampering | Content hash/`object_uri`/filename reaching SQL | mitigate | `discovery.py` contains zero raw `conn.execute`/SQL text — every value crosses exclusively through `MetadataRepository`'s parameterized methods (`create_file`, `find_file_by_content_hash`, `get_or_create_batch`, `link_batch_file`, `get_or_create_ingestion_run`) | closed |
| T-04-01 (04-04) | Tampering | `staging.py`'s COPY, `merge.py`'s publish SELECT | mitigate | `staging.py` L224-228: `copy.write_row(enriched_row)` binds every row as data via `cursor().copy()`; `merge.py` L117-119: `_PUBLISH_SQL.format(staging_table=staging_table)` — the only dynamic fragment is a table identifier built from `ctx.config.dataset`+`run_id`, never CSV content; independently confirmed via `ruff check --select S608` on both files → 0 findings | closed |
| T-04-11 (04-04) | Tampering | Concurrent publish races | mitigate | `pipeline/run.py` L307-310: caller-side `SELECT pg_advisory_xact_lock(hashtextextended(%s,0))` immediately before `publisher.publish(...)`, same transaction; `merge.py` L56: `DISTINCT ON (customer_id)` structurally prevents the `ON CONFLICT DO UPDATE` cardinality-violation failure mode | closed |
| T-04-02 (04-05) | Tampering, Elevation of Privilege | `ingest`'s assignment-document read | mitigate | `csv_processor/cli.py` L245: `AssignmentDocument.model_validate_json(raw_text)` — a `ValidationError` is caught and re-raised as `ConfigurationError` (L246-252) before any field is used; this is the READ side closing 04-03's write-side half | closed |
| T-04-04 (04-05) | Information Disclosure | `_write_xcom`'s FAILED-path payload (`ingest`'s `Receipt`) | mitigate | `_failure_receipt` (`csv_processor/cli.py` L188-214) constructs `Receipt(run_id=..., status="FAILED", rows_*=0, duration_ms=0, report_uri=None)` — no `str(exc)`, no traceback, ever | closed |
| T-04-12 (04-05) | Tampering | `dataplat.plugins` entry-point loading | accept | `dataplat/cli.py` L113-114 loads entry points by name via `importlib.metadata`; `docker/csv-processor/Dockerfile` L80 runs `uv sync --locked --all-packages --no-dev --no-editable` — the installed-package set is fixed and lockfile-verified at build time | closed |
| T-04-SC (04-05) | Tampering (supply chain) | `csv-processor/pyproject.toml`'s new `click` dependency | mitigate | `dependencies = ["dataplat", "click>=8.4,<9"]`; `uv.lock` L782-784 shows `click==8.4.2`, already present as a transitive dependency before this change | closed |
| T-04-01 (04-06) | Tampering | Test-seeding SQL in new/extended test files | mitigate | `test_publish_merge.py` L664 etc.: seeded values cross via `%s` (e.g. `conn_a.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (_ADVISORY_LOCK_KEY,))`); DDL/table-name fragments are test-internal constants, not attacker input, matching production's identifier-vs-value discipline | closed |
| T-04-11 (04-06) | Tampering | Concurrent publish races (verification) | mitigate | `test_publish_merge.py` L575-620 `test_advisory_lock_serializes_concurrent_publishers` — real two-connection/background-thread proof; a documented negative-case check (lock temporarily removed) is recorded in the test's own docstring, not hidden | closed |
| T-04-03 (04-07) | Elevation of Privilege | `namespace="etl", service_account_name="csv-processor"` on every KPO task | mitigate | `_common/kpo.py` L69-70 sets both on every call; both `discover` and `ingest` tasks in `csv_ingest_customers.py` use `**common_kpo_kwargs(...)` exclusively; regression-tested by `tests/unit/test_dag_structure.py::test_namespace_and_service_account` (L91-103, asserts `checked >= 3`) | closed |
| T-04-05 (04-07) | Denial of Service | `ingest.expand(arguments=...)` | mitigate | `csv_ingest_customers.py` L142: `max_active_tis_per_dag=5` on the expanded `ingest` task, layered on top of `discover_files`'s own `batching.max_units_per_run` cap (T-04-05 04-03) | closed |
| T-04-04 (04-07) | Information Disclosure | `Variable.get("csv_processor_image")` | accept | `grep -rn "Variable.get" airflow/dags/` returns exactly one call site (`_common/kpo.py` L71), reading only the image-tag string — no credential is ever read via `Variable.get` anywhere in this DAG folder | closed |
| T-04-04 (04-08) | Information Disclosure | Credentials via `analytics_connection`/MinIO fixtures | mitigate | `tests/e2e/slice/conftest.py` contains zero `print`/`log` statements; credentials are read fresh per call via `_read_secret_data`/`_read_app_secret`-style helpers (base64-decoded live Secret reads), never cached to disk | closed |
| T-04-13 (04-08) | Denial of Service | `kubectl delete pod` against a live-cluster pod | accept | `test_pod_kill_retry.py` L234 issues the real delete; the whole module is gated behind the imported `_require_cluster` skip-with-reason fixture (`tests/e2e/cluster/conftest.py`), never runnable without an explicit live local cluster | closed |
| T-04-09 (04-09) | Information Disclosure | `scripts/ingest-demo.py`'s credential handling | mitigate | `_read_minio_credentials` (L260-295) invokes `scripts/minio-credentials.sh show` via subprocess with no credential in argv, parses stdout; `_read_analytics_credentials` reads a Secret via `kubectl get secret -o json`; no `print()` in the file ever emits a password/secret/access-key value (confirmed by reading every `print(` call site) | closed |
| T-04-14 (04-09) | Elevation of Privilege | A future edit adding a CLI-trigger shortcut | mitigate | Module docstring names D-15 and the prohibition explicitly without spelling out the scanned literals; `grep -n "dags trigger\|dags_trigger\|trigger_dag" scripts/ingest-demo.py` → 0 matches, independently confirmed | closed |
| T-04-15 (04-10) | Tampering | `_heartbeat_loop` → `heartbeat_ingestion_run` | mitigate | `pipeline/run.py` L202-208 calls `ctx.metadata.heartbeat_ingestion_run(...)` — `update_ingestion_run_status` no longer appears in this file (`grep -c` = 0 vs `heartbeat_ingestion_run` = 1); `postgres.py` L380-390: `UPDATE ... WHERE run_id = %s AND status = 'RUNNING'`. **Live-cluster-confirmed** (independently queried this session): `meta.ingestion_runs` shows `13 SUCCEEDED, 1 PENDING, 0 RUNNING`; the deployed image tag (`csv_processor_image=localhost:5001/csv-processor:2247d2c`) is git-confirmed (`git merge-base --is-ancestor 18808cf 2247d2c` → true) to contain this fix | closed |
| T-04-16 (04-10) | Repudiation | `find_file_by_content_hash`'s duplicate-lineage resolution | mitigate | `postgres.py` L187: `ORDER BY file_id ASC` before `LIMIT 1`, both in the Protocol docstring and implementation. **Live-cluster-confirmed** (independently queried this session, wider check than originally required): a generic `IS DISTINCT FROM <group minimum>` query across every `(dataset_id, content_sha256)` group in the live `meta.files` table returned 0 rows | closed |
| T-04-17 (04-10) | Tampering | `scripts/repair-duplicate-file-lineage.py`'s live `UPDATE` against `meta.files` | mitigate | `_REPAIR_SQL` (L124-136) is `WHERE`-guarded, touches only `duplicate_of_file_id`, fully static (zero runtime string interpolation — confirmed via `ruff --select S608` clean); `grep -n "file_id ==\? *10"` → 0 matches (generic, not hardcoded); `main()` L426-451 performs read → write → re-verify, exits non-zero on an incomplete repair | closed |
| T-04-18 (04-11) | Information Disclosure | `_failure_receipt`'s payload for the new `except Exception:` branch | accept | `csv_processor/cli.py` L277-296: both `except DataPlatformError:` and `except Exception:` call the identical `_failure_receipt(doc)` helper — byte-for-byte the same `status`/`run_id`-only shape; `except Exception:` is listed after `except DataPlatformError:` so it never shadows the narrower clause, and never intercepts `BaseException`-only families. **Live-cluster-confirmed**: deployed image commit is a descendant of `ee3d591` (git ancestry check), and `git show 2247d2c:.../cli.py \| grep -c "except Exception:"` → 1 | closed |

*Status: open · closed*
*Disposition: mitigate (implementation required) · accept (documented risk) · transfer (third-party)*

---

## Accepted Risks Log

| Risk ID | Threat Ref | Rationale | Accepted By | Date |
|---------|------------|-----------|--------------|------|
| AR-01 | T-04-06 (04-01) | `finalize_publication`'s `conn` parameter breaks the "every method opens its own connection" convention by design (META-03 requires it to share the publish transaction). Verified: the only call site is `run_ingest`'s own locally-opened `conn` — never exposed to an external or untrusted caller. | 04-01 plan (verified this audit) | 2026-08-14 |
| AR-02 | T-04-07 (04-01) | `ObjectSummary` (key/etag/size/last_modified) is not secret and discloses no credential or file content; `list_objects` is a metadata-only operation. | 04-01 plan (verified this audit) | 2026-08-14 |
| AR-03 | T-04-08 (04-02) | `etl_app` receives table-level `SELECT,INSERT,UPDATE` (7 tables) + schema `USAGE` (`meta`,`normalized`,`staging`) + schema `CREATE` on `staging` only (needed for per-run ephemeral staging tables). No `DROP`/`ALTER`/superuser anywhere; no privilege outside the three named schemas. Rationale text updated this audit to reflect migrations 0007/0008, added by later plans, which were not reflected in 04-02's original threat-model wording. | 04-02 plan; rationale corrected 2026-08-14 (this audit) | 2026-08-14 |
| AR-04 | T-04-12 (04-05) | The `dataplat.plugins` entry-point set is fixed by the lockfile at image-build time (`uv sync --locked`); not a runtime-discoverable or writable surface. A broken/malicious installed plugin is a build-time review concern, not a runtime attack surface. | 04-05 plan (verified this audit) | 2026-08-14 |
| AR-05 | T-04-04 (04-07) | `Variable.get("csv_processor_image")` reads only a non-secret image-reference string; confirmed the sole `Variable.get` call site in `airflow/dags/`. | 04-07 plan (verified this audit) | 2026-08-14 |
| AR-06 | T-04-13 (04-08) | `kubectl delete pod` in `test_pod_kill_retry.py` is a deliberate, test-scoped destructive action against the developer's own local kind cluster, gated behind `_require_cluster`'s skip-with-reason; never reachable against a shared or production target. | 04-08 plan (verified this audit) | 2026-08-14 |
| AR-07 | T-04-18 (04-11) | The `except Exception:` branch added for WR-01 writes the identical, already-reviewed minimal `Receipt` shape (`status`/`run_id` only) the `except DataPlatformError:` branch already used — widening which exceptions produce a Receipt does not widen what that Receipt discloses. | 04-11 plan (verified this audit) | 2026-08-14 |

*Accepted risks do not resurface in future audit runs.*

---

## Security Audit Trail

| Audit Date | Threats Total | Closed | Open | Run By |
|------------|----------------|--------|------|--------|
| 2026-08-14 | 29 | 29 | 0 | gsd-security-auditor (Claude Sonnet 5) |

**Methodology this audit:** Every `mitigate`-disposition threat verified by direct grep/read of the current implementation file (not the plan's prose, not the SUMMARY.md narrative) — file:line evidence recorded above. `ruff check --select S608` run independently against every SQL-touching file named in the register (`postgres.py`, `staging.py`, `merge.py`, `discovery.py`, `repair-duplicate-file-lineage.py`) — 0 findings. For T-04-15/T-04-16/T-04-17/T-04-18 (the gap-closure round), verification additionally included live-cluster queries run fresh in this session (not reused from `04-REVIEW.md`/`04-VERIFICATION.md`): a generic duplicate-lineage integrity query against the live `meta.files` table (0 orphaned/mis-pointed rows, wider check than originally required), the live `meta.ingestion_runs` status distribution (0 `RUNNING`, no stuck/regressed rows), and a `git merge-base --is-ancestor` chain confirming the currently-deployed `csv-processor` image (`2247d2c`) contains all three gap-closure commits (`18808cf` CR-01, `9b59385` CR-02, `ee3d591` WR-01) — closing the "deployment-currency" warning `04-VERIFICATION.md` had flagged as still-open at its own time of writing. All 11 `04-*-SUMMARY.md` files independently re-checked for a `## Threat Flags` section: none exist (confirmed by grep, not assumed from the task prompt).

---

## Sign-Off

- [x] All threats have a disposition (mitigate / accept / transfer)
- [x] Accepted risks documented in Accepted Risks Log
- [x] `threats_open: 0` confirmed
- [x] `status: verified` set in frontmatter

**Approval:** verified 2026-08-14
