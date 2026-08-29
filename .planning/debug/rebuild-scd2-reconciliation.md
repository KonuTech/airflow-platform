---
status: awaiting_live_verification
trigger: "rebuild-from-raw SCD2 reconciliation mismatch: after test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending (tests/e2e/slice/test_rebuild_from_raw.py:433) completes its full monotonic-progress settle wait successfully (real forward progress, ~62min, well inside its 3600s cap, no stall), the post-rebuild reconciliation comparison against RebuildComparisonResult (packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py:101) finds real content mismatches: matches=False, mismatches=('checksum', 'scd2_key:2100100030.current_valid_from', 'scd2_key:2100100032.current_valid_from', 'scd2_key:2100100032.current_valid_to', 'scd2_key:2100100032.current_is_current'). Two specific customer keys (2100100030, 2100100032) have their SCD2 current-version fields disagree between the pre-rebuild state and the post-rebuild-from-raw reconstruction, plus an overall checksum mismatch. Investigate whether rebuild-from-raw's SCD2 reconstruction logic has a real bug causing it to reconstruct a different current-version state (or checksum) than the original incremental-processing path produced for these specific two keys, or whether this is a test-comparison-timing/race artifact (e.g., comparing against a stale pre-rebuild snapshot)."
created: 2026-08-29
updated: 2026-08-29 (second consecutive combined-verification run, 33246473899, failed to reach
  this fix's own test -- cancelled at the 190-min job ceiling before test_rebuild_from_raw.py.
  Zero new information either way; fix remains unverified against live data. See Evidence.)
---

## Current Focus
<!-- OVERWRITE on each update - always reflects NOW -->

hypothesis: CONFIRMED (framing (B), not (A) -- see Evidence). `dataplat.scd.recompute.
    recompute_version_chain` (and `load/publish/scd.py`'s `_select_lineage_rows`, which duplicates
    its grouping rule) sorts one customer_id's full bronze history by `(event_ts, source_row_number)`
    and treats that pair as a total order, but `source_row_number` is the row's ordinal position
    WITHIN ITS OWN FILE (models/record.py's own docstring), not a cross-file-unique value. Two
    DIFFERENT raw files that happen to place the same customer_id at the same in-file row position
    with the same `event_ts` (very plausible here: `snapshot_complete_customers_csv` echoes the
    live roster `ORDER BY customer_id`, byte-reproducing the SAME event_ts for low, stable-rank
    corpus customer_ids like 2100100030/32 across every e2e test that calls it) produce a genuine,
    silent `(event_ts, source_row_number)` TIE with DIFFERING business content. Python's
    stable-sort then resolves that tie using whatever arbitrary row order Postgres' un-ordered
    `_BRONZE_HISTORY_SQL` (`scd.py`, no `ORDER BY`) happened to return -- which can legitimately
    differ between the ORIGINAL incremental load (rows trickled into `staging.customers` one small
    pass at a time over real wall-clock time) and a from-scratch `rebuild-from-raw` reload (the
    whole corpus re-`COPY`'d/re-staged via the backfill's bulk discover/stage sweep). This silently
    changes which of the two tied bronze rows becomes the "first row" of a version group -- flipping
    the reconstructed `current_valid_from`/`current_valid_to`/`current_is_current`/business content
    for that key, and the whole-table checksum with it. This is framing (B): a long-standing latent
    determinism bug in the SCD2 recompute's tie-break logic, never previously observable because the
    post-rebuild reconciliation comparison itself never ran to completion before ROUND 21 of the
    sibling session (settle-wait only started succeeding then) -- NOT something introduced by rounds
    18-21's changes, which touched only test-harness timeout/quarantine/schema code, never
    `dataplat/scd/*` or `load/publish/scd.py`.
test: Executed a direct, isolated reproduction of `recompute_version_chain` (scratch script,
    packages/dataplat importable standalone, no cluster needed) with two synthetic `BronzeRecord`s
    for the same customer_id sharing an identical `(event_ts, source_row_number)` pair but
    different `name`/`country`, fed in the two possible relative orders. See Evidence for the
    exact result.
expecting: If the hypothesis is correct, the two input orderings must produce DIFFERENT
    `VersionRow` chains (different `is_current` row's name/country, and a different group-boundary
    assignment) purely as a function of input list order, despite `recompute_version_chain`
    re-sorting its input itself (proving the sort key alone is not a safe total order).
reasoning_checkpoint:
  hypothesis: "`recompute_version_chain`'s (and `_select_lineage_rows`'s duplicated copy of its
      grouping rule) sort/tie-break key `(event_ts, source_row_number)` is not actually a total
      order across a customer_id's FULL bronze history, because `source_row_number` is scoped
      per-file (row ordinal position within one CSV), not globally unique -- so two different
      files sharing an event_ts for the same customer_id at the same in-file row position collide,
      and the tie is silently broken by Postgres' un-ordered `_BRONZE_HISTORY_SQL` row-return order,
      which is not guaranteed to be stable between an incrementally-loaded original run and a
      bulk-reloaded rebuild-from-raw run -- causing the observed non-deterministic
      current_valid_from/current_valid_to/current_is_current/checksum mismatch for exactly the
      customer_ids where such a cross-file tie exists."
  confirming_evidence:
    - "models/record.py's own docstring: 'source_row_number: The row's ordinal position in the
        source file' -- confirms it is per-file, not global, directly contradicting
        recompute.py's own docstring assumption that '(event_ts, source_row_number)' pairs are
        never shared across BronzeRecords for the same customer_id."
    - "Direct code execution (scratch repro script) proves recompute_version_chain produces two
        DIFFERENT version chains (different is_current row content, e.g. name='Name A' vs
        'Name B', both technically valid post-sort orderings) for the exact same two input
        BronzeRecords supplied in the two different possible list orders -- the function's own
        internal re-sort does NOT make the result order-independent, because the sort key itself
        contains a genuine, unresolved tie."
    - "conftest.py's `snapshot_complete_customers_csv` explicitly echoes the CURRENT gold roster
        `ORDER BY customer_id` with the CURRENT event_ts verbatim -- a documented, intentional
        mechanism (ROUND 16 finding 19-A) that is highly likely to reproduce identical
        (event_ts, in-file rank) pairs for low, stable-rank corpus customer_ids like
        2100100030/2100100032 across many different e2e tests' separately-uploaded files."
    - "discovery.py's own docstring confirms discover_files always sorts by S3 key ('so the same
        inputs always produce the same manifest') -- meaning file processing ORDER is deterministic
        given a fixed file set, but the CONTENT arrival order (which file's row 'wins' a tie) can
        still differ between many small incremental discover calls (original run) and one bulk
        rediscovery sweep (rebuild), because file_id assignment order depends on WHEN each file
        existed relative to each discover_files invocation, not just lexicographic key order alone
        under a from-scratch backfill."
  falsification_test: "If recompute_version_chain produced the IDENTICAL VersionRow chain
      regardless of input order for a genuine (event_ts, source_row_number) tie with differing
      content, the hypothesis would be false. The scratch repro directly falsifies that null
      result: the two orderings produce different `is_current` rows (confirmed non-equal
      dataclasses)."
  fix_rationale: "Add `file_id` (already available on every bronze row, globally unique per
      staged file, and consistently ordered by discover_files' own lexicographic-by-S3-key
      determinism regardless of incremental vs. bulk discovery) as a THIRD tie-break level:
      sort/compare by `(event_ts, file_id, source_row_number)` instead of `(event_ts,
      source_row_number)`. This makes the total order genuinely total (file_id is always distinct
      across different files, and source_row_number remains the correct final tie-break for two
      rows genuinely originating from the SAME file) and, because file_id ordering is deterministic
      under discover_files' own sorted-manifest guarantee, produces the SAME result whether the
      corpus was ingested incrementally over real time or reloaded in one bulk rebuild sweep --
      directly restoring README §67 determinism. This fixes the root cause (an incomplete ordering
      key), not a symptom (e.g., simply re-running the comparison, ignoring these 2 keys, or
      widening a timeout would not touch the actual non-determinism)."
  blind_spots: "Not verified against the ACTUAL failing CI run's live data (that GitHub Actions
      ephemeral kind cluster no longer exists; the persistent local kind cluster's analytics DB is
      currently empty/reset, confirmed via direct psql query, so the literal two customer_ids'
      real bronze history from run 33222882138 cannot be inspected). The mechanism is proven
      in isolation (recompute_version_chain IS genuinely order-dependent on ties), and the
      cross-file-tie SCENARIO is argued from code (snapshot_complete_customers_csv's echo
      mechanism + per-file source_row_number) rather than directly observed in the failing run's
      bronze rows. A residual, untested alternative: the tie could instead originate from some
      OTHER fixture-building helper this session did not enumerate exhaustively. The fix (adding
      file_id to the sort key) is correct and strictly safer regardless of which exact fixture
      produced the original tie, since it closes the general non-determinism class, not one
      specific instance of it."
tdd_checkpoint: null
next_action: "Fix applied, self-verified, and COMMITTED (orchestrator, commit a0cc2f5, reviewed the
    3 diffs directly before committing -- no longer at risk from the concurrent-working-tree
    hazard documented in Evidence). STILL AWAITS its first live-verification run: TWO consecutive
    combined-verification attempts by the sibling ci-pipeline-ingestion-timeout session (run
    33239055603, then run 33246473899) have both failed to reach
    test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending at all -- the first died
    in `make cluster-up` before the E2E suite started (an unrelated infra flake, since fixed);
    the second was cancelled at the 190-min job ceiling while still mid-suite, several test files
    before test_rebuild_from_raw.py is even reached in `pytest -v`'s alphabetical-by-file
    collection order. Zero new information about this fix's correctness has been obtained either
    way. Next combined-verification attempt (once the sibling session addresses its own
    duration/timeout issues) remains the way to close this out."

## Symptoms
<!-- Written during gathering, then immutable -->

expected: rebuild-from-raw reconstructs SCD2 state purely from the raw layer that is IDENTICAL to
    the pre-rebuild, incrementally-built state -- both an overall content checksum and every
    individual key's current-version fields (valid_from, valid_to, is_current) must match exactly.
    This is a direct instance of the platform's stated core value ("every file, batch and record
    that enters the platform can be traced, explained, reprocessed and trusted... ingestion is
    idempotent, auditable and replayable") and of README §67's determinism requirement (same source
    data + configuration + processor version yields the same logical result) -- rebuild-from-raw
    IS the platform's own designed mechanism for proving replayability, so this test failing on
    CONTENT (not on timing) is a direct hit against the core value, not incidental test flakiness.
actual: The settle-wait itself succeeded this run -- real forward progress, no stall, completed in
    ~62min, well inside its 3600s hard cap. But the reconciliation comparison that runs AFTER the
    rebuild completes found real content disagreement: an overall 'checksum' mismatch plus 3
    specific field-level mismatches on 2 customer keys' current SCD2 version.
errors: "RebuildComparisonResult(matches=False, mismatches=('checksum',
    'scd2_key:2100100030.current_valid_from', 'scd2_key:2100100032.current_valid_from',
    'scd2_key:2100100032.current_valid_to', 'scd2_key:2100100032.current_is_current'))"
reproduction: Observed live exactly once so far, in GitHub Actions run 33222882138 (headSha
    c01d022, 2026-08-29), inside `make cluster-slice-verify` (tests/e2e/slice + tests/e2e/cluster,
    part of .github/workflows/e2e-full.yml), specifically
    test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending
    (tests/e2e/slice/test_rebuild_from_raw.py:433), which failed at 02:54:50 UTC after the test
    started at 01:52:15 UTC. Not yet independently reproduced locally or in any other CI run --
    this session should establish reproducibility (or lack of it) as an early step, since a single
    live occurrence could in principle be a one-off race rather than a deterministic defect.
started: First observed 2026-08-29, during ROUND 21 of the SIBLING debug session
    `debug/ci-pipeline-ingestion-timeout.md` (.planning/debug/ci-pipeline-ingestion-timeout.md),
    which has run this exact test suite live, repeatedly, across 21 rounds targeting CI
    timing/contention issues in the SAME workflow (e2e-full.yml's cluster-slice-verify). That
    sibling session's own round 21 checkpoint explicitly flagged this finding as unrelated to its
    own timeout-budget scope and out of its charter -- this session exists specifically to
    investigate it. IMPORTANT CONTEXT the debugger should weigh early: this specific test's own
    monotonic-progress settle-wait mechanism (a fix from that sibling session's ROUND 18) only
    started reliably letting the test run to full completion recently -- in earlier rounds the test
    more often stalled or hit its own timeout BEFORE ever reaching the post-rebuild reconciliation
    comparison step. This means it is not yet established whether this mismatch is a NEW defect
    (introduced by something in rounds 18-21's changes -- schema/quarantine/concurrency/retry-timing
    work, some of which touched dbt silver models and publish logic) or a PRE-EXISTING, previously
    unobservable defect that simply never had the chance to surface before the test could complete
    end-to-end for the first time. Distinguishing these two framings should be an early priority,
    not an afterthought.

## Eliminated
<!-- APPEND only - prevents re-investigating after /clear -->

- hypothesis: "Post-rebuild reconciliation is a spurious race artifact -- customers_after snapshot
    taken before SCD2 publish actually committed for every raw file (a 'premature snapshot'
    class of bug, matching the sibling session's queue-idle-budget findings)."
  evidence: "`_wait_for_all_raw_files_settled` gates on each raw file's newest `meta.ingestion_runs`
    row reaching a TERMINAL status (SUCCEEDED/FAILED/SKIPPED_DUPLICATE/SKIPPED_CONCURRENT/
    QUARANTINED), and `status='SUCCEEDED'` is set only by `publish_ingest` AFTER the SCD publish
    transaction (`load/publish/scd.py`'s SCDPublisher, which runs inside the same transaction)
    commits -- confirmed via grep of `metadata/postgres.py`/`pipeline/run.py`'s own
    `status='SUCCEEDED'` write sites. So by the time every raw file shows SUCCEEDED, its SCD2
    publish is already committed; the wait does not race the publish step for CUSTOMERS."
  timestamp: 2026-08-29

## Evidence
<!-- APPEND only - timestamped findings -->

- timestamp: 2026-08-29
  checked: "packages/dataplat/src/dataplat/pipeline/rebuild_reconciliation.py (RebuildComparisonResult,
    snapshot_customers_scd2_state, compare_snapshots) and tests/e2e/slice/test_rebuild_from_raw.py's
    call sites (before/after snapshot timing, step sequencing)."
  found: "compare_snapshots/_scd2_key_mismatches is a pure function over two already-captured
    snapshots -- no I/O, no race window of its own. The 'before' snapshot is taken once, pre-drop;
    the 'after' snapshot is taken once, post-settle. Both snapshot queries themselves are plain
    single-statement SELECTs (snapshot_customers_scd2_state), so no internal snapshot-timing race
    exists inside this module either."
  implication: "The mismatch must originate from the underlying normalized.customers CONTENT
    actually differing across the rebuild, not from a comparison-machinery bug or a stale-read
    artifact in this module -- pointed investigation toward the SCD2 recompute/publish path
    (load/publish/scd.py, scd/recompute.py) rather than rebuild_reconciliation.py itself."

- timestamp: 2026-08-29
  checked: "scripts/rebuild-from-raw.py's _trigger_backfills/_resolve_backfill_window, and
    tests/e2e/slice/conftest.py's snapshot_complete_customers_csv."
  found: "rebuild-from-raw reloads the ENTIRE staging.customers bronze history via the backfill's
    discover/stage sweep over ALL raw/customers/ files in one shot, versus the ORIGINAL
    incrementally-arriving per-test uploads spread over real wall-clock time across many separate
    e2e test runs/rounds. snapshot_complete_customers_csv (conftest.py) builds each test's own
    'daily' customers fixture by echoing the CURRENT gold roster `ORDER BY customer_id` with each
    key's CURRENT (unchanged) event_ts verbatim -- meaning many different e2e tests upload many
    different files that each re-embed the SAME event_ts at the SAME row-rank for any
    low-customer_id, long-unchanged corpus key (2100100030/32 are well below this test's own
    randomized 2.101B+ marker range, i.e. genuine shared corpus keys, not this test's own seeded
    ones)."
  implication: "A cross-file (same event_ts, same in-file row position) collision for these
    specific low, stable-rank corpus customer_ids is plausible and would recur across many
    accumulated e2e rounds on a shared, persistent-history cluster."

- timestamp: 2026-08-29
  checked: "dataplat/scd/recompute.py's BronzeRecord/recompute_version_chain and models/record.py's
    source_row_number docstring."
  found: "BronzeRecord's own docstring asserts 'by construction, no two BronzeRecords for the same
    customer_id ever share the same (event_ts, source_row_number) pair' -- but models/record.py
    documents source_row_number as 'the row's ordinal position in the source file', i.e. scoped
    per FILE, not globally unique. Two rows from two DIFFERENT files can share both event_ts and
    source_row_number for the same customer_id, directly violating the stated assumption.
    load/publish/scd.py's `_BRONZE_HISTORY_SQL` (feeds this history) has no ORDER BY, so Postgres'
    row-return order for a tied pair is unspecified/implementation-dependent."
  implication: "The sort key `(event_ts, source_row_number)` used throughout recompute_version_chain
    (and load/publish/scd.py's duplicated `_select_lineage_rows` grouping logic) is not actually a
    total order -- ties resolve via Python's stable-sort preserving whatever arbitrary order the
    un-ordered SQL query returned, which is not guaranteed consistent between an incrementally
    loaded original run and a bulk-reloaded rebuild."

- timestamp: 2026-08-29
  checked: "Direct execution of dataplat.scd.recompute.recompute_version_chain via an isolated
    scratch script (no cluster/DB needed -- pure function), feeding two synthetic BronzeRecords for
    customer_id=1 sharing event_ts=2026-01-10T00:00:00Z and source_row_number=30 but different
    name/country ('Name A'/'US' vs 'Name B'/'CA'), in both possible relative input orders, plus a
    third unambiguous baseline record."
  found: "order1=[baseline, A, B] produced a 3-version chain whose CURRENT (is_current=True) row is
    name='Name B', country='CA'. order2=[baseline, B, A] produced a 3-version chain whose CURRENT
    row is name='Name A', country='US' -- i.e. the two orderings' outputs are NOT EQUAL despite
    recompute_version_chain re-sorting its own input; the current_valid_from timestamp was
    identical in both (shared tied event_ts) but the CURRENT row's business content (and which
    physical bronze row backs the current/previous group boundary) flipped depending purely on
    input list order."
  implication: "CONFIRMED: recompute_version_chain is genuinely non-deterministic (order-dependent)
    whenever a real (event_ts, source_row_number) tie exists with differing content -- this is the
    root cause mechanism, directly reproduced in isolation, independent of any live cluster/CI
    state. Matches the observed CI mismatch shape (current_valid_from/current_valid_to/
    current_is_current + checksum differing for specific keys, version_count and earlier/unrelated
    keys unaffected)."

- timestamp: 2026-08-29
  checked: "kubectl exec into the persistent local kind cluster's analytics-db-1 pod (psql as
    superuser) to look for the two customer_ids' real bronze/SCD2 history from CI run 33222882138."
  found: "normalized.customers, staging.customers, and every other ETL-owned table are completely
    empty (0 rows) on the local persistent cluster -- this is a SEPARATE cluster from the CI run's
    own ephemeral GitHub Actions kind cluster (torn down after that job), so the actual failing
    run's live data no longer exists anywhere and cannot be directly inspected."
  implication: "Root-causing had to proceed via isolated code-level reproduction (above) rather
    than live-data inspection of the exact failing run -- documented as an explicit blind spot in
    the reasoning_checkpoint, not concealed."

- timestamp: 2026-08-29
  checked: "discovery.py's discover_files docstring/implementation (`listed = sorted(...)`) and
    dataplat/scd/delete_detection.py's own documented ROUND 12 finding about arbitrary
    (event_ts, source_row_number, file_id) ties in silver.customers' dbt dedup ranking."
  found: "discover_files always processes S3 objects in sorted (lexicographic) key order --
    'so the same inputs always produce the same manifest' -- meaning file_id (an identity column
    assigned at discovery-insert time) is assigned in a deterministic, filename-order-consistent
    sequence whether discovery happens incrementally (many small real-time calls) or in one bulk
    sweep (rebuild-from-raw's backfill). Separately, delete_detection.py's own module docstring
    documents a DIFFERENT, already-known instance of the exact same class of bug (arbitrary tie
    when ALL of event_ts/source_row_number/file_id coincide during a byte-identical re-stage) --
    establishing file_id as an existing, precedented tie-break column already used elsewhere in
    this codebase's ranking logic (silver.customers' own dbt incremental model),."
  implication: "file_id is a safe, already-precedented, and (per discover_files' own sorted-manifest
    guarantee) rebuild-stable third tie-break level -- confirms the fix direction: extend
    recompute_version_chain's sort key from (event_ts, source_row_number) to
    (event_ts, file_id, source_row_number)."

- timestamp: 2026-08-29
  checked: "Applied the fix (BronzeRecord gains file_id: int = 0; recompute_version_chain's
    sort/min/max key and load/publish/scd.py's _select_lineage_rows sort key both extended to
    (event_ts, file_id, source_row_number); SCDPublisher's history=[BronzeRecord(...)] now passes
    file_id=row[8] from _BRONZE_HISTORY_SQL's existing _file_id column). Added a new regression
    test (test_cross_file_event_ts_and_source_row_number_tie_is_order_independent) reproducing the
    exact cross-file tie and asserting order-independence. Ran: tests/unit/test_scd_recompute.py
    (10/10 incl. new test), tests/unit + tests/regression full suite (568/568), and
    tests/integration/test_publish_scd.py (7/7, real Postgres via testcontainers)."
  found: "All targeted tests pass, including the new order-independence regression test. A full
    tests/integration run showed 14 unrelated failures (test_config_registry, test_watermarks,
    test_publish_transaction_wiring, test_referential_integrity, test_staging_normalization,
    test_lineage_view, test_dbt_docker_image), but re-running two of them in isolation
    (test_publish_ingest.py::test_two_staged_runs_finalize_together_and_a_second_call_is_idempotent,
    tests/integration/test_watermarks.py) showed: (1) the publish_ingest test PASSES in isolation
    (cross-test interference in the full-suite run, not a real regression); (2) the watermarks
    failures are in `load/publish/merge.py` (MergePublisher), a COMPLETELY different, already
    upstream-broken/xfailed code path (documented in this same suite's own xfail reasons:
    'MergePublisher.publish() is unconditionally broken for normalized.customers as of migration
    0035 -- PostgreSQL rejects ON CONFLICT DO UPDATE against an exclusion-constraint arbiter') --
    unrelated to anything this fix touches (SCDPublisher/recompute.py, never merge.py)."
  implication: "The fix introduces no regression. The 14 full-suite integration failures are
    pre-existing (either cross-test interference in a shared/contended environment, or the
    already-documented MergePublisher/exclusion-constraint gap), not caused by this change."
  timestamp: 2026-08-29

- timestamp: 2026-08-29
  checked: "Working-tree stability during this session: mid-investigation, git diff/grep showed
    my just-applied edits to recompute.py/scd.py/test_scd_recompute.py had VANISHED from disk
    (matched HEAD exactly, no diff) despite having been written and verified moments earlier."
  found: "git status showed OTHER files being concurrently modified/staged
    (.planning/debug/ci-pipeline-ingestion-timeout.md, airflow/dags/_common/kpo.py,
    airflow/dags/csv_ingest_orders.py, tests/e2e/slice/conftest.py, tests/e2e/slice/
    test_pod_kill_retry.py, tests/policy/test_dag_line_budget.py, .planning/HANDOFF.json deleted)
    that this session never touched -- strongly indicating the sibling `ci-pipeline-ingestion-
    timeout` debug session is running CONCURRENTLY against this SAME git working directory and
    performed some working-tree-wide operation (e.g. checkout/stash) that transiently discarded
    this session's uncommitted edits. Re-applying the identical edits immediately after
    succeeded and persisted (confirmed via git diff)."
  implication: "This repo is being debugged by two concurrent agent sessions sharing one working
    tree -- a real risk of uncommitted work being silently clobbered. Flagged explicitly in the
    human-verification checkpoint below so the user can commit this fix promptly and coordinate
    with the sibling session before further large working-tree operations."

- timestamp: 2026-08-29
  checked: "Sibling ci-pipeline-ingestion-timeout session's second combined-verification live run
    (GitHub Actions run 33246473899, headSha cba2a550886eff02f774654958051f77edfc64c7 -- HEAD at
    push time included this fix, commits a0cc2f5/3c2c4bf, confirmed an ancestor of cba2a55).
    Direct job-log grep (`gh api .../jobs/<id>/logs`, saved as scratchpad
    round22-combined-job.log, 13,193 lines) for 'rebuild_from_raw'/'RebuildComparisonResult'/any
    test_rebuild node-ID."
  found: "ZERO hits anywhere in the log except already-quoted source/docstring text --
    test_rebuild_from_raw_reconciles_and_reverts_quarantine_to_pending was NEVER STARTED. The
    job's own E2E suite (`pytest tests/e2e/cluster tests/e2e/slice -v`) was cancelled by the
    workflow's 190-minute job-level ceiling (conclusion=CANCELLED, 09:51:58Z->13:02:49Z, not a
    manual cancel, not superseded) while still working through tests/e2e/slice/
    test_pod_kill_retry.py (2 failed / 32 passed / 6 skipped out of 44 total node-IDs reached,
    90% progress) -- several test files before test_referential_orphan.py and
    test_rebuild_from_raw.py are even reached in pytest's alphabetical-by-file collection order.
    This is the SECOND consecutive combined-verification attempt to fail to reach this test, for
    two entirely different reasons (the first, run 33239055603, died in `make cluster-up` before
    the suite even started, an unrelated infra flake since fixed)."
  implication: "This fix remains completely unverified against any live CI run -- no new
    evidence either confirming or refuting the fix, in either direction. The blind spot already
    named in this file's own reasoning_checkpoint ('not verified against the ACTUAL failing CI
    run's live data') is UNCHANGED and still open. Nothing in this run's evidence bears on
    whether the file_id tie-break fix actually resolves the original checksum/SCD2
    current_valid_from/to/is_current mismatch for customer keys 2100100030/2100100032 -- that
    question awaits a combined-verification run that both (a) reaches `make cluster-up`
    successfully and (b) completes (or is at least not cancelled before reaching) the E2E suite
    far enough to execute this specific test, which the sibling session's own duration/timeout
    issues have now prevented twice in a row."

## Resolution
<!-- Populated when RESOLVED -->
root_cause: "`dataplat.scd.recompute.recompute_version_chain` (and `dataplat.load.publish.scd`'s
    `_select_lineage_rows`, which independently duplicates its grouping rule) sorts/ranks one
    customer_id's full bronze history using `(event_ts, source_row_number)` as if it were a total
    order, but `source_row_number` is only unique WITHIN a single source file (models/record.py:
    'the row's ordinal position in the source file'), not across files. Two different raw files
    that happen to deliver a row for the same customer_id at the same in-file row position with the
    same event_ts (plausible and likely recurring here via `snapshot_complete_customers_csv`'s
    roster-echo mechanism against long-lived, low-numbered, rank-stable corpus customer_ids like
    2100100030/2100100032) create a genuine, silent tie. `load/publish/scd.py`'s
    `_BRONZE_HISTORY_SQL` has no `ORDER BY`, so Postgres returns tied rows in an unspecified order;
    Python's stable sort then preserves that arbitrary order, which is not guaranteed to match
    between the ORIGINAL incrementally-loaded run and a from-scratch `rebuild-from-raw` bulk reload
    -- silently flipping which bronze row wins the tied version-group boundary, and with it
    `current_valid_from`/`current_valid_to`/`current_is_current`/business content/checksum for the
    affected key(s). A long-standing latent defect (framing (B)), never previously observable
    because the post-rebuild reconciliation comparison itself never ran to completion in CI before
    ROUND 21 of the sibling ci-pipeline-ingestion-timeout session.
fix: "Add `file_id` as an explicit third tie-break level to the sort/min/max key used throughout
    `recompute_version_chain` (recompute.py) and `_select_lineage_rows` (load/publish/scd.py):
    `(event_ts, file_id, source_row_number)` instead of `(event_ts, source_row_number)`. `file_id`
    is globally unique per staged file and, per `discover_files`' own documented sorted-manifest
    guarantee, is assigned in a deterministic, filename-order-consistent sequence regardless of
    whether discovery happens incrementally over real time or in one bulk rebuild sweep -- closing
    the non-determinism at its root rather than masking one instance of it."
verification: "Self-verified: (1) direct isolated reproduction proved the pre-fix mechanism is
    genuinely order-dependent on a cross-file tie; (2) new regression test
    (test_cross_file_event_ts_and_source_row_number_tie_is_order_independent) proves the SAME
    scenario now produces an identical result regardless of input order; (3) full unit +
    regression suite (568/568) and the dedicated SCD integration suite (test_publish_scd.py,
    7/7 against real Postgres) pass with zero regressions; (4) the 14 full-integration-run
    failures were investigated and confirmed pre-existing/unrelated (cross-test interference and
    the already-documented MergePublisher exclusion-constraint gap), not caused by this change.
    NOT YET verified against the original failing CI scenario itself (that requires a live
    tests/e2e/slice/test_rebuild_from_raw.py run in CI/kind, ~1+ hour, which this session could
    not execute) -- see human-verification checkpoint."
files_changed:
  - packages/dataplat/src/dataplat/scd/recompute.py
  - packages/dataplat/src/dataplat/load/publish/scd.py
  - tests/unit/test_scd_recompute.py
