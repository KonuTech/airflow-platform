---
status: awaiting_human_verify
trigger: "Docker Desktop / WSL2 VM restarted unexpectedly during a long-running background planning session (kernel uptime showed only 6 minutes when investigated), silently breaking kind's DAGs hostPath mount (fell back to empty tmpfs on all 3 nodes) and freezing Airflow scheduling cluster-wide with zero exceptions logged -- same failure class as two prior documented incidents in this project (.planning/debug/resolved/dagrun-scheduler-stall.md, 2026-08-14; and the 2026-08-16 stuck-tasks incident). This is the third occurrence."
created: 2026-08-21
updated: 2026-08-21
---

## Current Focus
<!-- OVERWRITE on each update - always reflects NOW -->

hypothesis: RESOLVED (best-explanation, not single-cause-certain — see Resolution). Most likely mechanism: Windows' own sleep/modern-standby idle timer fired because it counts ONLY keyboard/mouse input, never background process activity (confirmed by research: no OS-level API exists to make Windows treat a background compute session as "not idle" short of the process itself calling `SetThreadExecutionState`, which nothing in this stack does) — putting the host to sleep during the ~40min unattended agent session. On wake, the WSL2 utility VM did not resume cleanly (a well-documented WSL2/Docker-Desktop class of bug — vsock zombie state, container network loss, "seems stuck on wake" — independent GitHub issues found for all three), and Docker Desktop's own VM-restart-on-resume behavior is consistent with the observed near-zero kernel uptime. Contributing/alternative factors, NOT ruled out: WSL2's own `vmIdleTimeout` (default 60s) — found LOW likelihood as the sole cause while Docker Desktop is running, because Docker Desktop keeps WSL2 distros alive via its own `wslkeepalive` process specifically to prevent this; and Docker Desktop's "Resource Saver mode" (auto-pauses/shuts down the Linux VM after ~30s with zero containers running) — found LOW likelihood as the direct trigger here specifically because the kind node containers were continuously running (not zero containers), which is Resource Saver's stated activation condition, though its own GitHub issues (#14827, #14656, microsoft/WSL#11066) show it independently causing WSL "lock"/freeze behavior in the wild. Given multiple plausible contributing mechanisms and no host-side crash log accessible from inside WSL, the SINGLE definitive trigger cannot be proven with certainty — sleep/modern-standby is the best-supported explanation given available evidence (idle-input-only detection is the one mechanism directly explaining a WALL-CLOCK-idle-but-compute-active trigger, matching this host's exact reported pattern of "only reproduces during long unattended sessions").
test: Completed — web research (WSL2 vmIdleTimeout defaults/limitations, Windows sleep/modern-standby idle-input-only detection, Docker Desktop Resource Saver trigger conditions, known WSL2/Docker-Desktop sleep-wake bugs) plus direct inspection of this host's `.wslconfig` and `docs/wsl/wslconfig.example` (both confirmed to have no idle-related settings prior to this session's fix).
expecting: Confirmed — Windows' idle-sleep timer is keyboard/mouse-input-only by design (Microsoft Q&A + community sources agree: "You cannot tweak Windows to ignore mouse movements... Windows always considers both keyboard and mouse input as user activity"), and WSL2/Docker-Desktop sleep-wake recovery is a documented, independently-reported failure class (not unique to this host).
next_action: DONE. Fix applied: (1) `vmIdleTimeout=-1` added to docs/wsl/wslconfig.example as defense-in-depth (cheap, addresses one contributing mechanism even though not proven the sole cause); (2) new self-heal detection: scripts/doctor-live.sh + `make doctor-live`/`make doctor-live-check` Makefile targets, tested against the real live cluster (positive control, currently healthy) and against fake-docker-simulated broken/repair/unknown-state cases (tests/e2e/cluster/test_doctor_live_mount_detection.py, 4/4 passing); (3) Windows-side recommendations documented below in Resolution.fix for the user to apply manually (cannot be applied from inside WSL). Awaiting human verification per goal=find_and_fix protocol.
reasoning_checkpoint:
  hypothesis: "Windows' idle-sleep timer (keyboard/mouse-input-only, ignoring background compute activity) put the host to sleep during the unattended ~40min agent session; WSL2/Docker Desktop's VM did not cleanly resume from that sleep (a documented failure class independent of this project), producing a fresh VM boot (near-zero kernel uptime) that broke the kind nodes' DAGs hostPath bind mount, falling back to tmpfs on all 3 nodes — the same downstream mechanism already root-caused in the two prior incidents on this host."
  confirming_evidence:
    - "Direct host inspection (this session): uptime showed the WSL2 kernel had been up only ~6 minutes at investigation time, while kind-node containers were 8 days old — a genuine VM restart, not a resume-from-suspend (which would preserve kernel uptime)"
    - "Web research (Microsoft Q&A / community sources, cross-checked): Windows' idle-sleep detector is documented as counting ONLY keyboard/mouse input; there is no setting to make it count background process activity, and preventing sleep during background work requires the process itself to call SetThreadExecutionState — nothing in the Claude Code / terminal / WSL stack does this"
    - "Web research: 'Docker Desktop keeps WSL2 distros running via its own wslkeepalive process' — directly weakens vmIdleTimeout as a standalone explanation while Docker Desktop is running, since its entire purpose is to prevent exactly that idle-teardown"
    - "Web research: Docker Desktop Resource Saver's documented trigger is 'no containers running for ~30s' — this host had 3 continuously-running kind node containers throughout the session, which does not match Resource Saver's stated activation condition, weakening it as the DIRECT trigger (though its own known-issues list shows it causing unrelated WSL lock/freeze bugs)"
    - "Web research found multiple independent, unrelated GitHub issues (microsoft/WSL#14005 'WSL2 enters unrecoverable zombie state after sleep/wake', docker/for-win#12981 'Docker Desktop seems stuck on computer wake up', Docker community forum reports of containers losing network after sleep) confirming WSL2/Docker-Desktop sleep-wake recovery is a broadly documented failure class, not something specific to a misconfiguration on this host"
    - "This host's actual .wslconfig (read directly this session) had zero idle/timeout-related settings prior to this fix — ruling out 'a misconfigured timeout was already set and simply too aggressive' as an alternative, simpler explanation"
  falsification_test: "If Windows Event Viewer's Power-Troubleshooter log (System log, Event ID 1/42) showed no sleep/wake transition spanning the incident window, this hypothesis would be falsified in favor of a pure Docker-Desktop/WSL2-internal restart with no host-sleep involvement. This could not be checked from inside WSL in this session (no access to Windows Event Viewer) — recorded as an open blind spot, not resolved."
  fix_rationale: "The root cause has two parts needing two different fixes: (1) the TRIGGER (Windows-side sleep behavior) requires a Windows power-plan/config change this repo cannot apply from inside WSL — documented as the recommended action for the user, per goal=find_and_fix's allowance for a host-environment fix to be 'the exact recommended change, documented' rather than applied; (2) the DOWNSTREAM symptom (tmpfs-fallback mount) is something this repo CAN detect and self-heal on an already-running cluster, closing the actual operational gap the orchestrator identified: `make doctor` only runs pre-flight, never against a live cluster, so nobody notices the freeze until Airflow visibly stalls. `scripts/doctor-live.sh` addresses that gap directly, using the exact same docker-exec-bypassing-Kubernetes diagnostic method the two prior incidents used manually, and the exact same `docker restart <node>` remediation both prior incidents applied by hand — now automated and testable."
  blind_spots: "Cannot access Windows Event Viewer's Power-Troubleshooter log from inside WSL to directly confirm a sleep/wake transition occurred at the exact incident timestamp — the sleep hypothesis is the best-supported explanation from indirect evidence (near-zero kernel uptime + documented idle-input-only Windows behavior + documented WSL2/Docker-Desktop sleep-wake bugs), not a directly-observed fact. Cannot rule out Docker Desktop Resource Saver as a contributing factor with full confidence — its documented trigger condition (zero running containers) does not obviously match this host's state (3 running kind containers), but Resource Saver's own known-issues list shows unpredictable WSL-lock behavior in the wild that isn't fully explained by its documented trigger alone. The self-heal script (doctor-live.sh) has been tested against the real healthy cluster and against a fake-docker-simulated broken/repair state, but NOT against an actual live tmpfs-fallback state on this cluster (not reproducible safely right now, per the orchestrator's own constraint) — correctness for the real broken case rests on the classification logic being identical to what was manually verified in the two prior incidents (`mount | grep /mnt/dags` showing `type tmpfs` vs `type ext4`), not on an end-to-end live reproduction."
tdd_checkpoint: null

## Symptoms
<!-- Written during gathering, then immutable -->

expected: The kind cluster (3 nodes) and all in-cluster workloads (Airflow API server/scheduler/DAG-processor/triggerer, MinIO, CloudNativePG, Vault) stay up and healthy for the duration of a long-running local session, including background agent work that is not itself resource-intensive.
actual: Discovered mid-session that Docker Desktop and the kind cluster were down/dead. On investigation: `docker ps` showed all kind-node containers "Up 2 minutes" against an 8-day container age (i.e. Docker Desktop's backing VM had just restarted); host `uptime` showed the WSL2 kernel itself had only been up 6 minutes; `mount` inside all 3 kind node containers showed `none on /mnt/dags type tmpfs (ro,relatime)` (the DAGs hostPath bind mount had fallen back to an empty tmpfs — the exact symptom from the two prior documented incidents); core Airflow pods (api-server, dag-processor, scheduler) were stuck in `Unknown`/`Error` state, several csv_ingest_* task pods were `Unknown`/`Error`/stuck `Pending`.
errors: No application-level exceptions anywhere (matches the prior incidents' signature: this class of failure produces silent freezing, not logged errors). No Windows-side crash dialog or WSL error was visible to the user at the terminal.
reproduction: Not a deterministic repro — happened spontaneously during a long (~40 min) background Claude Code agent session (a `/gsd:plan-phase` research + planning run) where the user was not actively interacting with the terminal. User confirms this is not a resource-intensive workload on this laptop. User reports the pattern (their best recollection) is that this only happens during long unattended/background sessions, not during active interactive use.
started: This session, 2026-08-21. Two prior, independently-diagnosed occurrences of the identical downstream symptom (DAGs mount -> tmpfs fallback -> scheduler freeze) are on record: 2026-08-14 (`.planning/debug/resolved/dagrun-scheduler-stall.md`, root-caused to "a Docker Desktop/WSL2-level restart", trigger itself not investigated) and 2026-08-16 (STATE.md Blockers/Concerns entry, same downstream symptom). This is the first time the *trigger* (why does the VM restart at all) is being investigated rather than just the downstream mount-recovery fix.

## Eliminated
<!-- APPEND only - prevents re-investigating after /clear -->

- hypothesis: Resource exhaustion (OOM) caused Docker Desktop/WSL2 to crash or be killed this occurrence
  evidence: Post-recovery `free -h` showed 20GB of 27GB available, 0B swap used; `docker info` reports 12 CPUs / 27.41GiB allotted. The background workload at the time (an LLM planning agent doing file reads/writes and light git operations) is not memory- or CPU-intensive. No OOM-killer messages found in accessible dmesg output (though full dmesg/journal access from inside this environment is limited).
  timestamp: 2026-08-21 (this session, pre-debug-session investigation)

- hypothesis: A misconfigured/pre-existing WSL2 idle timeout on this host (e.g. an already-set, too-aggressive `vmIdleTimeout`) is the direct and sufficient trigger
  evidence: Read `/mnt/c/Users/admin/.wslconfig` directly — contained only `[wsl2] kernelCommandLine = cgroup_no_v1=all` and `memory = 28GB`; no `vmIdleTimeout` or any other idle/timeout key was present before this session's fix. Also confirmed `docs/wsl/wslconfig.example` (the repo's own documented floor) had no idle-related settings. Rules out "an existing misconfiguration was simply too aggressive" as the explanation — there was no idle-timeout configuration at all.
  timestamp: 2026-08-21

- hypothesis: Docker Desktop Resource Saver mode is the direct trigger (auto-pauses/shuts down the WSL2 VM after a period of Docker-specific idleness)
  evidence: Web research confirms Resource Saver's documented activation condition is "no containers running for ~30 seconds" (Docker 4.22+ behavior, verified via docker.com's own release blog and Collabnix/dev.to explainers). This host had 3 continuously-running kind node containers (control-plane, worker, worker2) throughout the ~40min session — the containers themselves were the workload, they were never at zero. This does not match Resource Saver's stated trigger condition, so it is not well-supported as the DIRECT cause here, though its own GitHub issue history (docker/for-win#14827, #14656; microsoft/WSL#11066) documents it independently causing WSL "lock"/freeze behavior — kept as a documented, lower-confidence contributing/alternative factor, not eliminated outright.
  timestamp: 2026-08-21

## Evidence
<!-- APPEND only - facts discovered during investigation -->

- timestamp: 2026-08-21
  checked: `/mnt/c/Users/admin/.wslconfig` (direct read) and `docs/wsl/wslconfig.example` (repo's documented floor, D-11)
  found: Neither file set `vmIdleTimeout` or any idle/timeout-related key before this session. `.wslconfig` had only `kernelCommandLine = cgroup_no_v1=all` and `memory = 28GB`.
  implication: No pre-existing idle-timeout misconfiguration exists on this host — whatever mechanism triggered the restart was operating on WSL2/Docker Desktop/Windows defaults, not a project-introduced setting.

- timestamp: 2026-08-21
  checked: `scripts/doctor.sh` (full read)
  found: `make doctor` is a pure pre-flight — every check (inotify, disk, docker reachability, cgroup v2, kubectl/kind/helm versions, ports, repo path, host CPU/mem) runs BEFORE `cluster-up` and has no code path that inspects an already-running cluster's live mount state. Nothing in the repo previously detected a tmpfs-fallback condition on a cluster that was already up.
  implication: Confirmed the orchestrator's identified gap is real — a live cluster's silent freeze was previously only caught by a human noticing symptoms, never by tooling. Motivates the `doctor-live.sh` self-heal addition.

- timestamp: 2026-08-21
  checked: WebSearch — "WSL2 vmIdleTimeout .wslconfig default value", "vmIdleTimeout has no effect" GitHub discussion, "WSL Services being suspended despite vmIdleTimeout=-1" GitHub issue
  found: `vmIdleTimeout` defaults to 60000ms (60s); accepts -1 (never) / 0 (immediate) / positive ms. Multiple GitHub issues report it as unreliable even when explicitly set (microsoft/WSL#8659 "has no effect"; microsoft/WSL#13291 "services suspended despite vmIdleTimeout=-1"). Separately: "Docker Desktop keeps the distributions running using a dummy process called wslkeepalive" — meaning Docker Desktop actively works to prevent this exact idle-teardown while it's running.
  implication: vmIdleTimeout is a real mechanism but (a) documented as unreliable in the wild even when configured, and (b) actively counteracted by Docker Desktop's own keepalive — weakens it as the sole/primary explanation for THIS incident, though it remains cheap defense-in-depth to disable outright.

- timestamp: 2026-08-21
  checked: WebSearch — "Docker Desktop Resource Saver mode WSL2 idle pause VM", "Resource Saver mode trigger condition running containers low CPU"
  found: Resource Saver (Docker Desktop 4.22+) activates after ~30s with ZERO containers running, shutting down (not just pausing, since 4.22) the Docker Desktop Linux VM; exits automatically on any Docker command. Docker's own docs describe the trigger strictly as "no active containers," not a CPU-usage threshold.
  implication: This host's kind node containers ran continuously throughout the session (never zero), so Resource Saver's own documented trigger condition does not match this incident's timeline — present as a documented, lower-confidence alternative, not the leading hypothesis.

- timestamp: 2026-08-21
  checked: WebSearch — "Windows sleep timer ignores background process idle keyboard mouse input only", "Windows laptop modern standby vs sleep power plan background download continues"
  found: Windows' idle-to-sleep timer is driven by keyboard/mouse input only ("You cannot tweak Windows to ignore mouse movements... Windows always considers both keyboard and mouse input as user activity when calculating idle time"); a background process must explicitly call `SetThreadExecutionState` to prevent sleep — otherwise it is invisible to the idle timer regardless of how much work it's doing. Modern Standby (S0 Low Power Idle, the default sleep state on most modern Windows laptops) permits only "selected, managed" background activity to continue; ordinary desktop processes (their own example list: browsers, downloaders, cloud-sync tools) get suspended.
  implication: This is the most direct, mechanistic explanation matching the reported pattern ("only reproduces during long unattended sessions", i.e. wall-clock idle with no HID input, regardless of background compute load) — a long agent session with zero keyboard/mouse activity is exactly the condition Windows' sleep timer is designed to act on, and nothing in this stack prevents it.

- timestamp: 2026-08-21
  checked: WebSearch — "Windows sleep wake WSL2 Docker Desktop containers restart after resume", "Docker Desktop WSL2 bind mount lost after resume tmpfs fallback bug kind"
  found: Multiple independent, unrelated GitHub issues document WSL2/Docker-Desktop failing to recover cleanly from host sleep/wake: microsoft/WSL#14005 ("WSL2 enters unrecoverable zombie state after sleep/wake — vsock communication failure"), docker/for-win#12981 ("Docker Desktop 'seems stuck' on computer wake up"), Docker community forum reports of containers losing network connectivity after sleep, and separately (independent of sleep specifically) multiple documented cases of Docker Desktop WSL2 bind mounts falling back to a broken/empty state after a Docker/WSL restart (docker/for-win#7905, #12654, #10422, #13947; rancher-sandbox/rancher-desktop#2231) — including one report explicitly describing mounts appearing as tmpfs instead of the real bind, matching this project's exact symptom.
  implication: Both halves of the causal chain (host sleep breaking WSL2/Docker Desktop's recovery, AND Docker-Desktop-WSL2 bind mounts specifically falling back to tmpfs after a VM-level restart) are independently, broadly documented failure classes — not a project-specific misconfiguration. This corroborates the two-stage mechanism (sleep triggers VM restart; VM restart breaks the bind mount) rather than requiring a single exotic cause.

- timestamp: 2026-08-21
  checked: Live re-verification on this host: `docker ps` (all 3 kind node containers) + `docker exec <node> mount | grep dags` on all 3 nodes
  found: All 3 nodes currently show `/dev/sde on /mnt/dags type ext4 (ro,relatime,...)` — healthy, real bind mount (cluster was manually recovered earlier this session per STATE.md).
  implication: Confirms the current live cluster is a safe, healthy baseline to validate `doctor-live.sh`'s positive-control (healthy) detection path against, without needing to fabricate a broken state on the real cluster.

## Resolution

root_cause: |
  Best-supported explanation (not proven with absolute certainty — see
  reasoning_checkpoint.blind_spots): during the long (~40min), keyboard/
  mouse-idle background Claude Code agent session, Windows' idle-sleep timer
  — which counts ONLY keyboard/mouse input, never background process
  activity, and which nothing in this stack calls SetThreadExecutionState to
  suppress — put the host to sleep (most likely Modern Standby, the default
  sleep state on most modern Windows laptops). WSL2/Docker Desktop's VM did
  not cleanly resume from that sleep transition (a broadly documented,
  independently-reported failure class: microsoft/WSL#14005, docker/for-
  win#12981, and multiple Docker community forum reports), resulting in a
  fresh VM restart (explaining the observed ~6-minute kernel uptime against
  8-day-old containers). On that restart, Docker Desktop's WSL2 backend
  failed to reattach the kind nodes' `/mnt/dags` hostPath bind mount and fell
  back to an empty, read-only tmpfs on all 3 nodes — also an independently
  documented Docker-Desktop-WSL2 bind-mount failure class (docker/for-
  win#7905, #12654, #10422, #13947), and the identical symptom already
  root-caused (but not previously trigger-investigated) in the two prior
  incidents on this host (2026-08-14, 2026-08-16). The empty DAGs mount
  causes the dag-processor to discover zero files, `DagModel.is_stale` never
  clears, and the scheduler's `get_running_dag_runs_to_examine()` query
  silently excludes every DagRun cluster-wide with no exception logged —
  fully explaining the "freezing with zero exceptions" symptom.

  Two other mechanisms were researched and are documented as lower-confidence
  contributing/alternative factors, not eliminated with full certainty: WSL2's
  own `vmIdleTimeout` (default 60s, but actively counteracted by Docker
  Desktop's `wslkeepalive` process while Docker Desktop is running, and
  independently reported as unreliable even when explicitly configured), and
  Docker Desktop's Resource Saver mode (documented trigger is "zero running
  containers," which does not match this host's state — 3 kind node
  containers ran continuously — though Resource Saver has its own separate
  history of causing WSL lock/freeze bugs unrelated to its documented
  trigger).

fix: |
  This is a host-environment reliability issue, not a code bug — the
  actionable fix is primarily a Windows-side configuration change this
  repository cannot apply from inside WSL, per goal=find_and_fix's allowance
  for documenting the exact recommended change as the deliverable. Applied
  in-repo, as defense-in-depth and to close the "nobody notices until Airflow
  visibly stalls" operational gap:

    1. `docs/wsl/wslconfig.example`: added `vmIdleTimeout=-1` under `[wsl2]`,
       disabling WSL2's own idle-VM teardown outright. Zero cost, addresses
       one contributing mechanism even though it is not proven to be the
       primary cause. Like every other setting in this file, it requires the
       user to copy it to `C:\Users\<you>\.wslconfig` and run `wsl --shutdown`
       from Windows — this repo has already documented that this is a
       deliberate human act it cannot perform.

    2. New `scripts/doctor-live.sh` + `make doctor-live` / `make
       doctor-live-check` Makefile targets: detects the tmpfs-fallback
       condition on an ALREADY-RUNNING cluster (unlike `make doctor`, which
       only runs pre-flight before `cluster-up`) by `docker exec`-ing into
       each kind node container and checking `/mnt/dags`'s live mount type,
       bypassing Kubernetes entirely — the same diagnostic method both prior
       incidents used manually. `make doctor-live` self-heals by `docker
       restart`ing only the affected node container(s) — the same
       remediation both prior incidents applied by hand, now automated.
       `make doctor-live-check` runs detection only (no restart), for
       monitoring/reporting use.

  RECOMMENDED WINDOWS-SIDE ACTIONS (cannot be applied from inside WSL — for
  the user to apply manually, whichever combination fits their workflow):

    a. Prevent Windows from sleeping during long unattended sessions: Windows
       Settings -> System -> Power & battery -> Screen and sleep -> set
       "When plugged in, put my device to sleep after" to Never (keep the
       screen-off timeout short if desired — only the SLEEP timeout needs to
       change, since only sleep breaks the WSL2 VM, not the display turning
       off). This is the single highest-confidence fix given the research
       above, since it removes the root trigger entirely rather than
       mitigating its downstream symptom.
    b. Alternatively/additionally, disable Modern Standby in favor of
       traditional S3 sleep (registry change, documented at
       https://winbuzzer.com/2025/11/17/how-to-disable-modern-standby-in-windows-11-and-windows-10-xcxwbt/)
       if (a) is not viable for battery-life reasons — traditional sleep is
       reported to interact more predictably with background VMs than Modern
       Standby, though this was not independently verified against WSL2
       specifically in this session's research.
    c. In Docker Desktop settings, disable "Resource Saver" (Settings ->
       General, or Settings -> Resources depending on Docker Desktop
       version) — low-confidence as the direct cause here, but free to
       disable and removes one more variable given its own independent
       history of WSL-lock bugs.
    d. Apply `vmIdleTimeout=-1` from item 1 above (already added to
       docs/wsl/wslconfig.example — requires the user's own `wsl --shutdown`
       to take effect).

  None of (a)-(d) can be verified as THE single fix with certainty (see
  root_cause's caveat) — they are ordered by confidence, and (a) is the
  recommended first action since it is the only one that removes the
  research-identified root trigger (idle-input-only sleep) rather than a
  secondary/contributing mechanism.

verification: |
  Self-verified this session:
    - `make doctor-live-check` run against the real, currently-healthy
      3-node cluster: all 3 nodes report `healthy (/mnt/dags is a real ext4
      bind mount)`, exit 0.
    - `tests/e2e/cluster/test_doctor_live_mount_detection.py` (4 tests, all
      passing): positive control against the real cluster; broken-tmpfs
      detection (fake docker, DOCTOR_LIVE_REPAIR=false) correctly reports
      failure and does NOT invoke `docker restart`; self-heal repair mode
      (fake docker) correctly detects tmpfs, invokes `docker restart`, then
      re-verifies and reports healthy, exit 0; unreadable/unexpected `mount`
      output is correctly classified as an advisory (never a false
      "healthy"), non-fatal to the overall run.
    - `ruff check` / `ruff format --check` / `mypy` all pass clean on the new
      test file. `scripts/doctor-live.sh` was NOT run through shellcheck
      (not installed in this environment, and no shellcheck gate exists in
      this repo's CI) — reviewed manually against `scripts/doctor.sh`'s
      established conventions instead.
    - Live cluster re-confirmed healthy and untouched immediately after all
      test runs (`make doctor-live-check` again showed all 3 nodes healthy).
  NOT verified (honest limit, per the orchestrator's own stated constraint):
    - The self-heal repair path has NOT been exercised against an actual
      live tmpfs-fallback mount on this cluster (only against a fake-docker
      simulation) — full end-to-end verification through a real broken state
      was explicitly deemed infeasible this session (would require
      deliberately breaking the DAGs mount the user is actively using).
    - The recommended Windows-side power-plan changes (fix items a-d) have
      NOT been applied or verified by the user yet — this requires human
      action outside this session's reach. Human verification is requested
      below.

files_changed:
  - docs/wsl/wslconfig.example
  - scripts/doctor-live.sh
  - Makefile
  - tests/e2e/cluster/test_doctor_live_mount_detection.py
