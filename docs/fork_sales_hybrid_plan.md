# Hybrid Fork Plan: Managed `sales` Fork + `signals` Core Engine

## Summary

Yes, we should fork `sales`, but only as the operator experience and job-control shell.
The canonical signal ingestion/scoring/output pipeline remains in `/Users/raramuri/Projects/zopdev/signals`.
The forked app at `/Users/raramuri/Projects/zopdev/sales-research/sales` becomes the desktop control plane for running and monitoring `signals` in real time, with strict output compatibility.

## Scope

1. Build a managed fork of `sales` for desktop-local operations and live run visibility.
2. Integrate forked `sales` with `signals` via stable run/event contracts.
3. Keep existing `signals` CSV/DB outputs and filenames unchanged.
4. Add non-breaking telemetry/evidence artifacts to improve trust and explainability.
5. Do not replace the `signals` scoring/collector engine with prompt-only research.

## Non-Goals

1. No breaking schema change to existing main outputs (`daily_scores_*`, `review_queue_*`, `promotion_readiness_*`, `icp_coverage_*`, `ops_metrics_*`).
2. No immediate cloud migration in this phase (desktop-local orchestration chosen).
3. No full rewrite of `signals` into Rust/Tauri.

## Target Architecture

1. `signals` remains the execution engine and source-of-truth:
   `/Users/raramuri/Projects/zopdev/signals/src/main.py`, `/Users/raramuri/Projects/zopdev/signals/src/collectors/*`, `/Users/raramuri/Projects/zopdev/signals/src/scoring/*`.
2. Forked `sales` becomes orchestration + UX:
   `/Users/raramuri/Projects/zopdev/sales-research/sales/src-tauri/src/jobs/*`, `/Users/raramuri/Projects/zopdev/sales-research/sales/src/components/stream-panel/*`.
3. Integration boundary is CLI + structured stream events:
   `sales` starts `signals` runs, parses NDJSON progress, and displays run/account/signal state live.
4. Compatibility layer reads existing outputs from `signals/output` and `signals` Postgres without changing existing downstream consumers.

## Implementation Plan

### Phase 0: Fork Governance and Repo Topology

1. Create org-managed fork of upstream `chaitanyya/sales`.
2. Add remotes and policy:
   `origin=zopdev fork`, `upstream=chaitanyya/sales`.
3. Define sync cadence and process:
   monthly upstream sync branch, selective cherry-picks into `main`.
4. Add `FORK_NOTES.md` documenting divergences and migration rationale.

### Phase 1: Stable Run Control Contract in `signals`

1. Add a new machine-oriented command path in `/Users/raramuri/Projects/zopdev/signals/src/main.py`:
   `run-ui` (or `run --stream-json`).
2. Emit structured NDJSON events for lifecycle and progress, not only human text.
3. Ensure event coverage:
   run started, stage started/completed, account started/completed, signal observed, run completed/failed.
4. Add run status query command for reconnect/recovery:
   `status --run-id`.
5. Preserve current human-readable commands unchanged.

### Phase 2: `sales` Backend Adapter to `signals`

1. In `/Users/raramuri/Projects/zopdev/sales-research/sales/src-tauri/src/jobs/queue.rs`, add a new job mode `signals_run`.
2. Replace direct Claude-only company research path for bulk operations with subprocess execution of `signals`.
3. Parse NDJSON from `signals` and map to existing stream events shown in stream panel.
4. Store job/run linkage in `sales` DB:
   `job_id` ↔ `signals_run_id` for restart/recovery.
5. Keep single-company "deep research" action available as separate mode for qualitative drill-down.

### Phase 3: Real-Time Operator UX

1. Add "Run Monitor" page in forked `sales` with:
   overall run progress, stage status, active company count, completed count, failed/skipped count, ETA.
2. Add per-company timeline:
   picked up, source crawls attempted, signals found, score computed, tier decision.
3. Add explicit completion semantics:
   terminal state badge, duration, output file links.
4. Add glossary/term descriptions inline for all major terms and metrics (source, signal, confidence, reliability, tier, readiness).

### Phase 4: Evidence and Trust Layer

1. Add non-breaking output artifact in `signals`:
   `evidence_trace_YYYYMMDD.csv` with citation-grade rows.
2. Add columns in evidence trace:
   `company`, `signal_code`, `source`, `evidence_url`, `evidence_text`, `observed_at`, `confidence`, `source_reliability`, `score_contribution`.
3. In `sales`, add "Why this score" panel using evidence trace + existing observations.
4. Keep existing main CSVs unchanged; evidence trace is additive.

### Phase 5: Throughput and Parallelism for Desktop-Local

1. Replace fixed queue cap behavior in forked `sales` for `signals_run` mode with configurable execution profile.
2. Add desktop-local profile settings:
   `max_parallel_accounts`, `workers_per_source`, `http_timeout`, `request_interval_ms`.
3. Surface backpressure in UI:
   queue depth, per-source slowdown, stuck detector.
4. Add watchdog controls:
   abort, retry failed subset, rerun only missing accounts.

### Phase 6: Parity Cutover

1. Run side-by-side "old flow vs forked UI-driven flow" for 7 consecutive runs.
2. Compare key metrics and outputs daily.
3. Cut over only when parity gates are met.
4. Keep rollback path:
   direct CLI run in `/Users/raramuri/Projects/zopdev/signals` remains always available.

## Public APIs / Interfaces / Types (Planned Additions)

| Area | Change | Compatibility |
|---|---|---|
| `signals` CLI | Add machine-run command (`run-ui` or `run --stream-json`) | Additive |
| `signals` CLI | Add `status --run-id` | Additive |
| Stream schema | Introduce `SignalsRunEvent` NDJSON contract | New additive contract |
| `signals` outputs | Add `evidence_trace_YYYYMMDD.csv` | Additive |
| `sales` job types | Add `signals_run` job type in Rust queue/type enums | Additive |
| `sales` DB | Add `signals_run_id` and run-state metadata in jobs table | Additive migration |

### `SignalsRunEvent` (Decision-Complete Draft)

1. `event_type` (`run_started|stage_started|stage_completed|account_started|account_completed|signal_observed|run_completed|run_failed`)
2. `run_id`
3. `timestamp`
4. `stage` (nullable)
5. `account_id` (nullable)
6. `domain` (nullable)
7. `signal_code` (nullable)
8. `source` (nullable)
9. `message`
10. `metrics` (object, optional)

## Data Contract Mapping (Decision Complete)

1. Canonical watchlist/account inputs remain in:
   `/Users/raramuri/Projects/zopdev/signals/config/watchlist_accounts.csv` and `/Users/raramuri/Projects/zopdev/signals/config/account_source_handles.csv`.
2. Canonical scoring outputs remain in:
   `/Users/raramuri/Projects/zopdev/signals/output`.
3. Forked `sales` reads existing outputs and Postgres views; it does not re-score independently for production decisions.
4. `sales` scoring UI remains for exploratory tuning and can generate config suggestions, but production scoring stays in `signals`.

## Test Cases and Scenarios

### Contract and Parser Tests

1. NDJSON event schema validation with all event types.
2. Backward compatibility test: old CLI behavior unchanged.
3. Stream reconnection test: UI resumes from `status --run-id`.

### Functional Tests

1. Full run with 1000 companies, verify all accounted for in monitor counters.
2. Failure injection test: network errors and per-source retries surface correctly in UI.
3. Partial rerun test: rerun only failed/skipped accounts.
4. Single-company deep-research drill-down test with citation rendering.

### Output Compatibility Tests

1. File presence and naming equality for main outputs versus current baseline.
2. Row-level parity checks on `daily_scores_*` and `review_queue_*` within tolerance.
3. Evidence coverage test:
   minimum citation coverage threshold on medium/high tier rows.

### Performance Tests (Desktop-Local)

1. Throughput benchmark at different `workers_per_source` settings.
2. Long-run stability test (continuous 6h operation).
3. Stuck-process detection and cleanup test in job queue.

## Rollout and Acceptance Gates

1. Gate A: 100% run lifecycle visibility in UI for stage/account progress.
2. Gate B: Main output compatibility preserved for 7 consecutive daily runs.
3. Gate C: Zero "silent hangs" (all jobs end in completed/failed/cancelled with reason).
4. Gate D: Evidence trace available for all medium/high rows.
5. Gate E: Operator can identify top failure cause by source within 60 seconds from UI.

## Risks and Mitigations

1. Risk: Fork drift from upstream `sales`.
   Mitigation: managed sync cadence and explicit merge policy.
2. Risk: Dual scoring confusion (`sales` vs `signals`).
   Mitigation: enforce `signals` as production source; label `sales` scoring as exploratory.
3. Risk: Desktop resource limits for 5k runs.
   Mitigation: configurable profiles, backpressure telemetry, failure slicing, future remote executor extension path.
4. Risk: Event contract brittleness.
   Mitigation: versioned schema and parser contract tests.

## Assumptions and Defaults (Locked)

1. Architecture: Hybrid (`signals` core + forked `sales` UX).
2. Orchestration: Desktop-local first.
3. Rollout: Phased parity rollout.
4. Compatibility: Strict compatibility for existing `signals` outputs.
5. Fork policy: Managed fork with upstream synchronization.
6. Default runtime command in UI: machine-run mode from `signals` with structured stream events.
7. Deep-research mode remains optional and does not override production scoring outputs.
