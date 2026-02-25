# Comparison: Signals vs Sales-Research (Qual)

## What They Are

**Signals** is a headless Python pipeline that runs on a cron. Given a list of companies, it crawls job boards, RSS feeds, community forums, and tech profiles to collect "buying signals," then scores accounts with a weighted formula and exports review queues as CSV.

**Qual (sales-research)** is a Tauri desktop app (Rust backend + React frontend). Given a list of companies, it spawns Claude CLI processes to research each one via live web browsing, then scores them against a user-defined ICP and generates conversation starters for key contacts.

Both answer the same question: *"Which companies should we talk to, and why?"*

---

## Architectural Comparison

| Dimension | Signals | Qual |
|-----------|---------|------|
| **Runtime** | Headless Python CLI (Typer), cron-driven | Desktop app (Tauri 2), user-driven |
| **Language** | Python 3.12 | Rust + TypeScript (React 19) |
| **Database** | PostgreSQL (via hand-rolled SQLite→PG translation layer) | SQLite (local, WAL mode) |
| **Research Engine** | Custom HTTP crawlers per source type | Claude CLI subprocesses doing live web research |
| **Scoring** | Deterministic: weight × confidence × source_reliability × recency_decay | LLM-evaluated: Claude reads research + ICP config and outputs a score JSON |
| **Concurrency** | Synchronous `requests` + thread locks | Async Tokio + semaphore-limited job queue (5 concurrent) |
| **Config** | 12+ CSV files (signal_registry, keyword_lexicon, thresholds, etc.) | Database-stored prompts + JSON scoring config in UI |
| **Output** | CSV exports, Google Sheets sync, webhook-driven discovery | In-app markdown profiles, contact lists, score cards |
| **Tests** | 59 tests (integration with Postgres) | Zero tests |
| **Lines of Code** | ~12K Python | ~5.7K Rust + ~10.5K TypeScript |

---

## What Signals Does Better

### 1. Deterministic, Auditable Scoring
Signals' scoring engine is a pure function: `weight × confidence × source_reliability × recency_decay`. Every score can be decomposed into exactly which signals contributed what. The `top_reasons_json` field traces back to specific evidence URLs and text. You can explain to a human *exactly* why Company X scored 73.

Qual's scoring delegates entirely to Claude. The score is whatever Claude says it is. If Claude has a bad day, or the prompt drifts, or the model version changes, scores shift unpredictably. There's no recency decay, no source weighting, no reproducibility guarantee.

### 2. Continuous Signal Accumulation
Signals runs daily, accumulating observations over time. A company that posted 3 DevOps roles last month and just appeared on a Kubernetes forum gets scored higher than one that showed up once. The recency decay model means old signals fade naturally. This temporal dimension is critical for buying intent — it's the difference between "they posted a job" and "they've been ramping infrastructure spend for 6 weeks."

Qual is a point-in-time snapshot. Claude researches a company once, writes a profile, and that's it until someone manually re-runs research. There's no signal accumulation, no trend detection, no delta tracking.

### 3. Scale and Automation
Signals handles hundreds of accounts autonomously: crawl → collect → score → export → sync to sheets — all on a cron with advisory locks, retry queues, and quarantine for persistent failures. It's operationally mature for unattended execution.

Qual is inherently manual — someone clicks "Research" for each company. The 5-job concurrency limit and 10-minute timeout per job mean ~30 companies/hour throughput at best. It's a power tool for an SDR, not a pipeline.

### 4. Multi-Source Triangulation
Signals cross-references 5+ source types (job boards, RSS, technographics, community, first-party) and weights by source reliability. A signal from a verified job posting carries more weight than a Reddit mention. This triangulation reduces false positives.

Qual relies on whatever Claude finds during a single browsing session. There's no source reliability model, no multi-source corroboration, no deduplication across research runs.

### 5. Discovery Pipeline
Signals has an entire discovery subsystem that finds *new* companies — not just scores known ones. The watchlist builder queries Wikidata for CPG companies globally, the hunt pipeline proactively crawls documents for mentions, and discovered accounts get promoted through a policy-driven funnel.

Qual only works with companies you already know about.

---

## What Qual Does Better

### 1. Research Depth and Flexibility
This is Qual's killer advantage. Claude CLI with web browsing can discover things no keyword lexicon will ever capture: the nuance of a company's quarterly earnings call, a CTO's LinkedIn post about migration pain, a blog announcing a strategy shift. The research is qualitative, contextual, and human-readable.

Signals is limited to what its keyword lexicons can match against structured data sources. It finds "this company posted a Kubernetes role" but can't understand *why* — it has no way to read between the lines.

### 2. Person-Level Intelligence
Qual researches individual people: their roles, LinkedIn profiles, management level, year joined. It generates personalized conversation topics per contact. This is directly actionable for outbound sales — an SDR can use this output immediately.

Signals is purely account-level. It knows Company X has a high score but gives you nothing about who to contact or what to say.

### 3. User Experience
Qual has a real UI — streaming research output, score breakdowns, lead status tracking, keyboard shortcuts, system tray integration. A non-technical sales person can use it. The real-time streaming of Claude's thinking process builds trust in the output.

Signals outputs CSVs and syncs to Google Sheets. It's built for a data engineer, not a sales rep.

### 4. Customizable Prompts and ICP Definition
Qual lets users define their own ICP through a GUI: required characteristics (pass/fail gates) and demand signifiers (weighted factors). The scoring prompt is dynamically assembled from this config. Users can also customize the research prompts for company and person research.

Signals' scoring is configured through CSV files with numeric weights. More powerful for a technical operator, but less accessible.

### 5. Job Queue Engineering
The Rust job queue in Qual is genuinely well-engineered: RAII `JobGuard` for panic-safe cleanup, semaphore-based concurrency, graceful SIGTERM→SIGKILL shutdown, stream processing with batched log persistence, atomic completion handler with phase-based recovery. This is production-grade process management.

Signals' `_run_with_watchdog` is a basic timeout wrapper that checks elapsed time *after* the function returns — it can't actually kill a hung stage.

---

## Where They're Both Weak

### 1. No Tests (Qual) / Insufficient Tests (Signals)
Qual has literally zero tests. Signals has 59, but they miss all collectors, most of discovery, and the scoring engine. Neither project can refactor safely.

### 2. No Structured Logging
Signals uses no logging module at all. Qual uses `eprintln!()` throughout — better than nothing, but not structured or filterable.

### 3. No LLM Integration in Signals / No Deterministic Scoring in Qual
Each project has exactly the piece the other one needs. Signals needs LLM-powered qualitative research. Qual needs deterministic, reproducible scoring with signal accumulation.

---

## The Real Insight: They're Complementary

These aren't competing approaches — they're two halves of the same system:

```
┌─────────────────────────────────────────────────────┐
│                  Combined Pipeline                    │
│                                                       │
│  1. DISCOVER (Signals)                               │
│     Watchlist builder + hunt pipeline                │
│     → Find companies worth researching               │
│                                                       │
│  2. COLLECT SIGNALS (Signals)                        │
│     Job boards, RSS, technographics, community       │
│     → Structured, deterministic buying signals       │
│                                                       │
│  3. SCORE & PRIORITIZE (Signals)                     │
│     Weighted formula with recency decay              │
│     → Rank-ordered list with explainable scores      │
│                                                       │
│  4. DEEP RESEARCH (Qual)                             │
│     Claude CLI researches top-scoring accounts       │
│     → Rich company profiles, contact discovery       │
│                                                       │
│  5. QUALIFY & PERSONALIZE (Qual)                     │
│     ICP evaluation + conversation generation         │
│     → Sales-ready output per contact                 │
│                                                       │
│  6. FEEDBACK LOOP                                    │
│     Review labels (approved/rejected) from Qual      │
│     → Feed back into Signals' precision metrics      │
│     → Calibrate scoring thresholds over time         │
└─────────────────────────────────────────────────────┘
```

Signals is the *funnel top* — broad, automated, deterministic. It finds 1,000 companies showing buying intent and ranks them.

Qual is the *funnel bottom* — deep, interactive, qualitative. It takes the top 50 from Signals and turns them into actionable sales intelligence.

The missing piece is the bridge between them: an integration where Signals' top-scoring accounts automatically flow into Qual for deep research, and Qual's review decisions feed back into Signals' calibration.

---

## Summary Verdict

| Aspect | Winner | Why |
|--------|--------|-----|
| Scale | Signals | Hundreds of accounts, unattended, daily |
| Research quality | Qual | Claude + web browsing beats keyword matching |
| Scoring rigor | Signals | Deterministic, reproducible, explainable |
| Person intelligence | Qual | Contact discovery, conversation generation |
| Operational maturity | Signals | Retry queues, advisory locks, precision tracking |
| Engineering quality | Tie | Signals has more code debt; Qual has zero tests |
| UX | Qual | Real UI vs. CSV exports |
| Extensibility | Signals | CSV-driven config, pluggable collectors |

**Bottom line:** Signals is the better *system*. Qual is the better *tool*. The ideal product combines both: Signals' automated pipeline feeding Qual's deep research, with a shared scoring model that blends deterministic signals with LLM evaluation.
