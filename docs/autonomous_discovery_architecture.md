# Autonomous Discovery Architecture (Concurrent + Continuous)

## What Changes at 1,000+ Accounts
A single long batch query is not enough. The system must switch to:
- concurrent source ingestion
- per-source rate limiting
- incremental scoring
- continuous discovery of net-new companies

## Target Operating Model
Run three always-on loops in parallel:
1. Ingestion Loop (minutes): collect and normalize events from all sources.
2. Scoring Loop (hourly): update account scores incrementally as new events arrive.
3. Discovery Loop (daily/weekly): mine new companies and promote qualified candidates.

## Component Architecture
1. Connector layer
- Huginn agents for RSS/web/news/feed connectors and scheduled polling.
- Provider connectors for social/transcript APIs where Huginn is weak.

2. Ingestion API
- FastAPI webhook endpoint (`/v1/discovery/events`) with auth and idempotency.
- Validates payloads, rejects placeholder domains, stores raw event envelope.

3. Queue and workers
- Redis queue (or RabbitMQ) with source-specific worker pools.
- Workers perform: normalize -> dedupe -> signal map -> persist observations.
- Each source has its own concurrency/rate budget.

4. Storage
- PostgreSQL for events, observations, runs, candidates, audit trails.
- Object storage for raw payload archives (optional but recommended).
- Optional analytics store (ClickHouse/BigQuery) for long-horizon analysis.

5. Scoring and promotion service
- Applies weighted signal scoring + fixed thresholds (`high>=20`, `medium>=10`).
- Enforces gates (primary signal gate, POC progression first-party gate).
- Publishes `crm_candidates` queue.

6. Discovery service
- Expands candidate universe from incoming events + watchlists.
- Resolves entity/domain/company identity.
- Ranks candidates by ICP buying-pattern bundles.

## Concurrency Policy (Practical Defaults)
Per-source worker pools and budgets:
- `news/rss`: 24 workers, 3 req/sec/source, timeout 20s
- `company websites/blogs`: 16 workers, 2 req/sec/domain, timeout 25s
- `jobs/careers`: 12 workers, 1 req/sec/domain, timeout 20s
- `reddit`: 8 workers, follow API quota, timeout 15s
- `social/provider APIs`: 4 workers/provider, strict token bucket by contract

Execution controls:
- token-bucket rate limiter per source
- exponential backoff + circuit breaker on repeated failures
- jittered scheduling to avoid burst sync
- idempotent event keys and dedupe windows

## Database Choice: SQLite vs Postgres
## SQLite is fine only if
- mostly single-writer batch
- low parallelism
- modest volume growth

## Move to PostgreSQL now if
- you want concurrent autonomous workers (yes)
- you will ingest social/news/provider streams continuously (yes)
- you need reliable queue consumers and multi-run history (yes)

Recommendation for your stated goal: **upgrade to PostgreSQL now**.

## Signals Universe and Scoring
Source of truth:
- `config/signal_universe_stackrank.csv`

Scoring framework:
- Tier A (decision/rollout/procurement) = highest weights
- Tier B (context/pressure signals) = medium weights
- Tier C (tech/noise) = low weights and cannot independently qualify medium/high

Current fixed tiers remain:
- `high >= 20`
- `medium >= 10`
- `low < 10`

## What Is Automatable vs Not
Automated-direct:
- company PR/news/RSS
- filings, earnings snippets
- jobs/technographics
- first-party CRM/CS event feeds

Automated-via-provider:
- LinkedIn signals (official/provider workflows; no scraping)
- paid transcript/social feeds

Partial-manual/proxy:
- procurement/legal stage details not publicly exposed
- closed-door executive intent

Proxy strategy when direct data missing:
- use budget language, compliance timelines, vendor-consolidation language,
- corroborate with two-source evidence before promotion.

## Key-People Tracking (Required)
Track named decision stakeholders per account:
- CIO/CTO/CDO
- VP Supply Chain / Ops
- CISO / Risk / Compliance
- Procurement lead / Transformation office

Signals from people:
- role changes, executive hires, public statements, conference commentary,
- procurement/legal/security stage mentions.

## Minimal Data Model Additions
Tables to add for scale:
- `raw_events`
- `normalized_events`
- `entity_resolution`
- `people`
- `company_people`
- `score_features`
- `score_history`
- `promotion_decisions`
- `source_run_metrics`

## Scheduling and Cadence
- Every 15 minutes: connector fetch + ingestion workers.
- Hourly: incremental scoring refresh.
- Daily: full discovery ranking + candidate export.
- Weekly: calibration review + weight/keyword tuning.

## SLOs and KPIs
- ingestion lag < 15 minutes for active sources
- dedupe precision > 99%
- customer medium+ coverage >= 0.70
- POC medium+ coverage >= 0.50
- non-ICP medium+ rate <= 0.50
- candidate precision by band tracked weekly

## Build Sequence (Recommended)
1. Migrate DB from SQLite to PostgreSQL.
2. Add Redis queue and source-specific worker pools.
3. Move collectors to queue tasks with per-source budgets.
4. Enable hourly incremental scoring (not only daily full runs).
5. Add provider-backed social/people ingestion.
6. Launch reviewed autonomous discovery loop; tune monthly.
