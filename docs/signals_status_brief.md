# Signals Platform — Status Brief

## What It Does
Automated GTM intelligence pipeline that scores 1000+ target accounts based on buying signals. Collects signals from job postings, news, website tech scans, CRM data, and community forums. Scores accounts on a 0-100 scale across 5 dimensions, then ranks them into tiers for sales prioritization.

---

## Current State (Working)

| Component | Status | Details |
|-----------|--------|---------|
| Signal Collection | Live | 5 active sources collecting 300+ signals |
| Scoring Engine | Live | 5-dimension weighted scoring, 4-tier classification |
| Web Dashboard | Live | Account list, scores, signals, dimensions, contacts, labels |
| Research (LLM) | Live | Claude-powered company briefs + conversation starters |
| CSV Export | Live | Sales-ready CSV with scores, tiers, top reasons |

### Active Signal Sources
| Source | Type | What It Captures |
|--------|------|-----------------|
| First Party CSV | CRM/Internal | POC progression, compliance initiatives, vendor consolidation, cost mandates |
| Serper (Google Search) | Jobs + News | DevOps/Platform/FinOps job postings, relevant company news |
| Website Techscan | Free crawl | Kubernetes, Terraform, GitOps, monitoring tool sprawl detection |
| Greenhouse/Lever | Job scraping | Authentic career page job listings |
| Reddit RSS | Community | DevOps discussions, tool complaints, intent phrases |

### Scoring Dimensions
| Dimension | Weight | What It Measures |
|-----------|--------|-----------------|
| Trigger Intent | 35% | Compliance, cost mandates, migration signals |
| Engagement/PQL | 25% | POC progression, repo deploys, audit views |
| Tech Fit | 20% | Kubernetes, Terraform, GitOps, tool sprawl |
| Firmographic | 10% | Employee count, growth, funding stage |
| Hiring/Growth | 10% | DevOps, Platform, FinOps hiring |

---

## Current Problems

### 1. Signal Volume is Low
- Only **323 authentic signals** across 1000+ accounts
- Most accounts have 0 signals — scoring depends heavily on the 74 hand-curated CRM entries
- No real-time signal detection — batch pipeline runs daily

### 2. Limited External Signal Coverage
- **No social media signals** — Twitter/X collector not built yet
- **No intent data** — don't know which companies are actively researching DevOps/cloud topics
- **No competitor intelligence** — don't know who's evaluating competitor products (G2, Gartner)
- Reddit coverage is RSS-only (limited to search results, no subreddit monitoring)

### 3. Firmographic Data is Static
- Employee count, funding, growth data is manually maintained in CSV
- No auto-enrichment from Clearbit/Apollo/Crunchbase for new accounts
- Data goes stale quickly

### 4. No Feedback Loop
- Sales team labels accounts (qualified/not qualified) but labels don't feed back into scoring weights
- No way to measure if high-scored accounts actually convert
- Signal weights are manually tuned, not data-driven

---

## What's Needed

### Phase 1 — Expand Signal Coverage (Dev Work)

| Task | Owner | Effort | Impact |
|------|-------|--------|--------|
| **Twitter/X collector** | Assigned (2 people) | 1-2 weeks | New social signal source |
| **Reddit API upgrade** | Assigned (2 people) | 1 week | Better community signals, subreddit monitoring |
| **Improve first_party CSV** | CRM team | Ongoing | More events: meetings booked, demos, support tickets |

### Phase 2 — Paid Data Integrations (Budget Required)

| Service | Monthly Cost | What It Adds | Priority |
|---------|-------------|-------------|----------|
| **Bombora** (Intent Data) | $500-2000 | Knows which companies are actively researching DevOps, cloud cost, platform engineering topics. Highest signal quality possible. | **P0 — Highest ROI** |
| **Apollo.io** (Contacts) | $49-99 | Verified contact emails and phone numbers for outreach. Integration partly built. | **P1** |
| **Clearbit** (Firmographics) | $500+ | Auto-enriched employee count, revenue, tech stack for all accounts. Replaces manual CSV. | **P2** |
| **G2 Intent** (Reviews) | Custom | Signals when target accounts read G2 reviews for competitors. | **P2** |

### Phase 3 — Scoring Intelligence

| Task | Effort | Impact |
|------|--------|--------|
| Feedback loop from sales labels → scoring weight adjustment | 2-3 weeks | Data-driven signal weights instead of manual tuning |
| A/B testing framework for signal weights | 2 weeks | Measure which signals predict conversion |
| Real-time alerting on score changes | 1 week | Notify sales when account score jumps |

---

## Architecture (For Reference)

```
Collectors (Jobs, News, Techscan, CRM, Reddit, Twitter*)
    ↓
PostgreSQL (signal_observations table — 300+ signals)
    ↓
Scoring Engine (recency decay × signal weight × source reliability)
    ↓
Account Scores (5 dimensions → aggregate score → tier 1-4)
    ↓
Research (LLM company briefs) → Dashboard → CSV Export
```

**Stack:** Python, PostgreSQL, FastAPI, Serper API, Claude API
**Repo:** https://github.com/Riyami28/signals

---

*Generated: March 2026*
