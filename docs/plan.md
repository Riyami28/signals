# Build Plan: Unified Signals + LLM Research Pipeline

## How to Work This Plan

This document is the authoritative implementation guide. Claude should follow it story by story, in execution order.

**Before starting any story:**
1. Read this entire plan if not already done in this session
2. Read the files listed in the story's **Files** section
3. Run the existing test suite to confirm the baseline: `cd /Users/raramuri/Projects/zopdev/signals/.claude/worktrees/hungry-rosalind && python -m pytest tests/ -q`
4. Do only what the story says — do not refactor adjacent code or add features beyond the story's scope

**After completing any story:**
1. Run the test suite again and verify it passes
2. Check the story's **Acceptance criteria** — all must be true before the story is done
3. Move to the next story in execution order

**Key files to understand before starting:**
- `src/db.py` — PostgreSQL interface (currently has the SQLite compat layer being removed in 0.1)
- `src/settings.py` — settings loading (being replaced in 0.3)
- `src/main.py` — CLI entry points and pipeline orchestration
- `src/collectors/jobs.py` — job board collector (Greenhouse + Lever today)
- `src/scoring/engine.py` — deterministic scoring engine (not being changed)
- `config/signal_registry.csv` — signal codes, weights, half-lives
- `config/source_registry.csv` — sources and their reliability scores

---

## Goal

A single headless Python pipeline that:
1. Discovers and monitors companies for buying signals (existing capability)
2. Scores accounts with a deterministic, explainable formula (existing capability)
3. Deep-researches top accounts using Claude API — prose brief, contacts, enrichment data (new)
4. Outputs a single CSV per run that a sales person can open (or later sync to Sheets/Zoho)

The CSV is the product. Everything upstream exists to make that CSV useful.

---

## Target Output: `sales_ready_{YYYYMMDD}.csv`

**Rows:** All accounts scoring high or medium tier, sorted by `signal_score DESC`. One row per account (best product score used). Accounts without completed research still appear (with `research_status=skipped`).

**Columns:**

| Column | Source | Notes |
|--------|--------|-------|
| `company_name` | `accounts` table | |
| `domain` | `accounts` table | |
| `website` | enrichment_json | Full URL with https:// |
| `industry` | enrichment_json | |
| `sub_industry` | enrichment_json | |
| `country` | enrichment_json | ISO country name |
| `city` | enrichment_json | |
| `state` | enrichment_json | |
| `employees` | enrichment_json | Integer headcount |
| `employee_range` | enrichment_json | e.g. "201-500" |
| `revenue_range` | enrichment_json | e.g. "$10M-$50M" |
| `company_linkedin_url` | enrichment_json | |
| `signal_score` | `account_scores` | 0-100 float |
| `signal_tier` | `account_scores` | high / medium / low |
| `delta_7d` | `account_scores` | Score change vs 7 days ago, e.g. "+5.2" |
| `top_signals` | `account_scores.top_reasons_json` | Pipe-separated signal codes |
| `evidence_links` | `account_scores.top_reasons_json` | Pipe-separated URLs |
| `top_reason_1` | `account_scores.top_reasons_json` | Format: "{signal_code} via {source}" |
| `top_reason_2` | `account_scores.top_reasons_json` | |
| `top_reason_3` | `account_scores.top_reasons_json` | |
| `research_brief` | `company_research.research_brief` | 150-200 word prose company brief |
| `research_summary` | `company_research.research_profile` | Full markdown research profile |
| `key_contacts` | `contact_research` | "FirstName LastName (Title) — linkedin_url" one per line, max 5, sorted by seniority |
| `conversation_starters` | `company_research.research_profile` | 3-5 bullet points extracted from profile |
| `research_status` | `company_research.research_status` | completed / failed / skipped |
| `source_type` | `accounts` | seed / discovered |
| `first_seen_date` | `accounts.created_at` | ISO date |
| `last_signal_date` | `signal_observations` | MAX(observed_at) for account |

---

## Enrichment JSON Schema

The `enrichment_json` column in `company_research` stores this structure. Every field that Claude fills must include a `_confidence` sibling (0.0–1.0). Fields below 0.5 confidence are omitted from the CSV rather than included with bad data.

```json
{
  "website": "https://example.com",
  "website_confidence": 0.95,
  "industry": "Consumer Packaged Goods",
  "industry_confidence": 0.90,
  "sub_industry": "Food & Beverage",
  "sub_industry_confidence": 0.80,
  "employees": 450,
  "employees_confidence": 0.70,
  "employee_range": "201-500",
  "employee_range_confidence": 0.85,
  "revenue_range": "$50M-$100M",
  "revenue_range_confidence": 0.60,
  "company_linkedin_url": "https://linkedin.com/company/example",
  "company_linkedin_url_confidence": 0.95,
  "city": "Chicago",
  "city_confidence": 0.85,
  "state": "Illinois",
  "state_confidence": 0.85,
  "country": "United States",
  "country_confidence": 0.95,
  "tech_stack": ["AWS", "Kubernetes", "Terraform"],
  "tech_stack_confidence": 0.75
}
```

Pre-LLM waterfall enrichment (Story 2.5) fills fields first; Claude only fills the gaps.

---

## Execution Order

```
Epic 0 (Foundation)       ← do first, de-risks everything
  0.1  Remove SQLite compat layer         [HIGH RISK — mechanical but high touch]
  0.2  Add structured logging              [LOW RISK]
  0.3  Replace settings with pydantic-settings [LOW RISK]
  0.4  Extend jobs collector: Ashby + Workday  [LOW RISK]

Epic 1 (Schema)           ← small, unblocks Epic 2
  1.1  Add research tables to schema
  1.2  Add research CRUD functions

Epic 2 (LLM Research)     ← core new capability
  2.1  Claude API client
  2.2  Prompt templates (extraction + scoring, two files)
  2.3  Response parser
  2.4  Pre-LLM waterfall enrichment
  2.5  Research orchestrator

Epic 3 (CSV Export)       ← the deliverable
  3.1  Sales-ready CSV exporter
  3.2  Wire research + export into pipeline

Epic 4 (Tests)            ← write alongside Epic 2-3
  4.1  Parser tests
  4.2  Prompt builder tests
  4.3  Export tests
  4.4  Orchestrator tests

Epic 5 (Validation)       ← last; requires human review
  5.1  Dry run with real accounts
  5.2  Prompt tuning
```

Stories within each epic are sequential. Tests (Epic 4) should be written alongside the corresponding Epic 2-3 story, not all at the end.

---

## Epic 0: Foundation Cleanup

*Make the existing codebase safe to build on. No new features — just remove landmines.*

---

### Story 0.1: Remove SQLite compatibility layer

**Why first:** Every new query written for Epics 1-3 would otherwise go through a regex translator. Writing 15+ new queries in fake-SQLite-dialect is not acceptable.

**What to read first:**
- `src/db.py` in full — understand the compat layer before deleting it
- `src/db.py` lines containing `_rewrite_sql_for_postgres`, `PostgresCompatConnection`, `PostgresCompatCursor`

**What to do:**

1. **Replace all `?` placeholders with `%s`** in every SQL string in `db.py` (search: `?` within quoted SQL strings)

2. **Replace SQLite-specific SQL constructs:**
   - `INSERT OR IGNORE INTO` → `INSERT INTO ... ON CONFLICT DO NOTHING`
   - `datetime('now')` → `CURRENT_TIMESTAMP`
   - `date(x, '-7 day')` → `(x::date - INTERVAL '7 days')`
   - `date(x, '+N day')` → `(x::date + INTERVAL 'N days')`
   - `INTEGER PRIMARY KEY AUTOINCREMENT` → `BIGSERIAL PRIMARY KEY` (schema DDL only)
   - `COLLATE NOCASE` → remove entirely (Postgres text comparison is case-sensitive by default; the callers use lowercased values anyway)

3. **Delete the compat classes and helpers:**
   - Delete `_rewrite_sql_for_postgres()` function
   - Delete `_split_sql_statements()` function
   - Delete all `_RE_*` module-level compiled regex patterns
   - Delete `PostgresCompatConnection` class
   - Delete `PostgresCompatCursor` class

4. **Update `get_connection()`** to return a raw `psycopg` connection with `row_factory = psycopg.rows.dict_row` (so all results remain dict-accessible as they are today). Remove any wrapping in `PostgresCompatConnection`.

5. **Run tests:** `python -m pytest tests/ -q` — fix any failures before proceeding.

**Files:** `src/db.py`

**Acceptance criteria:**
- [ ] `grep -n "PostgresCompatConnection\|PostgresCompatCursor\|_rewrite_sql_for_postgres\|INSERT OR IGNORE\|datetime('now')\|COLLATE NOCASE" src/db.py` returns zero matches
- [ ] `python -m pytest tests/ -q` passes with same pass count as before the story
- [ ] `python -m python -c "from src.db import get_connection; print('ok')"` succeeds

---

### Story 0.2: Add structured logging

**Why:** The LLM research module makes API calls that can fail in subtle ways. Without logging, debugging production failures requires re-running the pipeline with print statements.

**What to read first:**
- `src/main.py` — see how it currently has no logging
- Any `except Exception: pass` patterns in `src/collectors/`

**What to do:**

1. **Create `src/logging_config.py`:**
```python
import logging
import sys

def configure_logging(level: str = "INFO") -> None:
    """Call once at process start from main.py."""
    fmt = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=fmt,
        stream=sys.stdout,
        force=True,
    )
```

2. **Add `logger = logging.getLogger(__name__)` near the top of each file** (after imports):
   - `src/db.py`
   - `src/main.py`
   - `src/http_client.py`
   - `src/collectors/jobs.py`
   - `src/collectors/news.py`
   - `src/collectors/community.py`
   - `src/collectors/technographics.py`
   - `src/discovery/config.py`
   - `src/discovery/watchlist_builder.py`

3. **Replace bare `except` swallowers** — search for `except Exception: pass` and `except Exception:\n\s*pass` and `except:` with nothing after. Replace with `logger.warning("…", exc_info=True)`. Add a meaningful message: what operation failed, which account/URL, what will be skipped.

4. **Call `configure_logging()` at startup** in `main.py` — in the Typer app callback or at the top of `main()`, before any other code runs.

5. **Add key lifecycle log lines** (use `logger.info`):
   - Collector start: `logger.info("collector=%s accounts=%d", name, len(accounts))`
   - Collector end: `logger.info("collector=%s observations=%d errors=%d", name, ok, err)`
   - Scoring start/end with account count
   - Export start/end with row count

**Files:** New `src/logging_config.py`, `src/main.py`, `src/db.py`, `src/http_client.py`, `src/collectors/*.py`, `src/discovery/config.py`, `src/discovery/watchlist_builder.py`

**Acceptance criteria:**
- [ ] `grep -rn "except Exception: pass\|except:\s*$\|except Exception:\s*$" src/` returns zero matches (or only intentional ones with a comment)
- [ ] Running `python -m signals ingest --help` prints startup log lines to stdout
- [ ] `python -m pytest tests/ -q` still passes

---

### Story 0.3: Replace settings boilerplate with pydantic-settings

**Why:** We're adding 5+ new settings for Claude API. Each one currently costs 5 lines of try/except. Pydantic validates all settings at startup with clear error messages.

**What to read first:**
- `src/settings.py` in full — understand all existing fields before rewriting

**What to do:**

1. **Add dependencies** to `requirements.txt`:
   - `pydantic-settings>=2.0`
   - `pydantic>=2.0` (likely already present, verify)

2. **Rewrite `src/settings.py`** as a `BaseSettings` class. All existing field names must stay identical (they're used throughout the codebase). New fields to add:

```python
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- existing fields (keep all names identical) ---
    db_url: str
    out_dir: Path = Field(default=Path("output"))
    # ... (all existing fields)

    # --- new fields for LLM research ---
    claude_api_key: str = Field(default="")
    claude_model: str = Field(default="claude-sonnet-4-5")
    research_max_accounts: int = Field(default=20, ge=1, le=200)
    research_stale_days: int = Field(default=30, ge=1)
    research_timeout_seconds: int = Field(default=120, ge=10)

    # --- new fields for waterfall enrichment ---
    clearbit_api_key: str = Field(default="")
    hunter_api_key: str = Field(default="")
```

3. **Replace `load_settings()` calls** — check `src/main.py` and anywhere else `load_settings()` is called. Change to `Settings()` direct instantiation. The `Settings` object should be constructed once and passed through; it is already used this way.

4. **Delete `load_settings()` function** once all callers are updated.

**Files:** `src/settings.py`, `requirements.txt`, `src/main.py`

**Acceptance criteria:**
- [ ] `python -c "from src.settings import Settings; s = Settings(); print(s.out_dir)"` works
- [ ] If `DB_URL` env var is unset, startup raises a clear `ValidationError` not a silent `None`
- [ ] `python -m pytest tests/ -q` passes

---

### Story 0.4: Extend jobs collector — Ashby and Workday

**Why:** The jobs collector currently covers Greenhouse and Lever. Ashby is the fastest-growing ATS in VC-backed startups. Workday covers enterprise. These are quick wins that improve signal coverage before we wire in LLM research.

**What to read first:**
- `src/collectors/jobs.py` in full — understand `_derive_slug_candidates()`, `_collect_greenhouse()`, `_collect_lever()`, and how they're called from `collect_jobs()`

**What to do:**

**Part A — Ashby:**

Ashby's job pages at `jobs.ashbyhq.com/{slug}` already embed `JobPosting` JSON-LD. The existing `_extract_job_titles_from_html()` already handles JSON-LD. You just need a fetcher:

```python
def _collect_ashby(domain: str, slug_candidates: list[str], http) -> list[str]:
    """Returns list of job title strings."""
    for slug in slug_candidates:
        url = f"https://jobs.ashbyhq.com/{slug}"
        try:
            html = http.get(url, timeout=10).text
            titles = _extract_job_titles_from_html(html)
            if titles:
                return titles
        except Exception:
            continue
    return []
```

Add `_collect_ashby()` to the same fallback chain in `collect_jobs()` after the existing Greenhouse and Lever attempts.

**Part B — Workday:**

Workday has an undocumented but stable JSON API endpoint:
`https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{board}/jobs`

The `tenant` is typically the company domain root (e.g., `unilever` for `unilever.wd1.myworkdayjobs.com`). The `board` is typically `{TenantName}_External_Career_Site`. Derive candidates with the same `_derive_slug_candidates()` pattern.

```python
def _collect_workday(domain: str, slug_candidates: list[str], http) -> list[str]:
    """Returns list of job title strings."""
    board_candidates = [f"{s}_External_Career_Site" for s in slug_candidates]
    for tenant in slug_candidates:
        for board in board_candidates:
            url = f"https://{tenant}.wd1.myworkdayjobs.com/wday/cxs/{board}/jobs"
            try:
                resp = http.get(url, timeout=10)
                if resp.status_code == 200:
                    data = resp.json()
                    # Response shape: {"jobPostings": [{"title": "...", ...}]}
                    postings = data.get("jobPostings") or data.get("jobs") or []
                    return [p.get("title", "") for p in postings if p.get("title")]
            except Exception:
                continue
    return []
```

**Registering new sources:** Add entries to `config/source_registry.csv`:
```
ashby_api,0.25,true
workday_api,0.25,true
```
(Same reliability as `lever_api` and `careers_live` — HTML-based collection is inherently noisy.)

**Files:** `src/collectors/jobs.py`, `config/source_registry.csv`

**Acceptance criteria:**
- [ ] `_collect_ashby()` and `_collect_workday()` functions exist in `jobs.py`
- [ ] Both are called from the main `collect_jobs()` fallback chain
- [ ] `ashby_api` and `workday_api` entries exist in `source_registry.csv`
- [ ] `python -m pytest tests/ -q` passes (existing tests don't break; no new integration tests required for these since they depend on live URLs)

---

## Epic 1: Database Schema for Research Data

*Extend Postgres schema to store LLM research outputs alongside signal data.*

---

### Story 1.1: Add research tables

**What to read first:**
- `src/db.py` — specifically `SCHEMA_SQL` constant and `_run_schema_migrations()` function — understand the pattern before adding to it

**What to do:**

Add the following to `SCHEMA_SQL` in `src/db.py` (inside the existing multi-line string, after the last existing `CREATE TABLE`):

```sql
CREATE TABLE IF NOT EXISTS company_research (
    account_id          TEXT PRIMARY KEY REFERENCES accounts(account_id),
    research_brief      TEXT,                        -- 150-200 word prose brief (intermediate artifact)
    research_profile    TEXT,                        -- full markdown company profile
    enrichment_json     TEXT NOT NULL DEFAULT '{}',  -- structured data with _confidence fields
    research_status     TEXT NOT NULL DEFAULT 'pending'
        CHECK (research_status IN ('pending', 'in_progress', 'completed', 'failed', 'skipped')),
    researched_at       TEXT,
    model_used          TEXT,
    prompt_hash         TEXT,                        -- hash of prompt template; re-research when changed
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS contact_research (
    contact_id          TEXT PRIMARY KEY,            -- stable_hash(account_id + linkedin_url or full_name)
    account_id          TEXT NOT NULL REFERENCES accounts(account_id),
    first_name          TEXT NOT NULL,
    last_name           TEXT NOT NULL,
    title               TEXT,
    email               TEXT,
    linkedin_url        TEXT,
    management_level    TEXT
        CHECK (management_level IN ('C-Level', 'VP', 'Director', 'Manager', 'IC', NULL)),
    year_joined         INTEGER,
    created_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_contact_research_account
    ON contact_research(account_id);

CREATE TABLE IF NOT EXISTS research_runs (
    research_run_id     TEXT PRIMARY KEY,
    run_date            TEXT NOT NULL,
    score_run_id        TEXT NOT NULL,
    accounts_attempted  INTEGER NOT NULL DEFAULT 0,
    accounts_completed  INTEGER NOT NULL DEFAULT 0,
    accounts_failed     INTEGER NOT NULL DEFAULT 0,
    accounts_skipped    INTEGER NOT NULL DEFAULT 0,
    started_at          TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at         TEXT,
    status              TEXT NOT NULL DEFAULT 'running'
        CHECK (status IN ('running', 'completed', 'failed'))
);
```

Call `_ensure_schema()` (or equivalent) to apply this against a live DB and verify the tables are created.

**Files:** `src/db.py`

**Acceptance criteria:**
- [ ] All three tables exist in the Postgres schema after running `_ensure_schema()`
- [ ] `research_status` constraint rejects values outside the allowed set
- [ ] `python -m pytest tests/ -q` passes

---

### Story 1.2: Add research CRUD functions

**What to read first:**
- `src/db.py` — existing CRUD patterns (e.g., `upsert_observation`, `get_account_scores`)
- Story 1.1 schema (above) — understand the columns before writing functions

**What to do:**

Add the following functions to `src/db.py`:

```python
def upsert_company_research(
    conn,
    account_id: str,
    *,
    research_brief: str | None = None,
    research_profile: str | None = None,
    enrichment_json: str = "{}",
    research_status: str,
    model_used: str | None = None,
    prompt_hash: str | None = None,
) -> None:
    """Insert or update a company research record."""
    # Use INSERT ... ON CONFLICT DO UPDATE
    # Always update updated_at = CURRENT_TIMESTAMP

def get_company_research(conn, account_id: str) -> dict | None:
    """Return the company_research row or None."""

def get_accounts_needing_research(
    conn,
    run_date: str,
    score_run_id: str,
    max_accounts: int,
    min_tier: str,         # "medium" means high+medium; "high" means high only
    stale_days: int,       # re-research if researched_at < NOW() - stale_days
    current_prompt_hash: str,  # re-research if prompt_hash differs
) -> list[dict]:
    """
    Returns accounts that:
    1. Have a current score at min_tier or above
    2. Have no completed research, OR
       have research older than stale_days, OR
       have a different prompt_hash than current_prompt_hash
    3. Limited to max_accounts rows
    4. Ordered by signal_score DESC (most promising first)

    Each dict includes: account_id, company_name, domain, signal_score, signal_tier,
    delta_7d, top_reasons_json (from account_scores).
    """

def mark_research_in_progress(conn, account_id: str) -> None:
    """Set research_status='in_progress' BEFORE making the API call.
    This prevents double-research if the pipeline is interrupted and restarted."""

def upsert_contacts(conn, account_id: str, contacts: list[dict]) -> None:
    """Delete all existing contacts for account, then insert new ones.
    Each contact dict must have: first_name, last_name, and at least one of: title, linkedin_url.
    contact_id = stable_hash(account_id + (linkedin_url or first_name+last_name))."""

def get_contacts_for_account(conn, account_id: str) -> list[dict]:
    """Return all contacts for an account, ordered by management_level seniority
    (C-Level first, IC last)."""

def create_research_run(conn, run_date: str, score_run_id: str) -> str:
    """Insert a new research_runs row with status='running'. Returns research_run_id."""

def finish_research_run(
    conn,
    research_run_id: str,
    status: str,
    accounts_attempted: int,
    accounts_completed: int,
    accounts_failed: int,
    accounts_skipped: int,
) -> None:
    """Update research_run with final counts and finished_at timestamp."""
```

**Important implementation note for `get_accounts_needing_research`:** The query must mark accounts as `in_progress` using `mark_research_in_progress` as a separate step — not in the SELECT. The orchestrator calls `get_accounts_needing_research` to get the list, then calls `mark_research_in_progress` for each account before the API call.

**Files:** `src/db.py`

**Acceptance criteria:**
- [ ] All 8 functions exist and are importable
- [ ] `upsert_company_research` uses `ON CONFLICT DO UPDATE` (not delete-and-insert)
- [ ] `upsert_contacts` uses delete-then-insert (contacts are always replaced as a set)
- [ ] `get_accounts_needing_research` correctly handles all three re-research conditions (no research, stale, different prompt_hash)
- [ ] `python -m pytest tests/ -q` passes

---

## Epic 2: LLM Research Module

*The core new capability — Claude API integration for deep company research.*

---

### Story 2.1: Claude API client

**What to read first:**
- `src/settings.py` — `claude_api_key`, `claude_model`, `research_timeout_seconds` fields added in 0.3
- Anthropic Python SDK docs pattern: `anthropic.Anthropic(api_key=...)` + `client.messages.create(...)`

**What to do:**

Create `src/research/__init__.py` (empty) and `src/research/client.py`:

```python
# src/research/client.py
from __future__ import annotations
import logging
import time
from dataclasses import dataclass

import anthropic
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

logger = logging.getLogger(__name__)

@dataclass
class ResearchResponse:
    raw_text: str
    model: str
    input_tokens: int
    output_tokens: int
    duration_seconds: float

class ResearchClient:
    def __init__(self, api_key: str, model: str, timeout_seconds: int, max_retries: int = 3):
        self._client = anthropic.Anthropic(api_key=api_key)
        self.model = model
        self.timeout = timeout_seconds
        self.max_retries = max_retries

    def research_company(self, system_prompt: str, user_prompt: str) -> ResearchResponse:
        """Make one research API call. Raises on hard failure after retries."""
        start = time.monotonic()

        @retry(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=4, max=30),
            retry=retry_if_exception_type((anthropic.RateLimitError, anthropic.APIConnectionError)),
            before_sleep=lambda state: logger.warning("retrying claude api call attempt=%d", state.attempt_number),
        )
        def _call() -> anthropic.types.Message:
            return self._client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
                timeout=self.timeout,
            )

        message = _call()
        duration = time.monotonic() - start
        raw_text = message.content[0].text if message.content else ""

        logger.info(
            "claude_api model=%s input_tokens=%d output_tokens=%d duration_s=%.1f",
            self.model,
            message.usage.input_tokens,
            message.usage.output_tokens,
            duration,
        )

        return ResearchResponse(
            raw_text=raw_text,
            model=self.model,
            input_tokens=message.usage.input_tokens,
            output_tokens=message.usage.output_tokens,
            duration_seconds=duration,
        )
```

Add `anthropic>=0.30` to `requirements.txt` if not already present.

**Files:** New `src/research/__init__.py`, new `src/research/client.py`, `requirements.txt`

**Acceptance criteria:**
- [ ] `from src.research.client import ResearchClient` succeeds
- [ ] `ResearchClient` can be instantiated with a dummy API key without making network calls
- [ ] `ResearchResponse` dataclass has all five fields
- [ ] `tenacity` retry decorates the inner `_call` function (not the outer method)
- [ ] `python -m pytest tests/ -q` passes

---

### Story 2.2: Prompt templates — extraction pass and scoring pass

**This story creates two separate prompt files.** Extraction (pull structured facts) and scoring/personalization (assess fit and generate talking points) are kept separate because combining them degrades both.

**What to read first:**
- `src/scoring/engine.py` — understand `AccountScore` and `top_reasons_json` format
- `config/signal_registry.csv` — understand signal codes and categories
- `docs/review-comparison-sales-research.md` — Qual's prompt schemas section

**What to do:**

**File 1: `src/research/prompts.py`** — prompt construction functions

```python
# src/research/prompts.py
from __future__ import annotations
import hashlib
import json
from pathlib import Path

_EXTRACTION_TEMPLATE_PATH = Path(__file__).parent.parent.parent / "config" / "research_extraction_prompt.md"
_SCORING_TEMPLATE_PATH = Path(__file__).parent.parent.parent / "config" / "research_scoring_prompt.md"

def _load_template(path: Path) -> str:
    return path.read_text(encoding="utf-8")

def build_extraction_prompt(account: dict, signals: list[dict]) -> tuple[str, str]:
    """
    Returns (system_prompt, user_prompt) for the extraction pass.
    The extraction pass pulls structured facts: enrichment JSON + prose brief.

    account dict keys: account_id, company_name, domain, signal_score, signal_tier,
                       delta_7d, top_reasons_json (parsed from DB)
    signals list: recent signal observations with signal_code, source, evidence_url, evidence_text
    """

def build_scoring_prompt(account: dict, research_brief: str) -> tuple[str, str]:
    """
    Returns (system_prompt, user_prompt) for the scoring/personalization pass.
    Input: the prose brief produced by the extraction pass.
    Output: contacts JSON + conversation starters.

    This prompt receives the brief (not raw data) so the LLM reasons about
    a coherent summary, not disconnected raw fields.
    """

def prompt_hash(extraction_template: str, scoring_template: str) -> str:
    """Stable hash of both prompt templates combined. Stored in DB to detect template changes."""
    combined = extraction_template + "|||" + scoring_template
    return hashlib.sha256(combined.encode()).hexdigest()[:16]
```

**File 2: `config/research_extraction_prompt.md`** — system prompt for extraction pass

```markdown
You are a B2B research analyst. Given a company name, domain, and buying signal evidence, produce:

1. A structured enrichment JSON block
2. A 150-200 word prose research brief

## Output format

Produce exactly two sections separated by nothing else:

### ENRICHMENT_JSON
```json
{
  "website": "...", "website_confidence": 0.0-1.0,
  "industry": "...", "industry_confidence": 0.0-1.0,
  "sub_industry": "...", "sub_industry_confidence": 0.0-1.0,
  "employees": integer_or_null, "employees_confidence": 0.0-1.0,
  "employee_range": "...", "employee_range_confidence": 0.0-1.0,
  "revenue_range": "...", "revenue_range_confidence": 0.0-1.0,
  "company_linkedin_url": "...", "company_linkedin_url_confidence": 0.0-1.0,
  "city": "...", "city_confidence": 0.0-1.0,
  "state": "...", "state_confidence": 0.0-1.0,
  "country": "...", "country_confidence": 0.0-1.0,
  "tech_stack": ["..."], "tech_stack_confidence": 0.0-1.0
}
```

### RESEARCH_BRIEF
Write a 150-200 word factual brief covering: what the company does, their scale/stage,
technology environment, and why the buying signals above suggest they are in-market.
Do not speculate. If you are unsure about a fact, omit it or lower its confidence score.
Include no fluff, filler, or marketing language.
```

**File 3: `config/research_scoring_prompt.md`** — system prompt for scoring/personalization pass

```markdown
You are a B2B sales intelligence analyst. Given a company research brief and a list of key contacts,
produce outreach intelligence for a sales rep.

## Output format

Produce exactly two sections:

### CONTACTS_JSON
```json
[
  {
    "first_name": "...",
    "last_name": "...",
    "title": "...",
    "email": "...",
    "linkedin_url": "https://linkedin.com/in/...",
    "management_level": "C-Level|VP|Director|Manager|IC",
    "year_joined": integer_or_null
  }
]
```
Find up to 5 real contacts who are decision-makers or influencers for infrastructure/platform/DevOps purchasing.
Only include contacts you can find with high confidence. Omit fields you cannot verify.

### CONVERSATION_STARTERS
Write 3-5 specific conversation starters for a sales rep. Each must:
- Reference a specific signal or fact from the research brief
- Be framed as a question or observation, not a pitch
- Be 1-2 sentences maximum
Example format: "- I noticed you're hiring a Head of Platform Engineering — are you evaluating internal platforms or consolidating vendors?"
```

**Files:** New `src/research/prompts.py`, new `config/research_extraction_prompt.md`, new `config/research_scoring_prompt.md`

**Acceptance criteria:**
- [ ] `build_extraction_prompt()` returns a tuple of (system_str, user_str)
- [ ] `build_scoring_prompt()` returns a tuple of (system_str, user_str)
- [ ] `prompt_hash()` returns a 16-char hex string
- [ ] User prompt for extraction includes: company name, domain, signal codes, evidence URLs/text
- [ ] User prompt for scoring includes: the research brief, not raw signal data
- [ ] Both config `.md` files exist and are readable
- [ ] `python -m pytest tests/ -q` passes

---

### Story 2.3: Response parser

**Why:** This is the most failure-prone component. Claude sometimes wraps JSON in extra prose, uses slightly wrong section headers, or returns partial responses. The parser must handle all these gracefully.

**What to read first:**
- `src/research/prompts.py` — understand exactly what output format the prompts request
- The enrichment JSON schema section at the top of this plan

**What to do:**

Create `src/research/parser.py`:

```python
# src/research/parser.py
from __future__ import annotations
import json
import logging
import re
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class Contact:
    first_name: str
    last_name: str
    title: str = ""
    email: str = ""
    linkedin_url: str = ""
    management_level: str = "IC"
    year_joined: int | None = None

@dataclass
class CompanyEnrichment:
    website: str = ""
    industry: str = ""
    sub_industry: str = ""
    employees: int | None = None
    employee_range: str = ""
    revenue_range: str = ""
    company_linkedin_url: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    tech_stack: list[str] = field(default_factory=list)
    # Confidence scores (0.0-1.0). Fields with confidence < 0.5 are omitted from CSV.
    confidences: dict[str, float] = field(default_factory=dict)

@dataclass
class ParsedExtractionResponse:
    enrichment: CompanyEnrichment
    research_brief: str
    parse_errors: list[str] = field(default_factory=list)

@dataclass
class ParsedScoringResponse:
    contacts: list[Contact]
    conversation_starters: list[str]
    parse_errors: list[str] = field(default_factory=list)

def parse_extraction_response(raw_text: str) -> ParsedExtractionResponse:
    """
    Parse the two-section extraction response:
    - ### ENRICHMENT_JSON (JSON block)
    - ### RESEARCH_BRIEF (prose)

    Tolerates:
    - Missing section headers (falls back to first JSON block found)
    - Extra prose before/after sections
    - Missing fields (uses defaults)
    - Confidence < 0.5 (records in confidences dict, caller decides whether to use field)
    """

def parse_scoring_response(raw_text: str) -> ParsedScoringResponse:
    """
    Parse the two-section scoring response:
    - ### CONTACTS_JSON (JSON array)
    - ### CONVERSATION_STARTERS (bullet list)

    Tolerates:
    - Partial contacts (contact with only first_name+last_name is still valid)
    - Empty contacts list
    - Numbered or bulleted conversation starters
    """

def _extract_json_block(text: str, section_header: str) -> str | None:
    """
    Extract the content of a ```json ... ``` block after a given section header.
    Falls back to first JSON block in text if header not found.
    Returns raw JSON string or None.
    """

def _parse_bullet_list(text: str) -> list[str]:
    """Extract bullet points from text. Handles -, *, • and numbered lists."""
```

Key implementation rules:
- **Never raise from parse functions** — all errors go into `parse_errors` list and the function returns whatever it could extract
- **Confidence threshold:** fields with `_confidence < 0.5` should have the field value set to its default (empty string / None) and a warning added to `parse_errors`
- **JSON extraction fallback:** try section header first, then regex for first `{...}` or `[...]` block

**Files:** New `src/research/parser.py`

**Acceptance criteria:**
- [ ] `parse_extraction_response` and `parse_scoring_response` never raise exceptions on malformed input
- [ ] Both return dataclasses with `parse_errors` list populated for any parsing failures
- [ ] A response with missing `ENRICHMENT_JSON` section returns an empty `CompanyEnrichment` with a parse error, not an exception
- [ ] Confidence scores < 0.5 result in the field being zeroed/emptied and a parse_error added
- [ ] `python -m pytest tests/ -q` passes

---

### Story 2.4: Pre-LLM waterfall enrichment

**Why:** Clearbit free tier and Hunter.io free tier can fill many enrichment fields (industry, headcount, email domain pattern) before the LLM call. This cuts Claude token usage (shorter prompts, no need to guess what Clearbit already knows) and improves data accuracy on factual fields.

**What to read first:**
- `src/research/parser.py` — `CompanyEnrichment` dataclass (Story 2.3)
- `src/settings.py` — `clearbit_api_key`, `hunter_api_key` fields (Story 0.3)

**What to do:**

Create `src/research/enrichment.py`:

```python
# src/research/enrichment.py
"""
Pre-LLM waterfall enrichment.
Sources are tried in order: Clearbit → Hunter → (future: Wappalyzer)
Each source fills only fields that are currently empty.
Every filled field is tagged with source + confidence in the enrichment dict.
"""
from __future__ import annotations
import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

@dataclass
class EnrichmentResult:
    """Partial enrichment from one source. Merged with other sources by caller."""
    website: str = ""
    industry: str = ""
    sub_industry: str = ""
    employees: int | None = None
    employee_range: str = ""
    city: str = ""
    state: str = ""
    country: str = ""
    company_linkedin_url: str = ""
    source: str = ""
    confidence: float = 0.0

def enrich_from_clearbit(domain: str, api_key: str) -> EnrichmentResult | None:
    """
    Clearbit Enrichment API: GET https://company.clearbit.com/v2/companies/find?domain={domain}
    Free tier: 100 calls/month.
    Returns None on any error (network, 404, rate limit).
    """

def enrich_from_hunter(domain: str, api_key: str) -> EnrichmentResult | None:
    """
    Hunter.io Domain Search API: GET https://api.hunter.io/v2/domain-search?domain={domain}&api_key={key}
    Returns company name, industry, country from the domain search response.
    Free tier: 25 searches/month.
    Returns None on any error.
    """

def run_enrichment_waterfall(domain: str, settings) -> dict:
    """
    Try enrichment sources in order. Return a merged enrichment dict
    (same schema as enrichment_json in DB) with _source and _confidence for each field.

    Only fills fields that are currently empty — does not overwrite.
    If both api_keys are empty strings, returns an empty dict immediately (no API calls).

    Fields filled here are marked with high confidence (from structured APIs).
    The LLM extraction pass will fill remaining empty fields.
    """
```

The waterfall result is a dict in the same schema as `enrichment_json`. It's passed to `build_extraction_prompt()` so the prompt can tell Claude which fields are already known and which need research.

**Files:** New `src/research/enrichment.py`

**Acceptance criteria:**
- [ ] If `clearbit_api_key = ""` and `hunter_api_key = ""`, `run_enrichment_waterfall` returns `{}` immediately (no HTTP calls)
- [ ] Each function returns `None` on any exception (never raises)
- [ ] Returned dict follows enrichment JSON schema with `_confidence` fields
- [ ] `python -m pytest tests/ -q` passes

---

### Story 2.5: Research orchestrator

**Why:** Coordinates the full research flow per account. Uses all components built in 2.1–2.4.

**What to read first:**
- `src/research/client.py` (2.1)
- `src/research/prompts.py` (2.2)
- `src/research/parser.py` (2.3)
- `src/research/enrichment.py` (2.4)
- `src/db.py` — `get_accounts_needing_research`, `mark_research_in_progress`, `upsert_company_research`, `upsert_contacts`, `create_research_run`, `finish_research_run` (Story 1.2)

**What to do:**

Create `src/research/orchestrator.py`:

```python
# src/research/orchestrator.py
from __future__ import annotations
import json
import logging

from src.research.client import ResearchClient
from src.research.enrichment import run_enrichment_waterfall
from src.research.parser import parse_extraction_response, parse_scoring_response
from src.research.prompts import build_extraction_prompt, build_scoring_prompt, prompt_hash
from src import db

logger = logging.getLogger(__name__)

def run_research_stage(conn, settings, run_date: str, score_run_id: str) -> dict:
    """
    Main entry point. Called from pipeline after scoring.

    Flow:
    1. Load both prompt templates, compute prompt_hash
    2. Create research_run record
    3. Query accounts needing research (respects max_accounts, stale_days, prompt_hash gate)
    4. For each account:
       a. mark_research_in_progress (BEFORE any API call)
       b. Load signal observations from DB
       c. Run waterfall enrichment (Clearbit → Hunter)
       d. Build extraction prompt (include pre-filled enrichment so LLM fills gaps)
       e. Call Claude extraction pass
       f. Parse extraction response → research_brief + enrichment
       g. Build scoring prompt (uses research_brief as input)
       h. Call Claude scoring pass
       i. Parse scoring response → contacts + conversation_starters
       j. Store: upsert_company_research + upsert_contacts
       k. Log success with token counts
    5. On any per-account exception: set research_status='failed', log, continue
    6. finish_research_run with final counts
    7. Return summary dict: {attempted, completed, failed, skipped, total_tokens}

    Returns summary dict even on partial failure.
    """
    client = ResearchClient(
        api_key=settings.claude_api_key,
        model=settings.claude_model,
        timeout_seconds=settings.research_timeout_seconds,
    )

    # compute prompt_hash from template files
    # get accounts needing research
    # for each account: follow the flow above
    # ...
```

**Critical implementation notes:**

1. **`mark_research_in_progress` must be called before ANY API call** — this is the guard against double-processing on restart
2. **Two separate API calls per account** — extraction pass, then scoring pass. The scoring prompt receives only the research brief from pass 1, not raw signal data.
3. **Store even partial results** — if extraction succeeds but scoring fails, store what was extracted and mark `research_status='failed'` with a log warning
4. **Error isolation** — wrap the per-account logic in `try/except Exception as exc: logger.warning(...)`. One company failure must never stop the batch.
5. **Skip accounts where `claude_api_key = ""`** — set status to 'skipped' and log a clear warning
6. **Prompt hash re-research:** accounts with a different `prompt_hash` in DB from the current `prompt_hash` should be re-researched, regardless of `researched_at`

**Files:** New `src/research/orchestrator.py`

**Acceptance criteria:**
- [ ] `mark_research_in_progress()` is called before `ResearchClient.research_company()` in the per-account loop
- [ ] Two separate Claude API calls are made per account (extraction pass + scoring pass)
- [ ] One failed account does not abort the batch — the loop continues
- [ ] Returns a dict with `attempted`, `completed`, `failed`, `skipped`, `total_input_tokens`, `total_output_tokens`
- [ ] When `claude_api_key` is empty, returns immediately with all accounts as `skipped`
- [ ] `python -m pytest tests/ -q` passes

---

## Epic 3: Unified CSV Export

*Produce the final sales-ready CSV combining signal scores with LLM research.*

---

### Story 3.1: Build the sales-ready CSV exporter

**What to read first:**
- The target CSV schema at the top of this plan (all 26 columns)
- `src/export/csv_exporter.py` — existing export functions to understand the pattern
- `src/db.py` — `account_scores`, `accounts` table schema

**What to do:**

Add to `src/export/csv_exporter.py`:

```python
def export_sales_ready(
    conn,
    score_run_id: str,
    output_path: Path,
    excluded_domains: set[str] | None = None,
) -> int:
    """
    Export the unified sales-ready CSV.
    Returns number of rows written.

    Query logic:
    - JOIN: account_scores → accounts → company_research (LEFT JOIN, research may be absent)
    - Subquery: MAX(observed_at) FROM signal_observations GROUP BY account_id → last_signal_date
    - Subquery: get contacts per account (sorted by management_level seniority)
    - Filter: signal_tier IN ('high', 'medium')
    - Exclude: domains in excluded_domains set
    - Sort: signal_score DESC
    - One row per account (account_scores already has one row per account per run)

    For each row:
    - Parse enrichment_json: extract each field, skip fields with _confidence < 0.5
    - Parse top_reasons_json: format as "signal_code via source" for top_reason_1/2/3
    - Format key_contacts: "FirstName LastName (Title) — linkedin_url", max 5, seniority order
    - Format conversation_starters: extract from research_profile (look for bullet section)
    - research_status: from company_research; if no row, use 'skipped'
    - research_brief: from company_research.research_brief; empty string if absent
    """
```

**Column formatting rules:**
- `delta_7d`: format as `+5.2` or `-3.1` (always show sign)
- `top_signals`: pipe-separated signal codes from top_reasons_json, e.g. `devops_role_open|compliance_initiative`
- `evidence_links`: pipe-separated URLs from top_reasons_json (deduplicated)
- `key_contacts`: one contact per line, `\n`-separated within the cell. CSV writer handles quoting.
- `conversation_starters`: one starter per line, `\n`-separated within the cell
- Empty fields: empty string (not `None`, not `"null"`)
- `first_seen_date`, `last_signal_date`: ISO date format `YYYY-MM-DD`

**Files:** `src/export/csv_exporter.py`

**Acceptance criteria:**
- [ ] Output CSV has exactly the 26 columns defined in the target schema (in that order)
- [ ] Accounts without research appear with `research_status=skipped` and empty research columns
- [ ] Enrichment fields with `_confidence < 0.5` appear as empty strings in CSV
- [ ] `key_contacts` is limited to 5 contacts, ordered by seniority
- [ ] No `None` values in any CSV cell — all empty fields are empty strings
- [ ] `python -m pytest tests/ -q` passes

---

### Story 3.2: Wire research + export into the pipeline

**What to read first:**
- `src/main.py` in full — CLI command structure, `run_daily` and `run_autonomous_loop` functions

**What to do:**

1. **Add `research` CLI command** to `main.py`:
```python
@app.command()
def research(
    date: str = typer.Option(..., help="Run date YYYY-MM-DD"),
    score_run_id: str = typer.Option(..., help="Score run ID to research"),
    max_accounts: int = typer.Option(None, help="Override research_max_accounts setting"),
    force_refresh: bool = typer.Option(False, help="Re-research even if already completed"),
) -> None:
    """Run LLM research on top-scoring accounts from a score run."""
```

2. **Add `export-sales-ready` CLI command**:
```python
@app.command()
def export_sales_ready(
    date: str = typer.Option(..., help="Run date YYYY-MM-DD"),
    score_run_id: str = typer.Option(..., help="Score run ID to export"),
    output: Path = typer.Option(None, help="Output path (default: out_dir/sales_ready_{date}.csv)"),
) -> None:
    """Export the unified sales-ready CSV for a given score run."""
```

3. **Modify `run_daily`** — add research and export stages:
```
Stage 1: ingest (existing)
Stage 2: score (existing)
Stage 3: research (new — runs after scoring, uses score_run_id from stage 2)
Stage 4: export (existing exports + new sales-ready CSV)
Stage 5: ops_metrics (existing)
```

4. **Research stage must be non-blocking** — if research fails entirely (e.g., API key missing), `run_daily` logs a warning and continues to export. The CSV is still produced; affected accounts get `research_status=skipped`.

5. **Apply same `_run_with_watchdog` pattern** to research stage as other stages.

**Files:** `src/main.py`

**Acceptance criteria:**
- [ ] `python -m signals research --help` works
- [ ] `python -m signals export-sales-ready --help` works
- [ ] `run_daily` includes research stage between score and export
- [ ] If `claude_api_key` is empty, `run_daily` logs a warning for research stage but does not fail
- [ ] Sales-ready CSV is always written, even if research stage partially or fully failed
- [ ] `python -m pytest tests/ -q` passes

---

## Epic 4: Tests

*Write tests alongside the corresponding Epic 2-3 stories. Do not wait until the end.*

---

### Story 4.1: Tests for response parser

**Target file:** `src/research/parser.py`

**What to do:** Create `tests/test_research_parser.py` with these test cases:

```python
# tests/test_research_parser.py

class TestParseExtractionResponse:
    def test_well_formed_response_parses_all_fields(self): ...
    def test_missing_enrichment_section_returns_empty_enrichment_with_error(self): ...
    def test_missing_brief_section_returns_empty_brief_with_error(self): ...
    def test_malformed_json_in_enrichment_returns_empty_with_error(self): ...
    def test_confidence_below_0_5_zeros_out_field(self): ...
    def test_extra_prose_before_sections_is_tolerated(self): ...
    def test_never_raises_on_completely_empty_string(self): ...

class TestParseScoringResponse:
    def test_well_formed_response_parses_contacts_and_starters(self): ...
    def test_empty_contacts_array_is_valid(self): ...
    def test_contact_missing_linkedin_url_is_still_included(self): ...
    def test_malformed_contacts_json_returns_empty_list_with_error(self): ...
    def test_numbered_conversation_starters_are_parsed(self): ...
    def test_bulleted_conversation_starters_are_parsed(self): ...
    def test_never_raises_on_garbage_input(self): ...
```

Use fixtures with realistic raw Claude response strings (hardcoded in the test file, no mocking needed).

**Files:** New `tests/test_research_parser.py`

**Acceptance criteria:**
- [ ] All test cases exist and pass
- [ ] `parse_extraction_response` and `parse_scoring_response` never raise in any test case

---

### Story 4.2: Tests for prompt builder

**Target file:** `src/research/prompts.py`

**What to do:** Create `tests/test_research_prompts.py`:

```python
class TestBuildExtractionPrompt:
    def test_prompt_includes_company_name(self): ...
    def test_prompt_includes_domain(self): ...
    def test_prompt_includes_signal_codes(self): ...
    def test_prompt_includes_evidence_urls(self): ...
    def test_prompt_handles_account_with_no_signals(self): ...
    def test_prompt_includes_pre_filled_enrichment_when_provided(self): ...

class TestBuildScoringPrompt:
    def test_prompt_includes_research_brief(self): ...
    def test_prompt_does_not_include_raw_signal_data(self): ...

class TestPromptHash:
    def test_same_templates_produce_same_hash(self): ...
    def test_different_templates_produce_different_hash(self): ...
    def test_hash_is_16_chars(self): ...
```

**Files:** New `tests/test_research_prompts.py`

**Acceptance criteria:**
- [ ] All test cases exist and pass

---

### Story 4.3: Tests for sales-ready CSV export

**Target file:** `src/export/csv_exporter.py` — `export_sales_ready` function

**What to do:** Create `tests/test_sales_ready_export.py` as an integration test (uses a real Postgres test DB — follow the pattern of existing integration tests in `tests/`):

```python
class TestExportSalesReady:
    def test_output_has_exactly_26_columns_in_correct_order(self): ...
    def test_only_high_and_medium_tier_accounts_are_included(self): ...
    def test_accounts_without_research_appear_with_status_skipped(self): ...
    def test_enrichment_fields_with_low_confidence_are_empty_in_csv(self): ...
    def test_key_contacts_limited_to_5_sorted_by_seniority(self): ...
    def test_no_none_values_in_any_csv_cell(self): ...
    def test_excluded_domains_are_not_in_output(self): ...
    def test_sorted_by_signal_score_desc(self): ...
    def test_delta_7d_formatted_with_sign(self): ...
```

**Files:** New `tests/test_sales_ready_export.py`

**Acceptance criteria:**
- [ ] All test cases exist and pass against a test Postgres DB

---

### Story 4.4: Tests for research orchestrator

**Target file:** `src/research/orchestrator.py`

**What to do:** Create `tests/test_research_orchestrator.py`. Mock `ResearchClient` and the waterfall enrichment. Use a real test DB for DB interactions.

```python
class TestRunResearchStage:
    def test_returns_summary_dict_with_all_count_fields(self): ...
    def test_only_high_and_medium_tier_accounts_are_researched(self): ...
    def test_already_completed_accounts_are_skipped(self): ...
    def test_one_failed_account_does_not_abort_batch(self): ...
    def test_max_accounts_limit_is_respected(self): ...
    def test_mark_in_progress_called_before_api_call(self): ...
    def test_returns_all_skipped_when_api_key_is_empty(self): ...
    def test_accounts_with_stale_research_are_re_researched(self): ...
    def test_accounts_with_different_prompt_hash_are_re_researched(self): ...
    def test_two_api_calls_made_per_account(self): ...
```

**Files:** New `tests/test_research_orchestrator.py`

**Acceptance criteria:**
- [ ] All test cases exist and pass
- [ ] The mock confirms `mark_research_in_progress` is called before `research_company`

---

## Epic 5: End-to-End Validation

*Run the full pipeline with real data and validate output quality. Human review required.*

---

### Story 5.1: Dry run

**Prerequisites:** All of Epics 0-4 complete.

**Steps:**
```bash
# From the repo root
python -m signals ingest --all
python -m signals score
python -m signals research --date $(date +%Y-%m-%d) --score-run-id <latest_run_id> --max-accounts 5
python -m signals export-sales-ready --date $(date +%Y-%m-%d) --score-run-id <latest_run_id>
```

Open `output/sales_ready_{date}.csv` in a spreadsheet. Review:
- Are research briefs accurate and non-hallucinated?
- Are contacts real people at the right companies?
- Are conversation starters specific and usable?
- Are enrichment fields (industry, employees, country) correct?
- Are signal reasons readable and accurate?

**This story is done when the user approves the output quality.** The user's approval triggers Story 5.2.

---

### Story 5.2: Prompt tuning

**Trigger:** User has reviewed the dry-run CSV and has specific feedback.

**Common failure modes and fixes:**

| Problem | Fix |
|---------|-----|
| Research brief too generic | Add "be specific — mention actual products, customers, or initiatives" to extraction prompt |
| Contacts missing or wrong | Add "search LinkedIn for current employees at {company}" emphasis |
| Conversation starters are pitches not questions | Add examples of bad starters vs. good starters to scoring prompt |
| Enrichment fields missing or wrong | Raise confidence threshold required for inclusion; or improve waterfall (add more sources) |
| Research too long | Add explicit word count constraint to extraction prompt |
| Hallucinated data | Strengthen "only include facts you can cite" instruction; add "if unsure, set confidence to 0" |

**How to tune without code changes:**
- Edit `config/research_extraction_prompt.md` and/or `config/research_scoring_prompt.md` directly
- Re-run `research --force-refresh --max-accounts 5` on the same 5 accounts
- The changed `prompt_hash` will trigger re-research automatically
- Compare new CSV against old CSV

**When tuning is complete:** The two prompt files are the only artifact — no code changes needed. Their content is the output of this story.

---

## What's Explicitly NOT in Scope

- **Async pipeline** — sequential is fine for 20 accounts/day; async is a Phase 3 optimization
- **Splitting `db.py`** — add to it but don't restructure it
- **Person-level deep research** — company + contacts is MVP; per-person research is a follow-up
- **Google Sheets sync** — comes after user approves CSV output quality
- **Zoho CRM sync** — comes after Sheets sync; use Zoho's `POST /crm/v2/Leads/upsert` with `duplicate_check_fields: ["Domain_Name"]` (documented for when ready)
- **Review UI** — Zoho IS the review UI
- **Discovery pipeline changes** — not touching existing discovery in this phase
- **New signal sources (GitHub, LinkedIn provider)** — signal coverage improvements are a future epic
- **Wappalyzer live detection** — the open-source rules approach is worth building; save for "Signal Sources Expansion" epic
- **ML-based lead scoring** — the deterministic formula is correct for this stage; ML requires labeled training data we don't have yet

---

## Appendix: Post-CSV Zoho Sync Pattern

*Not in scope now. Documented here for when the time comes.*

When the CSV output quality is approved and Zoho sync is ready to build:

**Correct Zoho API call (one call handles both create and update):**
```
POST https://www.zohoapis.com/crm/v2/Leads/upsert
Authorization: Zoho-oauthtoken {token}
Content-Type: application/json

{
  "data": [...],
  "duplicate_check_fields": ["Domain_Name"]
}
```

**Field mapping note:** Google Sheets column headers (e.g., "Company Name") do not match Zoho API field names (e.g., "Company"). A field mapping dict is required in the sync code. Use the Zoho API field names, not the display names.

**Do NOT use the n8n built-in Zoho CRM node for upsert** — it does not cleanly expose the `duplicate_check_fields` parameter. Use the HTTP Request node (in n8n) or direct `requests.post()` (in Python) against the Zoho API directly.

**Conflict resolution (bidirectional sync):** If both Zoho and the CSV have a field set, use `updated_at` timestamp comparison — the more recently updated value wins. Store Zoho record IDs in a local mapping table so future updates use direct record ID (no search needed).
