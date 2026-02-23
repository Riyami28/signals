# Code Quality Review — Signals Repo

## Critical Issues

### 1. Silent Exception Swallowing (6 locations)

Bare `except Exception: pass` blocks hide real failures:

| File | Line | Context |
|------|------|---------|
| `src/collectors/community.py` | 73-74 | Datetime parsing |
| `src/collectors/news.py` | 78-79 | Feed timestamp parsing |
| `src/discovery/config.py` | 244-245 | Domain extraction |
| `src/discovery/watchlist_builder.py` | 130-131 | Host extraction |
| `src/main.py` | 342-343 | Blocklist loading |
| `src/main.py` | 350-351 | Account profiles loading |

**Recommendation**: Add `logging.debug()` or `logging.warning()` calls so failures are traceable without changing control flow.

### 2. No Structured Logging

The codebase has **no `logging` module usage**. All error handling relies on exceptions and return values. For a pipeline that runs autonomously (`run-autonomous-loop`), this is a significant observability gap.

**Recommendation**: Add Python `logging` with structured output (JSON for production) at minimum in main.py, db.py, and collectors.

### 3. Test Coverage Gaps (~40-50% estimated)

**Modules with NO tests:**
- `src/http_client.py` — used extensively across collectors
- `src/models.py` — Pydantic data models
- `src/notifier.py` — alerting (Google Chat, email)
- `src/settings.py` — configuration loading
- `src/export/` — CSV export
- All individual collectors (`jobs.py`, `news.py`, `community.py`, `technographics.py`, `first_party.py`)
- Discovery submodules: `config.py`, `fetcher.py`, `frontier.py`, `parser.py`, `webhook.py`, `multilingual.py`, `speaker_intel.py`
- Scoring submodules: `engine.py`, `explain.py`, `rules.py`

Only **59 test functions** across 13 test files for a codebase of 41 source files.

## Architectural Concerns

### 4. Oversized Files

| File | Lines | Functions | Suggested Split |
|------|-------|-----------|-----------------|
| `src/db.py` | **2,487** | 88 | Connection mgmt, account ops, discovery ops, metrics, retry queue |
| `src/main.py` | **1,936** | many | CLI definitions, stage executors, retry handlers, utilities |
| `src/discovery/pipeline.py` | 867 | 20 | Domain resolution, policy evaluation, ranking could be separate |
| `src/collectors/jobs.py` | 696 | 14 | API collectors vs. generic crawl could split |

`db.py` at 2,487 lines with 88 functions is the biggest maintainability risk.

### 5. No Schema Versioning

Migrations use `CREATE TABLE IF NOT EXISTS` (idempotent but unversioned). No migration changelog or version tracking.

**Recommendation**: Consider Alembic or a lightweight numbered-migration approach.

### 6. No Linting or Pre-commit Hooks

- No `.pylintrc`, `ruff.toml`, `mypy.ini`, or `.pre-commit-config.yaml`
- Multiple `# type: ignore` comments
- No automated code quality enforcement beyond `pytest`

**Recommendation**: Add `ruff` and a basic `mypy` config.

## Minor Issues

### 7. Dev Credentials as Defaults

`src/db.py:520` and `src/settings.py:92` fall back to `"signals_dev_password"` when `SIGNALS_PG_PASSWORD` is unset.

### 8. Large Config CSV Committed

`config/account_source_handles.csv` is **578KB**.

## What's Done Well

- Clean separation of concerns: collectors, discovery, scoring, export, reporting
- No secrets committed: `.env` properly gitignored
- No TODO/FIXME debt markers
- CI with PostgreSQL service: tests run against a real database
- Migration safety: CI blocks destructive `DROP` statements
- Configuration-driven signals: new signal types can be added via CSV
- Concurrency controls: advisory locks, transaction-safe collectors, multi-worker discovery

## Priority Recommendations

1. **Add structured logging** — highest-impact improvement for operability
2. **Add ruff + mypy** — catch bugs early, enforce consistency
3. **Split `db.py`** — reduce merge conflicts and improve navigability
4. **Test the collectors** — most failure-prone modules with zero unit tests
5. **Add schema versioning** — before the next non-additive schema change
6. **Replace silent `except: pass`** — add at least debug-level logging
