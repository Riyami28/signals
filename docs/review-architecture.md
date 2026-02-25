# Architecture Review — What I Would Do Differently From Scratch

## 1. Async from Day One

The current codebase uses synchronous `requests` + `time.sleep()` rate limiting with thread locks. Every collector blocks while waiting for HTTP responses. For a system that crawls job boards, RSS feeds, and web pages across hundreds of accounts, this is the single biggest performance bottleneck.

**What I'd do:** Build on `asyncio` + `httpx` (or `aiohttp`). Rate limiting becomes `asyncio.Semaphore` per domain. You get 10-50x throughput on the collector stage without threads. The current architecture would require a near-rewrite to retrofit async — which is why this matters at project start.

## 2. Don't Write a SQL Dialect Translation Layer

`db.py` lines 89-114 contain `_rewrite_sql_for_postgres()` — a hand-rolled regex engine that translates SQLite-dialect SQL (`?` params, `INSERT OR IGNORE`, `datetime('now')`, `AUTOINCREMENT`) into PostgreSQL on the fly. This is an impressive amount of effort to maintain portability that **the project no longer needs** (it's Postgres-only now). Every new query has to be written in "SQLite that happens to translate to Postgres" — which is neither dialect done properly.

**What I'd do:** Use SQLAlchemy Core (not ORM) or raw `psycopg` with native Postgres SQL. Write Postgres directly. You get `RETURNING` clauses, `ON CONFLICT ... DO UPDATE`, `jsonb`, `INTERVAL` arithmetic, and CTEs without regex translation. The compatibility layer is ~120 lines of subtle bugs waiting to happen.

## 3. Real Migration System Instead of `CREATE TABLE IF NOT EXISTS`

The schema is defined as a single `SCHEMA_SQL` string constant, plus a `_run_schema_migrations()` function that manually checks if columns exist and adds them. This works until you need to rename a column, change a type, add a NOT NULL column with data backfill, or drop a table. Then you have no audit trail and no rollback path.

**What I'd do:** Alembic from day one. Numbered migrations, auto-generated diffs, rollback support.

## 4. Dependency Injection Instead of Global Bootstrap

Every CLI command calls `_bootstrap()` which creates a `Settings`, opens a DB connection, seeds accounts, and returns all three. Functions deep in the call chain reach into `settings` for 30+ config values and pass `conn` around as an untyped positional arg. There's no way to test a scoring engine without a live Postgres connection and a full config directory.

**What I'd do:**
- Define narrow interfaces: `Collector` protocol needs `HttpClient` + `SignalWriter`, not `(conn, settings, lexicon, source_reliability)`.
- Use a lightweight DI container or just constructor injection: `JobsCollector(http=..., writer=..., lexicon=...)`.
- Make `conn` a typed protocol so tests can inject fakes.

This is why 80% of the codebase has no tests — everything requires a running database and a populated filesystem.

## 5. Collector as a Protocol, Not Five Bespoke Modules

The five collectors (`jobs`, `news`, `community`, `technographics`, `first_party`) all follow the same pattern: load config → iterate accounts → fetch HTTP → parse response → match keywords → create `SignalObservation` → insert into DB. But each reimplements this from scratch with slightly different structures.

**What I'd do:** Define a `Collector` protocol:
```python
class Collector(Protocol):
    def sources(self, accounts: list[Account]) -> Iterable[FetchTarget]: ...
    def parse(self, response: Response, target: FetchTarget) -> list[RawSignal]: ...
    def classify(self, raw: RawSignal, lexicon: Lexicon) -> list[SignalObservation]: ...
```
Then the orchestrator handles the common loop (rate limiting, dedup, checkpointing, error recording) once. Each collector only implements the parts that differ.

## 6. Structured Logging from the Start

There is **zero** use of Python's `logging` module. For a pipeline that runs autonomously on a cron (`run-autonomous-loop`), this is a critical gap.

**What I'd do:** `structlog` with JSON output. Every collector emits structured log events. Every DB write emits a count. Every HTTP failure logs the URL and status.

## 7. Pydantic All the Way Down (Not Just for Models)

The `models.py` file uses Pydantic, but the rest of the codebase passes around `dict[str, Any]` constantly — especially in scoring, discovery, and export. The scoring engine receives `list[dict]`, indexes into it with string keys, casts everything to `float()` with fallbacks. One typo in a key name is a silent zero instead of an error.

**What I'd do:** Pydantic models for everything crossing a boundary: config loading, collector output, scoring input/output, export rows. Let Pydantic do the validation instead of hundreds of `float(row.get("x", 0.0) or 0.0)` patterns.

## 8. Settings as Pydantic, Not a 200-Line Function

`load_settings()` is 160 lines of repetitive `os.getenv() → try/except ValueError → fallback` blocks. Each integer setting has its own 5-line try/except. This is the exact problem `pydantic-settings` solves.

## 9. Separate the "What" from the "When"

`main.py` at ~2,000 lines mixes three concerns: CLI command definitions (Typer), pipeline orchestration (what stages run in what order), and operational infrastructure (advisory locks, retry queues, watchdog timers, alerting).

**What I'd do:**
- `cli.py` — thin Typer commands that parse args and delegate
- `orchestrator.py` — stage sequencing, retry logic, lock management
- `pipeline/{ingest,score,discover,hunt}.py` — each stage's business logic

## 10. Test-Friendly Architecture from Day One

The reason there are only 59 tests is structural, not motivational. When every function takes `(conn, settings)` and makes real DB calls, testing requires a full Postgres instance.

**What I'd do:**
- Pure functions for business logic (scoring, classification, ranking) that take data in, return data out, no side effects
- Repository pattern for DB access with in-memory fakes for unit tests
- Integration tests with a test database for the data layer only

---

## What I'd Keep

The project gets several things right that I wouldn't change:

- **CSV-driven config** — signal rules, lexicons, thresholds, and source policies as CSVs is pragmatically excellent. Non-engineers can tune the system without code changes.
- **Domain-rate-limiting + robots.txt** — responsible crawling built in from the start.
- **Advisory locks** — proper concurrency protection for the autonomous loop.
- **Retry queue with quarantine** — the operational maturity of the failure handling (backoff → retry → quarantine → alert) is genuinely well-designed.
- **Recency decay scoring** — the half-life-based signal decay is a clean, tunable model.
- **Dedup by content hash** — `stable_hash` on payloads prevents duplicate observations without timestamp-based heuristics.

The core *domain model* is sound. The issues are all in the engineering scaffolding around it.
