from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import typer

from src import db
from src.scoring.rules import load_source_registry
from src.settings import load_settings
from src.source_policy import load_source_execution_policy
from src.utils import normalize_domain, write_csv_rows

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Operator-friendly CLI for running signals locally.",
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MONITOR_SCRIPT = PROJECT_ROOT / "scripts" / "run_daily_live_monitor.sh"


def _today_iso() -> str:
    return date.today().isoformat()


def _normalize_date(value: str | None) -> str:
    raw = (value or "").strip()
    if not raw:
        return _today_iso()
    try:
        parsed = date.fromisoformat(raw)
    except ValueError as exc:
        raise typer.BadParameter(f"Invalid date: {raw} (expected YYYY-MM-DD)") from exc
    return parsed.isoformat()


def _python_bin() -> str:
    venv_python = PROJECT_ROOT / ".venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable or "python3"


def _apply_fast_fail_network(env: dict[str, str], enabled: bool) -> None:
    if not enabled:
        return
    env["SIGNALS_HTTP_TIMEOUT_SECONDS"] = "2"
    env["SIGNALS_MIN_DOMAIN_REQUEST_INTERVAL_MS"] = "0"
    env["SIGNALS_RESPECT_ROBOTS_TXT"] = "0"


def _apply_workers_per_source(env: dict[str, str], workers_per_source: int | None) -> None:
    if workers_per_source is None:
        return
    env["SIGNALS_LIVE_WORKERS_PER_SOURCE"] = str(max(1, int(workers_per_source)))


def _run_subprocess(cmd: list[str], env: dict[str, str]) -> int:
    completed = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        check=False,
    )
    return int(completed.returncode)


def _tier_value(tier: str) -> int:
    normalized = (tier or "").strip().lower()
    if normalized == "high":
        return 3
    if normalized == "medium":
        return 2
    return 1


def _source_reliability_label(reliability: float) -> str:
    if reliability <= 0:
        return "disabled"
    if reliability >= 0.8:
        return "high"
    if reliability >= 0.65:
        return "medium"
    return "low"


def _safe_pct(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((float(numerator) / float(denominator)) * 100.0, 1)


def _source_health_note(
    source: str,
    attempts: int,
    success_rate_pct: float,
    evidence_rate_pct: float,
    approved_rate: float | None,
    approved_sample_size: int,
    reliability: float,
) -> str:
    if source == "first_party_csv":
        return "product-event source; crawl attempts not applicable"
    if source in {"news_csv", "community_csv", "technographics_csv", "jobs_csv"}:
        return "manual seed source; quality depends on input curation"
    if reliability <= 0:
        return "disabled in source_registry"
    if attempts == 0:
        return "no crawl attempts in this run-date"
    if attempts >= 10 and success_rate_pct < 45.0:
        return "low fetch reliability; tune source/policy"
    if evidence_rate_pct < 30.0:
        return "low citation coverage; improve extraction"
    if approved_rate is not None and approved_sample_size >= 10 and approved_rate < 0.5:
        return "low analyst precision; source may be noisy"
    if source in {"google_news_rss", "rss_feed", "reddit_rss", "website_scan"}:
        return "healthy live source; keep"
    return "healthy"


_POLICY_SOURCE_ALIAS: dict[str, str] = {
    "careers_live": "jobs_pages",
    "reddit_rss": "reddit_api",
    "news_csv": "news_rss",
    "community_csv": "reddit_api",
    "technographics_csv": "technographics",
    "jobs_csv": "jobs_pages",
}


def _run_watch(
    run_date: str,
    live_max_accounts: int,
    stage_timeout_seconds: int,
    poll_interval_seconds: int,
    workers_per_source: int | None,
    live: bool,
    fast_fail_network: bool,
    extra_env: dict[str, str] | None = None,
) -> int:
    if not MONITOR_SCRIPT.exists():
        raise typer.BadParameter(f"Missing monitor script: {MONITOR_SCRIPT}")

    cmd = [
        str(MONITOR_SCRIPT),
        run_date,
        str(max(1, int(live_max_accounts))),
        str(max(30, int(stage_timeout_seconds))),
        str(max(1, int(poll_interval_seconds))),
        str(max(1, int(workers_per_source))) if workers_per_source is not None else "auto",
    ]
    env = os.environ.copy()
    env["SIGNALS_ENABLE_LIVE_CRAWL"] = "1" if live else "0"
    env["SIGNALS_VERBOSE_PROGRESS"] = "1"
    _apply_workers_per_source(env, workers_per_source)
    if extra_env:
        env.update(extra_env)
    _apply_fast_fail_network(env, enabled=fast_fail_network)

    mode = "live-crawl" if live else "non-live"
    typer.echo(
        f"mode={mode} date={run_date} live_max_accounts={live_max_accounts} "
        f"stage_timeout_seconds={stage_timeout_seconds} poll_interval_seconds={poll_interval_seconds} "
        f"workers_per_source={workers_per_source if workers_per_source is not None else 'auto'}"
    )
    typer.echo(f"command={' '.join(cmd)}")
    return _run_subprocess(cmd, env)


@app.command("start")
def start(
    run_date: str = typer.Option(None, "--date", help="Run date in YYYY-MM-DD. Defaults to today."),
    live_max_accounts: int = typer.Option(1000, "--live-max-accounts", help="Max accounts for live crawl."),
    workers_per_source: int | None = typer.Option(
        None,
        "--workers-per-source",
        min=1,
        help="Parallel live-crawl workers per source. Omit to use adaptive auto sizing.",
    ),
    stage_timeout_seconds: int = typer.Option(900, "--stage-timeout-seconds", help="Per-stage timeout seconds."),
    poll_interval_seconds: int = typer.Option(2, "--poll-interval-seconds", help="Monitor refresh interval seconds."),
    fast_fail_network: bool = typer.Option(
        False,
        "--fast-fail-network",
        help="Use short HTTP timeouts to move through large runs faster during operator checks.",
    ),
) -> None:
    """Single command for operators: real-time run with progress stream."""
    normalized_date = _normalize_date(run_date)
    code = _run_watch(
        run_date=normalized_date,
        live_max_accounts=live_max_accounts,
        stage_timeout_seconds=stage_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        workers_per_source=workers_per_source,
        live=True,
        fast_fail_network=fast_fail_network,
    )
    raise typer.Exit(code=code)


@app.command("watch")
def watch(
    run_date: str = typer.Option(None, "--date", help="Run date in YYYY-MM-DD. Defaults to today."),
    live: bool = typer.Option(True, "--live/--no-live", help="Enable/disable live crawl during monitored run."),
    live_max_accounts: int = typer.Option(1000, "--live-max-accounts", help="Max accounts for live crawl."),
    workers_per_source: int | None = typer.Option(
        None,
        "--workers-per-source",
        min=1,
        help="Parallel live-crawl workers per source. Omit to use adaptive auto sizing.",
    ),
    stage_timeout_seconds: int = typer.Option(900, "--stage-timeout-seconds", help="Per-stage timeout seconds."),
    poll_interval_seconds: int = typer.Option(2, "--poll-interval-seconds", help="Monitor refresh interval seconds."),
    fast_fail_network: bool = typer.Option(
        False,
        "--fast-fail-network",
        help="Use short HTTP timeouts to move through large runs faster during operator checks.",
    ),
) -> None:
    """Run with real-time monitoring in terminal."""
    normalized_date = _normalize_date(run_date)
    code = _run_watch(
        run_date=normalized_date,
        live_max_accounts=live_max_accounts,
        stage_timeout_seconds=stage_timeout_seconds,
        poll_interval_seconds=poll_interval_seconds,
        workers_per_source=workers_per_source,
        live=live,
        fast_fail_network=fast_fail_network,
    )
    raise typer.Exit(code=code)


@app.command("run")
def run_daily(
    run_date: str = typer.Option(None, "--date", help="Run date in YYYY-MM-DD. Defaults to today."),
    live: bool = typer.Option(False, "--live/--no-live", help="Enable/disable live crawl."),
    live_max_accounts: int | None = typer.Option(None, "--live-max-accounts", help="Override max live-crawl accounts."),
    workers_per_source: int | None = typer.Option(
        None,
        "--workers-per-source",
        min=1,
        help="Parallel live-crawl workers per source. Omit to use adaptive auto sizing.",
    ),
    stage_timeout_seconds: int | None = typer.Option(
        None, "--stage-timeout-seconds", help="Override per-stage timeout."
    ),
    fast_fail_network: bool = typer.Option(
        False,
        "--fast-fail-network",
        help="Use short HTTP timeouts to move through large runs faster during operator checks.",
    ),
) -> None:
    """Run daily pipeline (non-streaming)."""
    normalized_date = _normalize_date(run_date)
    cmd = [_python_bin(), "-m", "src.main", "run-daily", "--date", normalized_date]
    if live_max_accounts is not None:
        cmd.extend(["--live-max-accounts", str(max(1, int(live_max_accounts)))])
    if stage_timeout_seconds is not None:
        cmd.extend(["--stage-timeout-seconds", str(max(30, int(stage_timeout_seconds)))])

    env = os.environ.copy()
    env["SIGNALS_ENABLE_LIVE_CRAWL"] = "1" if live else "0"
    _apply_workers_per_source(env, workers_per_source)
    if live:
        env["SIGNALS_VERBOSE_PROGRESS"] = "1"
    _apply_fast_fail_network(env, enabled=fast_fail_network)

    mode = "live-crawl" if live else "non-live"
    typer.echo(
        f"mode={mode} date={normalized_date} "
        f"workers_per_source={workers_per_source if workers_per_source is not None else 'auto'}"
    )
    typer.echo(f"command={' '.join(cmd)}")
    raise typer.Exit(code=_run_subprocess(cmd, env))


@app.command("company")
def company(
    domain: str = typer.Argument(..., help="Company domain (example: example.com)."),
    run_date: str = typer.Option(None, "--date", help="Run date in YYYY-MM-DD. Defaults to today."),
    watch: bool = typer.Option(True, "--watch/--no-watch", help="Stream real-time progress in terminal."),
    workers_per_source: int | None = typer.Option(
        None,
        "--workers-per-source",
        min=1,
        help="Parallel live-crawl workers per source. Omit to use adaptive auto sizing.",
    ),
    stage_timeout_seconds: int = typer.Option(900, "--stage-timeout-seconds", help="Per-stage timeout seconds."),
    poll_interval_seconds: int = typer.Option(1, "--poll-interval-seconds", help="Monitor refresh interval seconds."),
    fast_fail_network: bool = typer.Option(
        False,
        "--fast-fail-network",
        help="Use short HTTP timeouts to move through large runs faster during operator checks.",
    ),
) -> None:
    """Run a targeted signal pass for one company domain."""
    normalized = normalize_domain(domain)
    if not normalized:
        raise typer.BadParameter(f"Invalid domain: {domain}")
    if normalized.endswith(".example"):
        raise typer.BadParameter("Template domains ending with .example are not allowed.")

    normalized_date = _normalize_date(run_date)
    extra_env = {"SIGNALS_LIVE_TARGET_DOMAINS": normalized}
    typer.echo(f"target_domain={normalized}")

    if watch:
        code = _run_watch(
            run_date=normalized_date,
            live_max_accounts=1,
            stage_timeout_seconds=stage_timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            workers_per_source=workers_per_source,
            live=True,
            fast_fail_network=fast_fail_network,
            extra_env=extra_env,
        )
        raise typer.Exit(code=code)

    cmd = [
        _python_bin(),
        "-m",
        "src.main",
        "run-daily",
        "--date",
        normalized_date,
        "--live-max-accounts",
        "1",
        "--stage-timeout-seconds",
        str(max(30, int(stage_timeout_seconds))),
    ]
    env = os.environ.copy()
    env.update(extra_env)
    env["SIGNALS_ENABLE_LIVE_CRAWL"] = "1"
    env["SIGNALS_VERBOSE_PROGRESS"] = "1"
    _apply_workers_per_source(env, workers_per_source)
    _apply_fast_fail_network(env, enabled=fast_fail_network)
    typer.echo(f"command={' '.join(cmd)}")
    raise typer.Exit(code=_run_subprocess(cmd, env))


@app.command("conviction")
def conviction(
    run_date: str = typer.Option(None, "--date", help="Run date in YYYY-MM-DD. Defaults to today."),
    top: int = typer.Option(10, "--top", help="Max companies to print."),
    min_tier: str = typer.Option("medium", "--min-tier", help="Minimum tier to include: high|medium|low."),
    new_only: bool = typer.Option(True, "--new-only/--all", help="Limit to newly discovered companies."),
    new_lookback_days: int = typer.Option(
        365,
        "--new-lookback-days",
        help="When --new-only is enabled, include discovered accounts created within this window.",
    ),
    domain: str = typer.Option("", "--domain", help="Optional domain filter (single company)."),
    product: str = typer.Option("", "--product", help="Optional product lane filter: zopdev|zopday|zopnight."),
    write_csv: bool = typer.Option(
        True,
        "--write-csv/--no-write-csv",
        help="Write conviction report CSV under data/out.",
    ),
) -> None:
    """Show scored companies with reason signals and evidence citations."""
    normalized_date = _normalize_date(run_date)
    min_tier_normalized = (min_tier or "medium").strip().lower()
    if min_tier_normalized not in {"high", "medium", "low"}:
        raise typer.BadParameter("min-tier must be one of: high, medium, low")
    product_normalized = (product or "").strip().lower()
    if product_normalized and product_normalized not in {"zopdev", "zopday", "zopnight"}:
        raise typer.BadParameter("product must be one of: zopdev, zopday, zopnight")
    domain_filter = normalize_domain(domain)
    if domain and not domain_filter:
        raise typer.BadParameter(f"Invalid domain filter: {domain}")

    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    try:
        run_id = db.get_latest_run_id_for_date(conn, normalized_date)
        if not run_id:
            raise typer.BadParameter(f"No score run found for date {normalized_date}")

        rows = conn.execute(
            """
            SELECT
                r.run_date,
                s.account_id,
                a.company_name,
                a.domain,
                a.source_type,
                a.created_at,
                s.product,
                s.score,
                s.tier,
                s.top_reasons_json
            FROM account_scores s
            JOIN accounts a ON a.account_id = s.account_id
            JOIN score_runs r ON r.run_id = s.run_id
            WHERE s.run_id = ?
            ORDER BY s.score DESC, a.company_name ASC
            """,
            (run_id,),
        ).fetchall()
    finally:
        conn.close()

    cutoff = date.fromisoformat(normalized_date) - timedelta(days=max(0, int(new_lookback_days)))
    min_tier_value = _tier_value(min_tier_normalized)

    filtered: list[dict[str, object]] = []
    for row in rows:
        tier = str(row["tier"] or "").strip().lower()
        if _tier_value(tier) < min_tier_value:
            continue
        row_product = str(row["product"] or "").strip().lower()
        if product_normalized and row_product != product_normalized:
            continue
        row_domain = normalize_domain(str(row["domain"] or ""))
        if domain_filter and row_domain != domain_filter:
            continue

        source_type = str(row["source_type"] or "").strip().lower()
        created_at_raw = str(row["created_at"] or "")
        if new_only:
            if source_type != "discovered":
                continue
            try:
                created_date = date.fromisoformat(created_at_raw[:10])
            except ValueError:
                continue
            if created_date < cutoff:
                continue

        reasons: list[dict[str, object]] = []
        raw_reasons = str(row["top_reasons_json"] or "").strip()
        if raw_reasons:
            try:
                parsed = json.loads(raw_reasons)
                if isinstance(parsed, list):
                    reasons = [item for item in parsed if isinstance(item, dict)]
            except json.JSONDecodeError:
                reasons = []

        citations: list[dict[str, str]] = []
        for reason in reasons[:3]:
            citations.append(
                {
                    "signal_code": str(reason.get("signal_code", "") or ""),
                    "source": str(reason.get("source", "") or ""),
                    "evidence_url": str(reason.get("evidence_url", "") or ""),
                    "evidence_sentence": str(
                        reason.get("evidence_sentence_en") or reason.get("evidence_sentence") or ""
                    )[:260],
                }
            )

        filtered.append(
            {
                "run_date": str(row["run_date"] or ""),
                "account_id": str(row["account_id"] or ""),
                "company_name": str(row["company_name"] or ""),
                "domain": str(row["domain"] or ""),
                "source_type": source_type,
                "created_at": created_at_raw,
                "product": row_product,
                "score": float(row["score"] or 0.0),
                "tier": tier,
                "citations": citations,
            }
        )

    selected = filtered[: max(1, int(top))]
    typer.echo(
        f"conviction run_date={normalized_date} run_id={run_id} "
        f"rows={len(selected)} filtered_total={len(filtered)} min_tier={min_tier_normalized} "
        f"new_only={int(new_only)} domain_filter={domain_filter or '-'}"
    )
    if not selected:
        typer.echo("No rows match the current filters.")
        raise typer.Exit(code=0)

    for idx, row in enumerate(selected, start=1):
        typer.echo(
            f"{idx}. {row['company_name']} ({row['domain']}) "
            f"product={row['product']} score={row['score']:.2f} tier={row['tier']}"
        )
        citations = list(row["citations"])  # type: ignore[arg-type]
        if not citations:
            typer.echo("   citation: none")
            continue
        for c_idx, citation in enumerate(citations, start=1):
            typer.echo(
                f"   citation_{c_idx}: signal={citation['signal_code']} source={citation['source']} "
                f"url={citation['evidence_url'] or '-'} text={citation['evidence_sentence'] or '-'}"
            )

    if write_csv:
        suffix = normalized_date.replace("-", "")
        out_path = settings.out_dir / f"conviction_report_{suffix}.csv"
        export_rows: list[dict[str, str | float]] = []
        for row in selected:
            citations = list(row["citations"])  # type: ignore[arg-type]
            citation_1 = citations[0] if len(citations) > 0 else {}
            citation_2 = citations[1] if len(citations) > 1 else {}
            citation_3 = citations[2] if len(citations) > 2 else {}
            export_rows.append(
                {
                    "run_date": str(row["run_date"]),
                    "company_name": str(row["company_name"]),
                    "domain": str(row["domain"]),
                    "source_type": str(row["source_type"]),
                    "created_at": str(row["created_at"]),
                    "product": str(row["product"]),
                    "score": float(row["score"]),
                    "tier": str(row["tier"]),
                    "citation_1_signal": str(citation_1.get("signal_code", "")),
                    "citation_1_source": str(citation_1.get("source", "")),
                    "citation_1_url": str(citation_1.get("evidence_url", "")),
                    "citation_1_text": str(citation_1.get("evidence_sentence", "")),
                    "citation_2_signal": str(citation_2.get("signal_code", "")),
                    "citation_2_source": str(citation_2.get("source", "")),
                    "citation_2_url": str(citation_2.get("evidence_url", "")),
                    "citation_2_text": str(citation_2.get("evidence_sentence", "")),
                    "citation_3_signal": str(citation_3.get("signal_code", "")),
                    "citation_3_source": str(citation_3.get("source", "")),
                    "citation_3_url": str(citation_3.get("evidence_url", "")),
                    "citation_3_text": str(citation_3.get("evidence_sentence", "")),
                }
            )

        write_csv_rows(
            out_path,
            export_rows,
            fieldnames=[
                "run_date",
                "company_name",
                "domain",
                "source_type",
                "created_at",
                "product",
                "score",
                "tier",
                "citation_1_signal",
                "citation_1_source",
                "citation_1_url",
                "citation_1_text",
                "citation_2_signal",
                "citation_2_source",
                "citation_2_url",
                "citation_2_text",
                "citation_3_signal",
                "citation_3_source",
                "citation_3_url",
                "citation_3_text",
            ],
        )
        typer.echo(f"wrote_csv={out_path}")


@app.command("sources")
def sources(
    run_date: str = typer.Option(None, "--date", help="Run date in YYYY-MM-DD. Defaults to today."),
    min_attempts: int = typer.Option(1, "--min-attempts", min=0, help="Hide sources below this attempt count."),
    write_csv: bool = typer.Option(
        True,
        "--write-csv/--no-write-csv",
        help="Write source depth report CSV under data/out.",
    ),
) -> None:
    """Show source depth, performance, and quality confidence for a run date."""
    normalized_date = _normalize_date(run_date)
    settings = load_settings()
    conn = db.get_connection(settings.pg_dsn)
    try:
        attempt_rows = db.fetch_crawl_attempt_summary(conn, normalized_date)
        quality_rows = db.fetch_source_metrics(conn, normalized_date)
        run_id = db.get_latest_run_id_for_date(conn, normalized_date)
        score_rows = db.fetch_scores_for_run(conn, run_id) if run_id else []
    finally:
        conn.close()

    reliability_map = load_source_registry(settings.source_registry_path)
    policy_map = load_source_execution_policy(settings.source_execution_policy_path)

    attempts_by_source: dict[str, dict[str, int]] = {}
    for row in attempt_rows:
        source = str(row["source"] or "").strip().lower() or "unknown"
        status = str(row["status"] or "").strip().lower() or "unknown"
        count = int(row["attempt_count"] or 0)
        source_counts = attempts_by_source.setdefault(
            source, {"attempts": 0, "success": 0, "http_error": 0, "exception": 0, "skipped": 0}
        )
        source_counts["attempts"] += count
        source_counts[status] = source_counts.get(status, 0) + count

    quality_by_source: dict[str, dict[str, float | int]] = {}
    for row in quality_rows:
        source = str(row["source"] or "").strip().lower()
        if not source:
            continue
        quality_by_source[source] = {
            "approved_rate": float(row["approved_rate"] or 0.0),
            "sample_size": int(row["sample_size"] or 0),
        }

    reason_stats: dict[str, dict[str, int]] = {}
    account_source_pairs: set[tuple[str, str]] = set()
    for row in score_rows:
        account_id = str(row["account_id"] or "")
        raw_reasons = str(row["top_reasons_json"] or "").strip()
        if not raw_reasons:
            continue
        try:
            parsed = json.loads(raw_reasons)
        except json.JSONDecodeError:
            continue
        if not isinstance(parsed, list):
            continue
        for reason in parsed:
            if not isinstance(reason, dict):
                continue
            source = str(reason.get("source", "") or "").strip().lower() or "unknown"
            source_reason = reason_stats.setdefault(
                source, {"reason_mentions": 0, "reason_with_url": 0, "account_mentions": 0}
            )
            source_reason["reason_mentions"] += 1
            evidence_url = str(reason.get("evidence_url", "") or "").strip()
            if evidence_url:
                source_reason["reason_with_url"] += 1
            if account_id and (account_id, source) not in account_source_pairs:
                account_source_pairs.add((account_id, source))
                source_reason["account_mentions"] += 1

    all_sources = sorted(
        set(reliability_map.keys())
        | set(policy_map.keys())
        | set(attempts_by_source.keys())
        | set(reason_stats.keys())
        | set(quality_by_source.keys())
    )

    report_rows: list[dict[str, str | int | float]] = []
    for source in all_sources:
        counts = attempts_by_source.get(source, {})
        attempts = int(counts.get("attempts", 0))
        if attempts < int(min_attempts) and source not in reason_stats and source not in quality_by_source:
            continue
        success = int(counts.get("success", 0))
        http_error = int(counts.get("http_error", 0))
        exception_count = int(counts.get("exception", 0))
        skipped = int(counts.get("skipped", 0))
        success_rate_pct = _safe_pct(success, attempts)

        reason = reason_stats.get(source, {})
        reason_mentions = int(reason.get("reason_mentions", 0))
        reason_with_url = int(reason.get("reason_with_url", 0))
        account_mentions = int(reason.get("account_mentions", 0))
        evidence_rate_pct = _safe_pct(reason_with_url, reason_mentions)

        reliability = float(reliability_map.get(source, 0.0))
        reliability_label = _source_reliability_label(reliability)

        quality = quality_by_source.get(source, {})
        approved_sample_size = int(quality.get("sample_size", 0) or 0)
        approved_rate: float | None = None
        approved_rate_str = "n/a"
        if approved_sample_size > 0:
            approved_rate = float(quality.get("approved_rate", 0.0) or 0.0)
            approved_rate_str = f"{round(approved_rate * 100.0, 1)}%"

        policy_key = _POLICY_SOURCE_ALIAS.get(source, source)
        policy = policy_map.get(policy_key)
        policy_workers = int(policy.max_parallel_workers) if policy is not None else 0
        policy_enabled = bool(policy.enabled) if policy is not None else True

        quality_score = round(
            (reliability * 40.0)
            + ((success_rate_pct / 100.0) * 35.0)
            + ((evidence_rate_pct / 100.0) * 15.0)
            + ((approved_rate if approved_rate is not None else 0.6) * 10.0),
            1,
        )

        note = _source_health_note(
            source=source,
            attempts=attempts,
            success_rate_pct=success_rate_pct,
            evidence_rate_pct=evidence_rate_pct,
            approved_rate=approved_rate,
            approved_sample_size=approved_sample_size,
            reliability=reliability,
        )

        report_rows.append(
            {
                "run_date": normalized_date,
                "source": source,
                "attempts": attempts,
                "success": success,
                "http_error": http_error,
                "exception": exception_count,
                "skipped": skipped,
                "success_rate_pct": success_rate_pct,
                "reliability": round(reliability, 2),
                "reliability_label": reliability_label,
                "reason_mentions": reason_mentions,
                "reason_with_url": reason_with_url,
                "evidence_rate_pct": evidence_rate_pct,
                "account_mentions": account_mentions,
                "approved_rate_pct": approved_rate_str,
                "approved_sample_size": approved_sample_size,
                "policy_source": policy_key,
                "policy_enabled": int(policy_enabled),
                "policy_max_workers": policy_workers,
                "quality_score": quality_score,
                "note": note,
            }
        )

    report_rows.sort(
        key=lambda row: (
            -float(row["attempts"]),
            -float(row["quality_score"]),
            str(row["source"]),
        )
    )

    total_attempts = sum(int(row["attempts"]) for row in report_rows)
    total_mentions = sum(int(row["reason_mentions"]) for row in report_rows)
    typer.echo(
        f"source_depth run_date={normalized_date} run_id={run_id or '-'} "
        f"sources={len(report_rows)} attempts={total_attempts} reason_mentions={total_mentions}"
    )
    if not report_rows:
        typer.echo("No source rows matched the selected filters.")
        raise typer.Exit(code=0)

    for row in report_rows:
        typer.echo(
            " ".join(
                [
                    f"source={row['source']}",
                    f"attempts={row['attempts']}",
                    f"success={row['success']}",
                    f"http_error={row['http_error']}",
                    f"exception={row['exception']}",
                    f"skipped={row['skipped']}",
                    f"success_rate={row['success_rate_pct']}%",
                    f"reliability={row['reliability']}({row['reliability_label']})",
                    f"policy_workers={row['policy_max_workers']}",
                    f"reason_mentions={row['reason_mentions']}",
                    f"evidence_rate={row['evidence_rate_pct']}%",
                    f"approved_rate={row['approved_rate_pct']}",
                    f"quality_score={row['quality_score']}",
                    f"note={str(row['note']).replace(' ', '_')}",
                ]
            )
        )

    weak_rows = [row for row in report_rows if float(row["quality_score"]) < 55.0]
    if weak_rows:
        typer.echo("improvement_candidates:")
        for row in weak_rows[:5]:
            typer.echo(f"- source={row['source']} quality_score={row['quality_score']} recommendation={row['note']}")

    if write_csv:
        suffix = normalized_date.replace("-", "")
        out_path = settings.out_dir / f"source_depth_{suffix}.csv"
        write_csv_rows(
            out_path,
            report_rows,
            fieldnames=[
                "run_date",
                "source",
                "attempts",
                "success",
                "http_error",
                "exception",
                "skipped",
                "success_rate_pct",
                "reliability",
                "reliability_label",
                "reason_mentions",
                "reason_with_url",
                "evidence_rate_pct",
                "account_mentions",
                "approved_rate_pct",
                "approved_sample_size",
                "policy_source",
                "policy_enabled",
                "policy_max_workers",
                "quality_score",
                "note",
            ],
        )
        typer.echo(f"wrote_csv={out_path}")


@app.command("hunt")
def hunt(
    domain: str = typer.Argument(..., help="Company domain (example: example.com)."),
    run_date: str = typer.Option(None, "--date", help="Run date in YYYY-MM-DD. Defaults to today."),
    top: int = typer.Option(5, "--top", help="How many score rows to show for the target company."),
    workers_per_source: int | None = typer.Option(
        None,
        "--workers-per-source",
        min=1,
        help="Parallel live-crawl workers per source. Omit to use adaptive auto sizing.",
    ),
    fast_fail_network: bool = typer.Option(
        True,
        "--fast-fail-network/--no-fast-fail-network",
        help="Use short HTTP timeouts and no per-domain sleep for faster single-company checks.",
    ),
) -> None:
    """Fast single-company hunt: ingest live signals, score, then print citations."""
    normalized = normalize_domain(domain)
    if not normalized:
        raise typer.BadParameter(f"Invalid domain: {domain}")
    if normalized.endswith(".example"):
        raise typer.BadParameter("Template domains ending with .example are not allowed.")

    normalized_date = _normalize_date(run_date)
    env = os.environ.copy()
    env["SIGNALS_ENABLE_LIVE_CRAWL"] = "1"
    env["SIGNALS_VERBOSE_PROGRESS"] = "1"
    env["SIGNALS_LIVE_MAX_ACCOUNTS"] = "1"
    env["SIGNALS_LIVE_TARGET_DOMAINS"] = normalized
    _apply_workers_per_source(env, workers_per_source)
    _apply_fast_fail_network(env, enabled=fast_fail_network)

    ingest_cmd = [_python_bin(), "-m", "src.main", "ingest", "--all"]
    score_cmd = [_python_bin(), "-m", "src.main", "score", "--date", normalized_date]
    conviction_cmd = [
        _python_bin(),
        "-m",
        "src.cli",
        "conviction",
        "--date",
        normalized_date,
        "--domain",
        normalized,
        "--all",
        "--min-tier",
        "low",
        "--top",
        str(max(1, int(top))),
        "--write-csv",
    ]

    typer.echo(f"target_domain={normalized}")
    typer.echo(f"step=ingest command={' '.join(ingest_cmd)}")
    ingest_code = _run_subprocess(ingest_cmd, env)
    if ingest_code != 0:
        raise typer.Exit(code=ingest_code)

    typer.echo(f"step=score command={' '.join(score_cmd)}")
    score_code = _run_subprocess(score_cmd, env)
    if score_code != 0:
        raise typer.Exit(code=score_code)

    typer.echo(f"step=conviction command={' '.join(conviction_cmd)}")
    conviction_code = _run_subprocess(conviction_cmd, env)
    raise typer.Exit(code=conviction_code)


@app.command("ui")
def ui(
    host: str = typer.Option("127.0.0.1", "--host", help="UI bind host."),
    port: int = typer.Option(8788, "--port", help="UI bind port."),
    log_level: str = typer.Option("info", "--log-level", help="Uvicorn log level."),
) -> None:
    """Start local web UI."""
    cmd = [
        _python_bin(),
        "-m",
        "src.main",
        "serve-local-ui",
        "--host",
        host,
        "--port",
        str(max(1, int(port))),
        "--log-level",
        log_level,
    ]
    env = os.environ.copy()
    typer.echo(f"command={' '.join(cmd)}")
    raise typer.Exit(code=_run_subprocess(cmd, env))


def main() -> None:
    app()


if __name__ == "__main__":
    main()
