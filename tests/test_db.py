import json
from datetime import datetime, timezone
from pathlib import Path

from src import db
from src.models import SignalObservation
from src.utils import stable_hash


def test_signal_observation_dedupe(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)

    account_id = db.upsert_account(conn, company_name="Acme", domain="acme.example", source_type="seed")
    payload = {"k": "v"}
    raw_hash = stable_hash(payload, prefix="raw")

    observation = SignalObservation(
        obs_id=stable_hash({"x": 1}, prefix="obs"),
        account_id=account_id,
        signal_code="devops_role_open",
        product="shared",
        source="jobs_csv",
        observed_at="2026-02-16T00:00:00Z",
        evidence_url="",
        evidence_text="devops engineer",
        confidence=0.7,
        source_reliability=0.75,
        raw_payload_hash=raw_hash,
    )

    first = db.insert_signal_observation(conn, observation)
    second = db.insert_signal_observation(conn, observation)

    assert first is True
    assert second is False


def test_crawl_attempt_summary_and_failures(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)

    today = datetime.now(timezone.utc).date().isoformat()
    db.record_crawl_attempt(
        conn,
        source="google_news_rss",
        account_id="acc_1",
        endpoint="https://example.com/rss",
        status="success",
        error_summary="",
    )
    db.record_crawl_attempt(
        conn,
        source="google_news_rss",
        account_id="acc_1",
        endpoint="https://example.com/rss",
        status="exception",
        error_summary="timeout",
    )

    summary_rows = db.fetch_crawl_attempt_summary(conn, today)
    by_status = {str(row["status"]): int(row["attempt_count"]) for row in summary_rows}
    assert by_status["success"] == 1
    assert by_status["exception"] == 1

    failures = db.fetch_latest_crawl_failures(conn, today, limit=5)
    assert len(failures) == 1
    assert str(failures[0]["status"]) == "exception"
    assert str(failures[0]["error_summary"]) == "timeout"


def test_select_accounts_for_live_crawl_rotates_by_last_attempt(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)

    acc_a = db.upsert_account(conn, company_name="Acme", domain="acme.com", source_type="seed")
    acc_b = db.upsert_account(conn, company_name="Beta", domain="beta.com", source_type="seed")
    acc_c = db.upsert_account(conn, company_name="Core", domain="core.com", source_type="seed")

    source = "google_news_rss"
    conn.execute(
        """
        INSERT INTO crawl_attempts (source, account_id, endpoint, attempted_at, status, error_summary)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source, acc_a, "https://a.example/rss", "2026-02-21T10:00:00+00:00", "success", ""),
    )
    conn.execute(
        """
        INSERT INTO crawl_attempts (source, account_id, endpoint, attempted_at, status, error_summary)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (source, acc_b, "https://b.example/rss", "2026-02-20T10:00:00+00:00", "success", ""),
    )
    conn.commit()

    ordered = db.select_accounts_for_live_crawl(conn, source=source, limit=3)
    ordered_ids = [str(row["account_id"]) for row in ordered]

    # Unseen accounts should be crawled first, then oldest-attempted accounts.
    assert ordered_ids[0] == acc_c
    assert ordered_ids[1] == acc_b
    assert ordered_ids[2] == acc_a


def test_select_accounts_for_live_crawl_excludes_example_domains(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)

    ignored = db.upsert_account(conn, company_name="Template", domain="template.example", source_type="seed")
    included = db.upsert_account(conn, company_name="Acme", domain="acme.com", source_type="seed")

    ordered = db.select_accounts_for_live_crawl(conn, source="careers_live", limit=10)
    ordered_ids = {str(row["account_id"]) for row in ordered}

    assert included in ordered_ids
    assert ignored not in ordered_ids


def test_select_accounts_for_live_crawl_respects_include_domains(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)

    chosen = db.upsert_account(conn, company_name="Chosen", domain="chosen.com", source_type="seed")
    other = db.upsert_account(conn, company_name="Other", domain="other.com", source_type="seed")

    ordered = db.select_accounts_for_live_crawl(
        conn,
        source="google_news_rss",
        limit=10,
        include_domains=["chosen.com"],
    )
    ordered_ids = {str(row["account_id"]) for row in ordered}

    assert chosen in ordered_ids
    assert other not in ordered_ids


def test_advisory_lock_single_flight_behavior(tmp_path: Path):
    conn1 = db.get_connection()
    db.init_db(conn1)
    conn2 = db.get_connection()
    db.init_db(conn2)
    lock_name = "signals:test-lock"

    acquired_1 = db.try_advisory_lock(conn1, lock_name=lock_name, owner_id="owner-1")
    acquired_2 = db.try_advisory_lock(conn2, lock_name=lock_name, owner_id="owner-2")
    assert acquired_1 is True
    assert acquired_2 is False

    assert db.release_advisory_lock(conn1, lock_name=lock_name, owner_id="owner-1") is True
    acquired_2_after_release = db.try_advisory_lock(conn2, lock_name=lock_name, owner_id="owner-2")
    assert acquired_2_after_release is True
    assert db.release_advisory_lock(conn2, lock_name=lock_name, owner_id="owner-2") is True

    conn1.close()
    conn2.close()


def test_retry_queue_quarantine_lifecycle(tmp_path: Path):
    conn = db.get_connection()
    db.init_db(conn)
    task_id = db.enqueue_retry_task(
        conn,
        task_type="ingest_cycle",
        payload_json=json.dumps({"run_date": "2026-02-17"}, ensure_ascii=True),
        due_at="2026-02-17T00:00:00+00:00",
        max_attempts=3,
    )

    due = db.fetch_due_retry_tasks(conn, limit=10, now_iso="2026-02-17T01:00:00+00:00")
    due_ids = {str(row["task_id"]) for row in due}
    assert task_id in due_ids

    db.mark_retry_task_running(conn, task_id)
    db.reschedule_retry_task(
        conn,
        task_id=task_id,
        attempt_count=1,
        due_at="2026-02-17T02:00:00+00:00",
        error_summary="temporary failure",
    )
    assert db.fetch_retry_depth(conn) == 1
    assert db.fetch_retry_queue_size(conn) == 1

    db.quarantine_retry_task(
        conn,
        task_id=task_id,
        task_type="ingest_cycle",
        payload_json=json.dumps({"run_date": "2026-02-17"}, ensure_ascii=True),
        attempt_count=3,
        error_summary="hard failure",
    )
    assert db.fetch_retry_queue_size(conn) == 0
    assert db.fetch_quarantine_size(conn) == 1
    conn.close()
