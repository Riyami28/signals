#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

TEST_ROOT="${SIGNALS_TEST_ROOT:-/tmp/signals_smallset_autonomous}"
WEBHOOK_HOST="127.0.0.1"
WEBHOOK_PORT="${SIGNALS_TEST_WEBHOOK_PORT:-8791}"
WEBHOOK_TOKEN="${SIGNALS_TEST_WEBHOOK_TOKEN:-smallset-token}"
RUN_DATE="${SIGNALS_TEST_RUN_DATE:-2026-02-17}"

rm -rf "$TEST_ROOT"
mkdir -p "$TEST_ROOT/config" "$TEST_ROOT/data/raw" "$TEST_ROOT/data/out"

# Copy baseline configs and override small-set files.
cp config/signal_registry.csv "$TEST_ROOT/config/"
cp config/source_registry.csv "$TEST_ROOT/config/"
cp config/thresholds.csv "$TEST_ROOT/config/"
cp config/keyword_lexicon.csv "$TEST_ROOT/config/"
cp config/signal_classes.csv "$TEST_ROOT/config/"
cp config/account_profiles.csv "$TEST_ROOT/config/"
cp config/discovery_thresholds.csv "$TEST_ROOT/config/"
cp config/discovery_blocklist.csv "$TEST_ROOT/config/"
cp config/profile_scenarios.csv "$TEST_ROOT/config/"
cp config/icp_signal_playbook.csv "$TEST_ROOT/config/"
cp config/source_execution_policy.csv "$TEST_ROOT/config/"
cp config/signal_universe_stackrank.csv "$TEST_ROOT/config/"

cat > "$TEST_ROOT/config/seed_accounts.csv" <<'CSV'
company_name,domain,source_type
Zopdev,zop.dev,seed
PepsiCo,pepsico.com,seed
Unilever,unilever.com,seed
Colgate-Palmolive Company,colgatepalmolive.com,seed
Mondelez International,mondelezinternational.com,seed
Nestle,nestle.com,seed
Diageo,diageo.com,seed
Procter & Gamble,pg.com,seed
The Coca-Cola Company,coca-colacompany.com,seed
CSV

cat > "$TEST_ROOT/config/watchlist_accounts.csv" <<'CSV'
company_name,domain,source_type,country,region_group,industry_label,website_url,wikidata_id,sitelinks,revenue_usd,employees,ranking_score,data_source,last_refreshed_on
PepsiCo,pepsico.com,seed,United States,us,fast-moving consumer goods,https://www.pepsico.com,Q334800,65,91854000000,309000,61.86,wikidata,2026-02-17
Unilever,unilever.com,seed,United Kingdom,uk,fast-moving consumer goods,https://www.unilever.com,Q157062,65,60073000000,148000,59.59,wikidata,2026-02-17
Colgate-Palmolive Company,colgatepalmolive.com,seed,United States,us,personal care product,https://www.colgatepalmolive.com,Q609466,46,17967000000,33000,49.38,wikidata,2026-02-17
Mondelez International,mondelezinternational.com,seed,United States,us,food industry,https://www.mondelezinternational.com,Q12857502,36,31496000000,91000,48.02,wikidata,2026-02-17
Nestle,nestle.com,seed,Switzerland,europe,food industry,https://www.nestle.com,Q160746,90,91354000000,270000,53.28,wikidata,2026-02-17
Diageo,diageo.com,seed,United Kingdom,uk,beverage industry,https://www.diageo.com,Q161140,40,28270000000,30000,53.02,wikidata,2026-02-17
Procter & Gamble,pg.com,seed,United States,us,fast-moving consumer goods,https://www.pg.com,Q212405,62,84284000000,105000,60.90,wikidata,2026-02-17
The Coca-Cola Company,coca-colacompany.com,seed,United States,us,fast-moving consumer goods,https://www.coca-colacompany.com,Q3295867,73,47061000000,69700,61.08,wikidata,2026-02-17
CSV

cat > "$TEST_ROOT/config/icp_reference_accounts.csv" <<'CSV'
company_name,domain,relationship_stage,notes
PepsiCo,pepsico.com,customer,customer
Unilever,unilever.com,customer,customer
Colgate-Palmolive Company,colgatepalmolive.com,customer,customer
Mondelez International,mondelezinternational.com,poc,poc
Nestle,nestle.com,poc,poc
Diageo,diageo.com,customer,customer
Procter & Gamble,pg.com,customer,customer
The Coca-Cola Company,coca-colacompany.com,customer,customer
CSV

cat > "$TEST_ROOT/config/account_profiles.csv" <<'CSV'
domain,relationship_stage,vertical_tag,is_self,exclude_from_crm
zop.dev,customer,internal,1,1
pepsico.com,customer,cpg,0,0
unilever.com,customer,cpg,0,0
colgatepalmolive.com,customer,cpg,0,0
mondelezinternational.com,poc,cpg,0,0
nestle.com,poc,cpg,0,0
diageo.com,customer,cpg,0,0
pg.com,customer,cpg,0,0
coca-colacompany.com,customer,cpg,0,0
CSV

cat > "$TEST_ROOT/config/discovery_blocklist.csv" <<'CSV'
domain,reason
zop.dev,self_domain
example.com,placeholder
example.org,placeholder
example.net,placeholder
CSV

cat > "$TEST_ROOT/config/account_source_handles.csv" <<'CSV'
domain,company_name,greenhouse_board,lever_company,careers_url,website_url,news_query,news_rss,reddit_query
pepsico.com,PepsiCo,,,,https://www.pepsico.com,"PepsiCo SAP S/4HANA rollout OR control tower OR cost transformation office",,"PepsiCo procurement opened OR security review started"
unilever.com,Unilever,,,,https://www.unilever.com,"Unilever margin improvement program OR vendor consolidation OR audit readiness",,"Unilever policy enforcement OR go-live date set"
colgatepalmolive.com,Colgate-Palmolive Company,,,,https://www.colgatepalmolive.com,"Colgate policy enforcement OR risk controls OR audit readiness",,"Colgate cloud cost takeout"
mondelezinternational.com,Mondelez International,,,,https://www.mondelezinternational.com,"Mondelez demand planning platform OR warehouse digitization OR procurement opened",,"Mondelez pilot expanded OR success criteria signed"
nestle.com,Nestle,,,,https://www.nestle.com,"Nestle control tower OR cost transformation office OR governance",,"Nestle security review started"
diageo.com,Diageo,,,,https://www.diageo.com,"Diageo ERP modernization phase-2 OR vendor consolidation",,"Diageo go-live date set"
pg.com,Procter & Gamble,,,,https://www.pg.com,"P&G policy enforcement OR compliance governance",,"P&G risk controls"
coca-colacompany.com,The Coca-Cola Company,,,,https://www.coca-colacompany.com,"Coca-Cola demand planning platform OR control tower",,"Coca-Cola procurement opened"
zop.dev,Zopdev,,,,https://zop.dev,"zop.dev internal",,"zop.dev"
CSV

# Minimal raw files used by collectors.
cat > "$TEST_ROOT/data/raw/first_party_events.csv" <<'CSV'
company_name,domain,product,signal_code,source,evidence_url,evidence_text,confidence,observed_at
Mondelez International,mondelezinternational.com,shared,poc_stage_progression,first_party_csv,internal://crm/mondelez/security-review,security review started and success criteria signed,0.95,2026-02-17T08:00:00Z
CSV

cat > "$TEST_ROOT/data/raw/news.csv" <<'CSV'
company_name,domain,title,content,url,observed_at,signal_code,confidence
CSV

cat > "$TEST_ROOT/data/raw/jobs.csv" <<'CSV'
company_name,domain,title,description,url,observed_at,signal_code,confidence
CSV

cat > "$TEST_ROOT/data/raw/community.csv" <<'CSV'
company_name,domain,text,url,observed_at,signal_code,confidence
CSV

cat > "$TEST_ROOT/data/raw/technographics.csv" <<'CSV'
company_name,domain,text,url,observed_at,signal_code,confidence
CSV

cat > "$TEST_ROOT/data/raw/news_feeds.csv" <<'CSV'
company_name,domain,feed_url
CSV

cat > "$TEST_ROOT/data/raw/review_input.csv" <<'CSV'
run_date,account_id,decision,reviewer,notes
CSV

source .venv/bin/activate
export SIGNALS_PROJECT_ROOT="$TEST_ROOT"
export SIGNALS_PG_DSN="${SIGNALS_TEST_PG_DSN:-postgresql://signals:signals_dev_password@127.0.0.1:55432/signals}"
export SIGNALS_DISCOVERY_WEBHOOK_TOKEN="$WEBHOOK_TOKEN"
export SIGNALS_ENABLE_LIVE_CRAWL=0

python - <<'PY'
from src import db

conn = db.get_connection()
db.init_db(conn)
for table in (
    "people_activity",
    "people_watchlist",
    "observation_lineage",
    "document_mentions",
    "documents",
    "crawl_frontier",
    "discovery_evidence",
    "discovery_candidates",
    "discovery_runs",
    "external_discovery_events",
    "crawl_attempts",
    "crawl_checkpoints",
    "source_metrics",
    "review_labels",
    "account_scores",
    "score_components",
    "score_runs",
    "accounts",
):
    conn.execute(f"DELETE FROM {table}")
conn.commit()
conn.close()
PY

python -m src.main serve-discovery-webhook --host "$WEBHOOK_HOST" --port "$WEBHOOK_PORT" --log-level warning > "$TEST_ROOT/webhook.log" 2>&1 &
WEBHOOK_PID=$!
cleanup() {
  kill "$WEBHOOK_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM
sleep 2

WEBHOOK_PORT="$WEBHOOK_PORT" WEBHOOK_TOKEN="$WEBHOOK_TOKEN" TEST_ROOT="$TEST_ROOT" python - <<'PY'
import json
import os
import requests
from pathlib import Path

port = os.environ["WEBHOOK_PORT"]
token = os.environ["WEBHOOK_TOKEN"]
test_root = Path(os.environ["TEST_ROOT"])
url = f"http://127.0.0.1:{port}/v1/discovery/events"
headers = {"X-Discovery-Token": token}

events = [
    {
        "source": "huginn_webhook",
        "source_event_id": "evt-pepsico-1",
        "observed_at": "2026-02-17T09:00:00Z",
        "title": "PepsiCo rollout update",
        "text": "SAP S/4HANA rollout with ECC sunset, supply chain control tower, cost transformation office, and procurement opened for implementation.",
        "url": "https://www.pepsico.com/news",
        "company_name_hint": "PepsiCo",
        "domain_hint": "pepsico.com",
        "raw_payload": {"channel": "huginn"},
    },
    {
        "source": "huginn_webhook",
        "source_event_id": "evt-unilever-1",
        "observed_at": "2026-02-17T09:05:00Z",
        "title": "Unilever operations program",
        "text": "margin improvement program, vendor consolidation, policy enforcement, audit readiness and go-live date set for rollout teams.",
        "url": "https://www.unilever.com/news",
        "company_name_hint": "Unilever",
        "domain_hint": "unilever.com",
        "raw_payload": {"channel": "huginn"},
    },
    {
        "source": "huginn_webhook",
        "source_event_id": "evt-colgate-1",
        "observed_at": "2026-02-17T09:10:00Z",
        "title": "Colgate governance update",
        "text": "policy enforcement, risk controls, audit readiness and data governance program announced with compliance messaging.",
        "url": "https://www.colgatepalmolive.com/en-us/news",
        "company_name_hint": "Colgate-Palmolive Company",
        "domain_hint": "colgatepalmolive.com",
        "raw_payload": {"channel": "huginn"},
    },
    {
        "source": "huginn_webhook",
        "source_event_id": "evt-mondelez-1",
        "observed_at": "2026-02-17T09:15:00Z",
        "title": "Mondelez supply chain modernization",
        "text": "demand planning platform, warehouse digitization, procurement opened, security review started, success criteria signed and pilot expanded.",
        "url": "https://www.mondelezinternational.com/news",
        "company_name_hint": "Mondelez International",
        "domain_hint": "mondelezinternational.com",
        "raw_payload": {"channel": "huginn"},
    },
    {
        "source": "huginn_webhook",
        "source_event_id": "evt-nestle-1",
        "observed_at": "2026-02-17T09:20:00Z",
        "title": "Nestle modernization signal",
        "text": "ERP modernization phase-2, control tower platform, cost reduction mandate and governance enforcement need across regions.",
        "url": "https://www.nestle.com/media/news",
        "company_name_hint": "Nestle",
        "domain_hint": "nestle.com",
        "raw_payload": {"channel": "huginn"},
    },
    {
        "source": "huginn_webhook",
        "source_event_id": "evt-zop-1",
        "observed_at": "2026-02-17T09:30:00Z",
        "title": "Internal Zop update",
        "text": "cost transformation office and policy enforcement roadmap",
        "url": "https://zop.dev/blog",
        "company_name_hint": "Zopdev",
        "domain_hint": "zop.dev",
        "raw_payload": {"channel": "huginn"},
    },
    {
        "source": "huginn_webhook",
        "source_event_id": "evt-noise-1",
        "observed_at": "2026-02-17T09:40:00Z",
        "title": "TechInfra hiring",
        "text": "kubernetes detected in stack and devops role open with platform role open",
        "url": "https://techinfralabs.com/careers",
        "company_name_hint": "TechInfra Labs",
        "domain_hint": "techinfralabs.com",
        "raw_payload": {"channel": "huginn"},
    },
]

results = []

for event in events:
    r = requests.post(url, json=event, headers=headers, timeout=30)
    body = r.json()
    print(event["source_event_id"], r.status_code, body)
    results.append(
        {
            "source_event_id": event["source_event_id"],
            "domain_hint": event["domain_hint"],
            "status_code": r.status_code,
            "accepted": int(body.get("accepted", 0)),
            "inserted": int(body.get("inserted", 0)),
        }
    )

# Duplicate check
r = requests.post(url, json=events[0], headers=headers, timeout=30)
body = r.json()
print("duplicate_evt_pepsico_1", r.status_code, body)
results.append(
    {
        "source_event_id": "duplicate_evt_pepsico_1",
        "domain_hint": events[0]["domain_hint"],
        "status_code": r.status_code,
        "accepted": int(body.get("accepted", 0)),
        "inserted": int(body.get("inserted", 0)),
    }
)

(test_root / "webhook_results.json").write_text(json.dumps(results, indent=2), encoding="utf-8")
PY

python -m src.main run-autonomous-loop --once --ingest-interval-minutes 15 --score-interval-minutes 60 --discovery-interval-minutes 180

TEST_ROOT="$TEST_ROOT" RUN_DATE="$RUN_DATE" python - <<'PY'
import csv
import os
import json
from collections import Counter
from pathlib import Path

from src import db

root = Path(os.environ["TEST_ROOT"])
run_date = os.environ["RUN_DATE"].replace("-", "")
queue = root / "data" / "out" / f"discovery_queue_{run_date}.csv"
crm = root / "data" / "out" / f"crm_candidates_{run_date}.csv"
metrics = root / "data" / "out" / f"discovery_metrics_{run_date}.csv"
daily = root / "data" / "out" / f"daily_scores_{run_date}.csv"
webhook_results_path = root / "webhook_results.json"

queue_rows = list(csv.DictReader(queue.open())) if queue.exists() else []
crm_rows = list(csv.DictReader(crm.open())) if crm.exists() else []
metrics_rows = list(csv.DictReader(metrics.open())) if metrics.exists() else []
daily_rows = list(csv.DictReader(daily.open())) if daily.exists() else []
webhook_results = json.loads(webhook_results_path.read_text(encoding="utf-8")) if webhook_results_path.exists() else []

print("=== summary ===")
print("queue_rows", len(queue_rows))
print("crm_rows", len(crm_rows))
print("metrics", {r['metric']: r['value'] for r in metrics_rows})
print("webhook_attempted", len(webhook_results))
print("webhook_accepted", sum(int(r.get("accepted", 0)) for r in webhook_results))
print("webhook_inserted", sum(int(r.get("inserted", 0)) for r in webhook_results))

print("=== webhook event results ===")
for row in webhook_results:
    print(
        row.get("source_event_id", ""),
        "domain=", row.get("domain_hint", ""),
        "status=", row.get("status_code", ""),
        "accepted=", row.get("accepted", ""),
        "inserted=", row.get("inserted", ""),
    )

bands = Counter(r.get("confidence_band", "") for r in queue_rows)
print("confidence_split", dict(bands))

print("=== queue domains ===")
for row in queue_rows:
    print(
        row.get("domain", ""),
        row.get("tier", ""),
        row.get("confidence_band", ""),
        "eligible=", row.get("eligible_for_crm", ""),
        "signals=", row.get("top_signals", ""),
    )

print("=== crm domains ===")
for row in crm_rows:
    print(row.get("domain", ""), row.get("tier", ""), row.get("confidence_band", ""))

print("=== daily tiers (best product rows) ===")
best = {}
for row in daily_rows:
    domain = row.get("domain", "")
    score = float(row.get("score", "0") or 0)
    prev = best.get(domain)
    if prev is None or score > prev[0]:
        best[domain] = (score, row.get("tier", ""), row.get("product", ""))
for domain, (score, tier, product) in sorted(best.items(), key=lambda x: x[1][0], reverse=True):
    print(domain, round(score, 2), tier, product)

print("=== discovery events persisted ===")
conn = db.get_connection()
event_rows = conn.execute(
    """
    SELECT source_event_id, domain_hint, processing_status, error_summary
    FROM external_discovery_events
    ORDER BY event_id ASC
    """
).fetchall()
for row in event_rows:
    print(
        row["source_event_id"],
        "domain=", row["domain_hint"],
        "status=", row["processing_status"],
        "error=", row["error_summary"],
    )

print("=== inserted signal observations by target (huginn_webhook) ===")
target_domains = (
    "pepsico.com",
    "unilever.com",
    "colgatepalmolive.com",
    "mondelezinternational.com",
    "nestle.com",
)
signal_rows = conn.execute(
    """
    SELECT a.domain, so.signal_code, COUNT(*) AS signal_count
    FROM signal_observations so
    JOIN accounts a ON a.account_id = so.account_id
    WHERE so.source = 'huginn_webhook'
      AND a.domain IN (?, ?, ?, ?, ?)
    GROUP BY a.domain, so.signal_code
    ORDER BY a.domain ASC, signal_count DESC, so.signal_code ASC
    """,
    target_domains,
).fetchall()

for row in signal_rows:
    print(row["domain"], row["signal_code"], row["signal_count"])

print("=== inserted signal totals by target and source ===")
total_rows = conn.execute(
    """
    SELECT a.domain, so.source, COUNT(*) AS total
    FROM signal_observations so
    JOIN accounts a ON a.account_id = so.account_id
    WHERE a.domain IN (?, ?, ?, ?, ?)
    GROUP BY a.domain, so.source
    ORDER BY a.domain ASC, so.source ASC
    """,
    target_domains,
).fetchall()
for row in total_rows:
    print(row["domain"], row["source"], row["total"])
conn.close()
PY

echo "Small-set test root: $TEST_ROOT"
echo "Webhook log: $TEST_ROOT/webhook.log"
