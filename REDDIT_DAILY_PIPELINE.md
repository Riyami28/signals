# Daily Reddit Official Subreddit Collection

## Overview

The signals pipeline now includes **reddit_official** collector that automatically fetches posts from official company subreddits every day. This provides high-quality signals directly from companies' official Reddit communities.

## How It Works

### Daily Pipeline Flow

When you run the daily pipeline, it executes these collectors in parallel:

```
1. INGEST STAGE (parallel collectors):
   ├── jobs_pages        → Career page job postings
   ├── news_rss          → News articles & RSS feeds
   ├── technographics    → Tech stack detection from websites
   ├── reddit_api        → General Reddit mentions (existing)
   ├── reddit_official   → Official company subreddit posts (NEW)
   ├── first_party_csv   → Direct company data imports
   ├── website_techscan  → Website technology scan
   └── crunchbase        → Firmographic enrichment

2. SCORE STAGE → Calculate account scores based on signals
3. EXPORT STAGE → Generate review queues and reports
4. SYNC STAGE → Upload to Google Sheets
```

### Reddit Official Collector Configuration

**Execution Policy** (`config/source_execution_policy.csv`):
```
source,max_parallel_workers,requests_per_second,timeout_seconds,retry_attempts,backoff_seconds,batch_size,enabled
reddit_official,4,0.5,20,2,5,50,true
```

- **Workers**: 4 (conservative to avoid rate limits)
- **Rate**: 0.5 requests/second (very respectful to Reddit)
- **Timeout**: 20 seconds per request
- **Retries**: 2 attempts with exponential backoff
- **Enabled**: true (runs every day)

**Source Reliability** (`config/source_registry.csv`):
```
reddit_official,0.75,true
```

- **Reliability**: 0.75 (higher than general reddit_api which is 0.65)
- **Enabled**: true

## Running the Daily Pipeline

### Option 1: Web UI (Recommended)

```bash
# Start the web UI
./signals ui --port 8788

# Click "Run Full Pipeline" button in the dashboard
# The pipeline will run with all collectors including reddit_official
```

### Option 2: Command Line

```bash
# Run the daily pipeline
./signals start

# Or run specific pipeline stages:
./signals run --date 2026-03-04  # Ingest + score + export for specific date
```

### Option 3: Scheduled Daily Run (Cron)

```bash
# Add to crontab to run every day at 2 AM
0 2 * * * cd /Users/zopdec/signals && source venv/bin/activate && ./signals start

# Check scheduled runs:
crontab -l
```

## What Gets Collected

### Official Company Subreddits

The collector automatically searches for and collects from these official subreddits:

| Company | Subreddit | Subscribers | Status |
|---------|-----------|-------------|--------|
| Notion Labs | r/notion | 445,410 | ✓ Highest value |
| GitHub Inc | r/github | 181,054 | ✓ Highest value |
| Instacart | r/instacart | 80,590 | ✓ High value |
| Virgin Group | r/virgin | 53,374 | ✓ High value |
| Stripe Inc | r/stripe | 23,966 | ✓ Medium value |
| Figma Inc | r/figma | 12,881 | ✓ Medium value |
| Cirkul | r/cirkul | 11,705 | ✓ Medium value |
| Prose Beauty | r/prose | 2,328 | ✓ Active |
| Datadog Inc | r/datadog | 1,856 | ✓ Active |
| ... and others | ... | ... | ✓ Active |

### Signal Types Generated

Posts from official subreddits are classified into:

1. **Keyword-matched signals** (high confidence):
   - `kubernetes_detected` - Infrastructure topics
   - `terraform_detected` - IaC discussions
   - `high_intent_phrase_*` - Business intent signals
   - etc. (from keyword lexicon)

2. **Community mention** (fallback, 0.7 confidence):
   - General company discussion in official subreddit
   - Used when post doesn't match specific keywords

### Example Signals

```
Signal: community_mention
Source: reddit_official (reliability: 0.75)
Company: Stripe Inc
Date: 2026-03-03
Text: "How to integrate Stripe API with Kubernetes cluster..."
Link: https://reddit.com/r/stripe/comments/abc123/...
Confidence: 0.7
```

## Daily Schedule

The pipeline runs on your configured timezone (default: America/Los_Angeles):

```
SIGNALS_RUN_TIMEZONE=America/Los_Angeles
```

Configure in `.env`:
```bash
SIGNALS_RUN_TIMEZONE=America/Los_Angeles    # or your preferred timezone
```

## Monitoring Collection

### View Collected Posts in Dashboard

1. **Open dashboard**: http://localhost:8788
2. **Search company** (e.g., "Notion", "Stripe")
3. **Click company row** → Detail panel opens
4. **Click "Signals" tab** → See all signals including reddit_official
5. **Filter by source**: Look for "reddit_official" badge

### Check Pipeline Logs

```bash
# View last 100 lines of pipeline logs
tail -100 /var/log/signals/pipeline.log

# Or during pipeline run, watch real-time logs:
./signals start 2>&1 | tail -f
```

### Check Database

```bash
# Count reddit_official signals collected
psql $SIGNALS_PG_DSN -c "
  SELECT company_name, COUNT(*) as signals
  FROM signals.accounts a
  JOIN signals.signal_observations so ON a.account_id = so.account_id
  WHERE so.source = 'reddit_official'
  GROUP BY company_name
  ORDER BY signals DESC
  LIMIT 20;
"
```

## Troubleshooting

### "429 Too Many Requests" Errors

**Problem**: Reddit is rate-limiting requests

**Solution**:
1. The collector automatically handles this with exponential backoff
2. If errors persist, increase delays in source_execution_policy.csv:
   ```
   reddit_official,2,0.25,20,2,10,50,true  # Slower settings
   ```
3. Or disable temporarily:
   ```
   reddit_official,2,0.25,20,2,10,50,false  # Disable
   ```

### No Signals Being Collected

**Problem**: Pipeline runs but reddit_official shows 0 signals

**Causes**:
1. Rate limited by Reddit (check logs for 429 errors)
2. Subreddit not found for company (expected for many companies)
3. No recent posts in official subreddit
4. Collector disabled in config

**Solution**:
1. Check logs: `grep reddit_official /var/log/signals/pipeline.log`
2. Verify config: `grep reddit_official config/source_execution_policy.csv`
3. Try manual test: `python3 src/collectors/reddit_official.py`

### Posts Not Appearing in Dashboard

**Problem**: Collector runs successfully but signals don't appear in dashboard

**Solution**:
1. Ensure scoring pipeline completed: `./signals score`
2. Refresh dashboard: F5 in browser
3. Check signal was inserted:
   ```bash
   psql $SIGNALS_PG_DSN -c "
     SELECT COUNT(*) FROM signals.signal_observations
     WHERE source = 'reddit_official' LIMIT 5;
   "
   ```

## Advanced Configuration

### Customize Subreddit Mappings

Edit `reddit_official.py` to add/remove company subreddits:

```python
official_subreddits = {
    "Notion Labs": "notion",
    "GitHub Inc": "github",
    "Stripe Inc": "stripe",
    # Add more mappings here
    "Your Company": "your_subreddit",
}
```

### Adjust Rate Limiting

In `source_execution_policy.csv`:

**Conservative** (for slow connections/being extra respectful):
```
reddit_official,2,0.25,20,2,10,50,true
```

**Default** (balanced):
```
reddit_official,4,0.5,20,2,5,50,true
```

**Aggressive** (if you have good connection and Reddit allows):
```
reddit_official,6,1.0,20,2,3,50,true
```

### Disable Reddit Official Collection

If you want to disable just reddit_official (but keep other collectors):

```csv
# In source_execution_policy.csv - change enabled to false:
reddit_official,4,0.5,20,2,5,50,false
```

Or in code, comment out in `src/pipeline/ingest.py`:

```python
# Disabled for now:
# results["reddit_official"] = (
#     await reddit_official.collect(conn, settings, lexicon, source_reliability)
#     if _collector_enabled("reddit_official")
#     else {"inserted": 0, "seen": 0}
# )
```

## Performance Expectations

### Collection Time

- **Per company**: ~2-5 seconds (including rate limiting delays)
- **13 companies with Reddit**: ~1-2 minutes
- **All collectors**: ~5-10 minutes total (parallel execution)

### Signal Volume

- **per company with active subreddit**: 3-20 posts/day
- **Signal quality**: High (official company content)
- **Reliability score**: 0.75 (between general Reddit at 0.65 and first-party at 0.90)

## Next Steps

1. **First Run**: Execute `./signals start` to collect initial reddit_official signals
2. **Monitor**: Check dashboard for "reddit_official" source signals
3. **Schedule**: Set up cron job to run daily
4. **Expand**: Add more company subreddits as needed
5. **Optimize**: Tune rate limiting based on actual Reddit behavior

## API Key Requirements

**Good news**: Reddit official subreddit collection requires **NO API keys**!

- Uses public Reddit API (`old.reddit.com/r/*/new.json`)
- No authentication needed
- No rate limit quota management needed
- Works with just HTTP requests

## Support

For issues or questions:
1. Check logs: `grep reddit_official ~/.signals/pipeline.log`
2. Test manually: `python3 src/collectors/reddit_official.py`
3. Check config files in `config/` directory
4. Review this documentation

---

**Last Updated**: 2026-03-03
**Pipeline Version**: 2.0 (includes reddit_official)
**Status**: Ready for production daily runs
