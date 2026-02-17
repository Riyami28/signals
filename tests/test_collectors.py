import time

from src.collectors.community import _parse_entry_observed_at, _reddit_search_rss_url
from src.collectors.jobs import _extract_job_titles_from_html, _matches_from_text
from src.collectors.news import _google_news_rss_url, _match_signals


def test_jobs_fallback_match_for_role_terms():
    lexicon_rows = []
    matches = _matches_from_text("Hiring senior DevOps engineer", lexicon_rows)
    assert any(signal == "devops_role_open" for signal, _, _ in matches)


def test_news_keyword_matching_uses_lexicon():
    lexicon_rows = [
        {"signal_code": "compliance_initiative", "keyword": "soc 2", "confidence": "0.8"},
    ]
    matches = _match_signals("Company starts SOC 2 project", lexicon_rows)
    assert matches
    assert matches[0][0] == "compliance_initiative"


def test_extract_job_titles_from_jsonld_html():
    html = """
    <html>
      <head>
        <script type="application/ld+json">
          {"@context":"https://schema.org","@type":"JobPosting","title":"Senior DevOps Engineer"}
        </script>
      </head>
      <body></body>
    </html>
    """
    titles = _extract_job_titles_from_html(html)
    assert "Senior DevOps Engineer" in titles


def test_google_news_rss_url_contains_google_domain():
    url = _google_news_rss_url('"acme.com" cloud cost')
    assert url.startswith("https://news.google.com/rss/search?q=")
    assert "acme.com" in url


def test_reddit_rss_url_contains_reddit_domain():
    url = _reddit_search_rss_url('"acme.com" devops')
    assert url.startswith("https://www.reddit.com/search.rss?q=")
    assert "acme.com" in url


def test_community_entry_observed_at_prefers_published_parsed():
    ts = time.gmtime(1735689600)  # 2025-01-01T00:00:00Z
    observed_at = _parse_entry_observed_at({"published_parsed": ts})
    assert observed_at.startswith("2025-01-01T00:00:00")
