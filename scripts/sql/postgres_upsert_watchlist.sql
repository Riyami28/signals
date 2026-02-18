\set ON_ERROR_STOP on

INSERT INTO signals.accounts (
  account_id,
  company_name,
  domain,
  source_type,
  created_at,
  updated_at
)
SELECT
  ('acc_' || SUBSTRING(md5(lower(trim(sw.domain))) FROM 1 FOR 12)) AS account_id,
  COALESCE(NULLIF(trim(sw.company_name), ''), trim(sw.domain)) AS company_name,
  lower(trim(sw.domain)) AS domain,
  CASE WHEN lower(trim(COALESCE(sw.source_type, 'seed'))) = 'seed' THEN 'seed' ELSE 'discovered' END AS source_type,
  NOW(),
  NOW()
FROM signals.stage_watchlist sw
WHERE trim(COALESCE(sw.domain, '')) <> ''
  AND lower(trim(sw.domain)) <> 'zop.dev'
  AND lower(trim(sw.domain)) NOT LIKE '%.example'
ON CONFLICT (domain) DO UPDATE
SET
  company_name = EXCLUDED.company_name,
  source_type = EXCLUDED.source_type,
  updated_at = NOW();

INSERT INTO signals.account_metadata (
  account_id,
  country,
  region_group,
  industry_label,
  website_url,
  wikidata_id,
  sitelinks,
  revenue_usd,
  employees,
  ranking_score,
  data_source,
  last_refreshed_on,
  updated_at
)
SELECT
  ('acc_' || SUBSTRING(md5(lower(trim(sw.domain))) FROM 1 FOR 12)) AS account_id,
  COALESCE(trim(sw.country), ''),
  COALESCE(trim(sw.region_group), ''),
  COALESCE(trim(sw.industry_label), ''),
  COALESCE(trim(sw.website_url), ''),
  COALESCE(trim(sw.wikidata_id), ''),
  COALESCE(NULLIF(trim(sw.sitelinks), '')::INTEGER, 0),
  COALESCE(NULLIF(trim(sw.revenue_usd), '')::DOUBLE PRECISION, 0),
  COALESCE(NULLIF(trim(sw.employees), '')::INTEGER, 0),
  COALESCE(NULLIF(trim(sw.ranking_score), '')::DOUBLE PRECISION, 0),
  COALESCE(NULLIF(trim(sw.data_source), ''), 'migration'),
  NULLIF(trim(sw.last_refreshed_on), '')::DATE,
  NOW()
FROM signals.stage_watchlist sw
WHERE trim(COALESCE(sw.domain, '')) <> ''
  AND lower(trim(sw.domain)) <> 'zop.dev'
  AND lower(trim(sw.domain)) NOT LIKE '%.example'
ON CONFLICT (account_id) DO UPDATE
SET
  country = EXCLUDED.country,
  region_group = EXCLUDED.region_group,
  industry_label = EXCLUDED.industry_label,
  website_url = EXCLUDED.website_url,
  wikidata_id = EXCLUDED.wikidata_id,
  sitelinks = EXCLUDED.sitelinks,
  revenue_usd = EXCLUDED.revenue_usd,
  employees = EXCLUDED.employees,
  ranking_score = EXCLUDED.ranking_score,
  data_source = EXCLUDED.data_source,
  last_refreshed_on = EXCLUDED.last_refreshed_on,
  updated_at = NOW();

INSERT INTO signals.account_source_handles (
  account_id,
  domain,
  company_name,
  greenhouse_board,
  lever_company,
  careers_url,
  website_url,
  news_query,
  news_rss,
  reddit_query,
  updated_at
)
SELECT
  ('acc_' || SUBSTRING(md5(lower(trim(sh.domain))) FROM 1 FOR 12)) AS account_id,
  lower(trim(sh.domain)) AS domain,
  COALESCE(trim(sh.company_name), ''),
  COALESCE(trim(sh.greenhouse_board), ''),
  COALESCE(trim(sh.lever_company), ''),
  COALESCE(trim(sh.careers_url), ''),
  COALESCE(trim(sh.website_url), ''),
  COALESCE(trim(sh.news_query), ''),
  COALESCE(trim(sh.news_rss), ''),
  COALESCE(trim(sh.reddit_query), ''),
  NOW()
FROM signals.stage_handles sh
JOIN signals.accounts a
  ON a.domain = lower(trim(sh.domain))
WHERE trim(COALESCE(sh.domain, '')) <> ''
ON CONFLICT (account_id) DO UPDATE
SET
  domain = EXCLUDED.domain,
  company_name = EXCLUDED.company_name,
  greenhouse_board = EXCLUDED.greenhouse_board,
  lever_company = EXCLUDED.lever_company,
  careers_url = EXCLUDED.careers_url,
  website_url = EXCLUDED.website_url,
  news_query = EXCLUDED.news_query,
  news_rss = EXCLUDED.news_rss,
  reddit_query = EXCLUDED.reddit_query,
  updated_at = NOW();
