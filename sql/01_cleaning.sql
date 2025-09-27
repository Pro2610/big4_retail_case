/* ===========================================================
   01_cleaning.sql  (PostgreSQL)
   Purpose: Stage raw CSVs → validate → flag anomalies → clean
            → derive fields → expose clean views
   =========================================================== */

-- Optional: create schemas to keep things tidy
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS core;

-- 0) Drop & re-create staging if needed
DROP TABLE IF EXISTS raw.regions;
DROP TABLE IF EXISTS raw.stores;
DROP TABLE IF EXISTS raw.sales;

-- 1) RAW tables (structure mirrors CSVs)
CREATE TABLE raw.regions (
  region       TEXT PRIMARY KEY,
  population   BIGINT,
  avg_income   NUMERIC
);

CREATE TABLE raw.stores (
  store_id       BIGINT PRIMARY KEY,
  city           TEXT,
  region         TEXT,
  opening_date   DATE
);

CREATE TABLE raw.sales (
  store_id      BIGINT,
  date          DATE,
  revenue       NUMERIC,
  transactions  BIGINT
  -- duplicates may exist; no PK here intentionally
);

-- 2) (Load CSVs)
-- Postgres psql example:
-- \copy raw.regions  FROM 'data/regions.csv'  CSV HEADER
-- \copy raw.stores   FROM 'data/stores.csv'   CSV HEADER
-- \copy raw.sales    FROM 'data/sales.csv'    CSV HEADER

/* ===========================================================
   VALIDATION & BASIC SANITY
   =========================================================== */

-- Missing region rows?
WITH missing AS (
  SELECT s.region
  FROM raw.stores s
  LEFT JOIN raw.regions r ON r.region = s.region
  WHERE r.region IS NULL
)
SELECT 'MISSING_REGION_IN_REGIONS' AS issue, COUNT(*) AS cnt
FROM missing;

-- Duplicate (store_id, date)?
WITH dups AS (
  SELECT store_id, date, COUNT(*) AS c
  FROM raw.sales
  GROUP BY store_id, date
  HAVING COUNT(*) > 1
)
SELECT 'DUPLICATE_STORE_DATE' AS issue, COUNT(*) AS rows_affected
FROM dups;

-- Date span
SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM raw.sales;

/* ===========================================================
   3) STAGING with anomaly flags
   =========================================================== */

DROP TABLE IF EXISTS stg.sales_flagged;
CREATE TABLE stg.sales_flagged AS
WITH base AS (
  SELECT
    s.store_id,
    s.date,
    s.revenue,
    s.transactions,
    -- joins for enrichment
    st.region,
    st.opening_date,
    r.population,
    r.avg_income
  FROM raw.sales s
  JOIN raw.stores st  ON st.store_id = s.store_id
  JOIN raw.regions r  ON r.region   = st.region
),
anomaly_rules AS (
  SELECT
    *,
    /* Rule A: tx = 0 but revenue > 0 (data-entry/test anomaly) */
    CASE WHEN transactions = 0 AND revenue > 0 THEN 1 ELSE 0 END AS flag_tx0_revpos,
    /* Rule B: transactions < 0 (invalid) */
    CASE WHEN transactions < 0 THEN 1 ELSE 0 END AS flag_tx_negative,
    /* Rule C: revenue <= 0 while tx > 0 (likely returns day) */
    CASE WHEN transactions > 0 AND revenue <= 0 THEN 1 ELSE 0 END AS flag_rev_nonpos_txpos,
    /* Rule D: missing or invalid date */
    CASE WHEN date IS NULL THEN 1 ELSE 0 END AS flag_date_null
  FROM base
)
SELECT * FROM anomaly_rules;

-- Helpful index for later
CREATE INDEX IF NOT EXISTS idx_stg_sales_flagged_region_date
  ON stg.sales_flagged(region, date);

-- 4) Extreme outlier winsorization thresholds (by region+date)
-- Here we compute percentiles for revenue per store-day to cap extremes.
-- Note: requires Postgres 15+ for PERCENTILE_CONT in SELECT; otherwise move to window approach.
DROP TABLE IF EXISTS stg.rev_thresholds;
CREATE TABLE stg.rev_thresholds AS
SELECT
  region,
  date,
  PERCENTILE_CONT(0.01) WITHIN GROUP (ORDER BY revenue) AS p01_rev,
  PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY revenue) AS p99_rev
FROM stg.sales_flagged
GROUP BY region, date;

CREATE INDEX IF NOT EXISTS idx_rev_thresholds_region_date
  ON stg.rev_thresholds(region, date);

-- 5) Apply cleaning rules
DROP TABLE IF EXISTS stg.sales_cleaned;
CREATE TABLE stg.sales_cleaned AS
WITH joined AS (
  SELECT f.*,
         t.p01_rev,
         t.p99_rev
  FROM stg.sales_flagged f
  LEFT JOIN stg.rev_thresholds t
    ON t.region = f.region AND t.date = f.date
),
filtered AS (
  SELECT
    *,
    -- Core keep-logic: exclude bad structural anomalies from KPI base
    CASE
      WHEN flag_tx_negative = 1 THEN 0
      WHEN flag_date_null   = 1 THEN 0
      WHEN flag_tx0_revpos  = 1 THEN 0
      ELSE 1
    END AS keep_core
  FROM joined
),
winsorized AS (
  SELECT
    store_id,
    date,
    region,
    opening_date,
    population,
    avg_income,
    transactions,
    /* Cap revenue at [p01, p99] if thresholds present */
    CASE
      WHEN p01_rev IS NOT NULL AND revenue < p01_rev THEN p01_rev
      WHEN p99_rev IS NOT NULL AND revenue > p99_rev THEN p99_rev
      ELSE revenue
    END AS revenue_capped,
    -- carry flags
    flag_tx0_revpos,
    flag_tx_negative,
    flag_rev_nonpos_txpos,
    flag_date_null,
    keep_core
  FROM filtered
)
SELECT * FROM winsorized;

CREATE INDEX IF NOT EXISTS idx_stg_sales_cleaned_store_date
  ON stg.sales_cleaned(store_id, date);

-- 6) Deduplicate (store_id, date) keeping the highest revenue after capping
DROP TABLE IF EXISTS stg.sales_dedup;
CREATE TABLE stg.sales_dedup AS
WITH ranked AS (
  SELECT
    sc.*,
    ROW_NUMBER() OVER (PARTITION BY store_id, date ORDER BY revenue_capped DESC, transactions DESC) AS rn
  FROM stg.sales_cleaned sc
)
SELECT * FROM ranked WHERE rn = 1;

CREATE INDEX IF NOT EXISTS idx_stg_sales_dedup_store_date
  ON stg.sales_dedup(store_id, date);

/* ===========================================================
   7) DERIVED FIELDS (AOV, store_age, buckets, per-capita)
   =========================================================== */

DROP TABLE IF EXISTS stg.sales_enriched;
CREATE TABLE stg.sales_enriched AS
SELECT
  store_id,
  date,
  region,
  opening_date,
  population,
  avg_income,
  transactions,
  revenue_capped AS revenue,
  keep_core,
  flag_tx0_revpos,
  flag_tx_negative,
  flag_rev_nonpos_txpos,
  flag_date_null,
  /* Safe AOV */
  CASE WHEN transactions > 0 THEN (revenue_capped / NULLIF(transactions, 0)) END AS aov,
  /* Age in days at date */
  GREATEST((date - opening_date), 0) AS store_age_days,
  CASE
    WHEN (date - opening_date) < 0 THEN 'pre-open'
    WHEN (date - opening_date) < 180 THEN '0-6m'
    WHEN (date - opening_date) < 365 THEN '6-12m'
    WHEN (date - opening_date) < 730 THEN '1-2y'
    ELSE '2y+'
  END AS store_age_bucket,
  /* Per-capita daily revenue (rough proxy) */
  CASE WHEN population > 0 THEN revenue_capped / population::NUMERIC END AS rev_per_capita
FROM stg.sales_dedup;

CREATE INDEX IF NOT EXISTS idx_stg_sales_enriched_region_date
  ON stg.sales_enriched(region, date);

/* ===========================================================
   8) CORE VIEWS for analysis
   =========================================================== */

-- Window selector for “last 90 days” (you can change to a fixed quarter with DATE_TRUNC)
DROP VIEW IF EXISTS core.v_sales_last90;
CREATE VIEW core.v_sales_last90 AS
SELECT *
FROM stg.sales_enriched
WHERE date >= (CURRENT_DATE - INTERVAL '90 day')
  AND keep_core = 1;

-- All-time clean (keeping core), useful for rolling analyses
DROP VIEW IF EXISTS core.v_sales_clean_alltime;
CREATE VIEW core.v_sales_clean_alltime AS
SELECT * FROM stg.sales_enriched
WHERE keep_core = 1;

-- Anomalies (for data quality dashboard)
DROP VIEW IF EXISTS core.v_sales_anomalies;
CREATE VIEW core.v_sales_anomalies AS
SELECT *
FROM stg.sales_enriched
WHERE keep_core = 0
   OR flag_rev_nonpos_txpos = 1;

-- Convenience dimension views
DROP VIEW IF EXISTS core.v_stores;
CREATE VIEW core.v_stores AS
SELECT * FROM raw.stores;

DROP VIEW IF EXISTS core.v_regions;
CREATE VIEW core.v_regions AS
SELECT * FROM raw.regions;

/* ===========================================================
   9) QUICK QA QUERIES (optional)
   =========================================================== */

-- What % of rows were excluded from core?
SELECT
  COUNT(*) FILTER (WHERE keep_core = 0)::NUMERIC / NULLIF(COUNT(*),0) AS excluded_share
FROM stg.sales_enriched;

-- AOV sanity by region (last 90 days)
SELECT region,
       AVG(aov) AS avg_aov,
       AVG(transactions) AS avg_tx,
       AVG(revenue) AS avg_rev
FROM core.v_sales_last90
GROUP BY region
ORDER BY avg_aov DESC;

-- Age bucket mix
SELECT store_age_bucket, COUNT(*) AS rows
FROM core.v_sales_last90
GROUP BY store_age_bucket
ORDER BY rows DESC;
