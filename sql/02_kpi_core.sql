/* ===========================================================
   02_kpi_core.sql
   Depends on: core.v_sales_last90, core.v_sales_clean_alltime,
               core.v_stores, core.v_regions
   DB: PostgreSQL (notes for BigQuery at bottom)
   =========================================================== */

SET search_path = public, core, stg, raw;

-- -----------------------------------------------------------
-- 0) Convenience CTE: base window (last 90 days) + safe AOV
-- -----------------------------------------------------------
WITH base AS (
  SELECT
    s.store_id,
    s.date,
    s.region,
    s.revenue::NUMERIC AS revenue,
    s.transactions::NUMERIC AS transactions,
    s.aov::NUMERIC AS aov,
    s.store_age_bucket,
    s.rev_per_capita,
    s.avg_income,
    s.population
  FROM core.v_sales_last90 s
),

/* ===========================================================
   1) EXECUTIVE KPI (network-level)
   =========================================================== */
exec_kpi AS (
  SELECT
    SUM(revenue) AS total_revenue,
    SUM(transactions) AS total_transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS network_aov
  FROM base
),

/* ===========================================================
   2) REGIONAL KPIs (Revenue, Tx, AOV) + per capita
   =========================================================== */
regional_kpis AS (
  SELECT
    region,
    SUM(revenue) AS revenue,
    SUM(transactions) AS transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS aov,
    -- daily per-capita, averaged over window
    AVG(CASE WHEN population > 0 THEN revenue/population END) AS avg_rev_per_capita
  FROM base
  GROUP BY region
),

/* ===========================================================
   3) STORE KPIs (aggregate over window)
   =========================================================== */
store_kpis AS (
  SELECT
    b.store_id,
    b.region,
    SUM(b.revenue) AS revenue,
    SUM(b.transactions) AS transactions,
    CASE WHEN SUM(b.transactions) > 0
         THEN SUM(b.revenue)/NULLIF(SUM(b.transactions),0)
    END AS aov
  FROM base b
  GROUP BY b.store_id, b.region
),

/* ===========================================================
   4) REGION AVERAGES to benchmark stores against peers
   =========================================================== */
region_bench AS (
  SELECT
    region,
    AVG(revenue) AS avg_rev_store,
    AVG(aov)     AS avg_aov_store,
    AVG(transactions) AS avg_tx_store,
    /* dispersion for z-scores */
    STDDEV_POP(revenue)      AS std_rev_store,
    STDDEV_POP(aov)          AS std_aov_store,
    STDDEV_POP(transactions) AS std_tx_store
  FROM store_kpis
  GROUP BY region
),

/* ===========================================================
   5) STORES VS REGION: gaps and z-scores
   =========================================================== */
stores_vs_region AS (
  SELECT
    s.store_id,
    s.region,
    s.revenue,
    s.transactions,
    s.aov,
    r.avg_rev_store,
    r.avg_aov_store,
    r.avg_tx_store,
    (s.revenue - r.avg_rev_store) AS gap_rev_vs_region,
    (s.aov     - r.avg_aov_store) AS gap_aov_vs_region,
    (s.transactions - r.avg_tx_store) AS gap_tx_vs_region,
    CASE WHEN r.std_rev_store > 0 THEN (s.revenue - r.avg_rev_store)/r.std_rev_store END AS z_rev_vs_region,
    CASE WHEN r.std_aov_store > 0 THEN (s.aov - r.avg_aov_store)/r.std_aov_store END       AS z_aov_vs_region,
    CASE WHEN r.std_tx_store > 0 THEN (s.transactions - r.avg_tx_store)/r.std_tx_store END  AS z_tx_vs_region
  FROM store_kpis s
  JOIN region_bench r USING (region)
),

/* ===========================================================
   6) WEEKDAY SEASONALITY (network-level)
   =========================================================== */
weekday_seasonality AS (
  SELECT
    EXTRACT(ISODOW FROM date) AS weekday,  -- 1=Mon..7=Sun
    SUM(revenue) AS revenue,
    SUM(transactions) AS transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS aov
  FROM base
  GROUP BY EXTRACT(ISODOW FROM date)
),

/* ===========================================================
   7) DAILY / WEEKLY TRENDS
   =========================================================== */
daily_trend AS (
  SELECT
    date,
    SUM(revenue) AS revenue,
    SUM(transactions) AS transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS aov
  FROM base
  GROUP BY date
),
weekly_trend AS (
  SELECT
    DATE_TRUNC('week', date)::DATE AS week,
    SUM(revenue) AS revenue,
    SUM(transactions) AS transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS aov
  FROM base
  GROUP BY DATE_TRUNC('week', date)
),

/* ===========================================================
   8) STORE LEAGUE (quartiles within region, current quarter)
   =========================================================== */
this_quarter AS (
  SELECT
    b.store_id, b.region,
    SUM(b.revenue) AS q_rev,
    SUM(b.transactions) AS q_tx,
    CASE WHEN SUM(b.transactions) > 0
         THEN SUM(b.revenue)/NULLIF(SUM(b.transactions),0)
    END AS q_aov
  FROM core.v_sales_clean_alltime b
  WHERE b.date >= DATE_TRUNC('quarter', CURRENT_DATE)
    AND b.keep_core = 1
  GROUP BY b.store_id, b.region
),
store_league AS (
  SELECT
    *,
    NTILE(4) OVER (PARTITION BY region ORDER BY q_rev DESC) AS quartile_by_rev,
    NTILE(4) OVER (PARTITION BY region ORDER BY q_aov DESC) AS quartile_by_aov,
    NTILE(4) OVER (PARTITION BY region ORDER BY q_tx  DESC) AS quartile_by_tx
  FROM this_quarter
),

/* ===========================================================
   9) LIFECYCLE COHORTS (age buckets)
   =========================================================== */
lifecycle AS (
  SELECT
    store_age_bucket,
    SUM(revenue) AS revenue,
    SUM(transactions) AS transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS aov
  FROM base
  GROUP BY store_age_bucket
),

/* ===========================================================
   10) INCOME ↔ AOV (regional elasticity proxy)
   Regression at region-level: avg_income → avg AOV
   =========================================================== */
aov_by_region AS (
  SELECT
    region,
    AVG(aov) AS avg_aov
  FROM (
    SELECT region,
           CASE WHEN transactions > 0 THEN revenue/NULLIF(transactions,0) END AS aov
    FROM base
  ) t
  GROUP BY region
),
income_vs_aov AS (
  SELECT
    r.region,
    r.avg_income,
    a.avg_aov
  FROM aov_by_region a
  JOIN core.v_regions r USING (region)
)
SELECT
  /* -------- Executive KPIs -------- */
  (SELECT total_revenue FROM exec_kpi)      AS exec_total_revenue,
  (SELECT total_transactions FROM exec_kpi) AS exec_total_tx,
  (SELECT network_aov FROM exec_kpi)        AS exec_network_aov,

  /* -------- Top/Bottom regions by AOV -------- */
  (SELECT JSON_AGG(t) FROM (
     SELECT region, aov, revenue, transactions, avg_rev_per_capita
     FROM regional_kpis
     ORDER BY aov DESC
     LIMIT 5
   ) t) AS top5_regions_by_aov,
  (SELECT JSON_AGG(t) FROM (
     SELECT region, aov, revenue, transactions, avg_rev_per_capita
     FROM regional_kpis
     ORDER BY aov ASC
     LIMIT 5
   ) t) AS bottom5_regions_by_aov,

  /* -------- Top/Bottom stores vs region (z-score by revenue) -------- */
  (SELECT JSON_AGG(t) FROM (
     SELECT store_id, region, revenue, z_rev_vs_region, gap_rev_vs_region
     FROM stores_vs_region
     ORDER BY z_rev_vs_region DESC NULLS LAST
     LIMIT 20
   ) t) AS top20_stores_vs_region_rev,
  (SELECT JSON_AGG(t) FROM (
     SELECT store_id, region, revenue, z_rev_vs_region, gap_rev_vs_region
     FROM stores_vs_region
     ORDER BY z_rev_vs_region ASC NULLS LAST
     LIMIT 20
   ) t) AS bottom20_stores_vs_region_rev,

  /* -------- Weekday seasonality -------- */
  (SELECT JSON_AGG(t) FROM (
     SELECT weekday, revenue, transactions, aov
     FROM weekday_seasonality
     ORDER BY weekday
   ) t) AS weekday_seasonality,

  /* -------- Daily & weekly trend -------- */
  (SELECT JSON_AGG(t) FROM (
     SELECT date, revenue, transactions, aov
     FROM daily_trend
     ORDER BY date
   ) t) AS daily_trend,
  (SELECT JSON_AGG(t) FROM (
     SELECT week, revenue, transactions, aov
     FROM weekly_trend
     ORDER BY week
   ) t) AS weekly_trend,

  /* -------- Store league (current quarter) -------- */
  (SELECT JSON_AGG(t) FROM (
     SELECT region, store_id, q_rev, q_aov, q_tx,
            quartile_by_rev, quartile_by_aov, quartile_by_tx
     FROM store_league
     ORDER BY region, quartile_by_rev, q_rev DESC
   ) t) AS store_league,

  /* -------- Lifecycle cohorts -------- */
  (SELECT JSON_AGG(t) FROM (
     SELECT store_age_bucket, revenue, transactions, aov
     FROM lifecycle
     ORDER BY store_age_bucket
   ) t) AS lifecycle_kpis,

  /* -------- Income vs AOV regression (network-level) -------- */
  (SELECT JSON_BUILD_OBJECT(
      'n_regions', (SELECT COUNT(*) FROM income_vs_aov),
      'corr',      CORR(avg_income, avg_aov),
      'slope',     REGR_SLOPE(avg_aov, avg_income),
      'intercept', REGR_INTERCEPT(avg_aov, avg_income)
    )
   FROM income_vs_aov
  ) AS income_aov_regression;
