/* ===========================================================
   04_store_lifecycle.sql
   Goal: Cohorts & Ramp-up / Time-to-Benchmark analysis
   Depends on: core.v_sales_clean_alltime, core.v_sales_last90,
               core.v_stores, core.v_regions
   DB: PostgreSQL (BigQuery notes at bottom)
   =========================================================== */

SET search_path = public, core, stg, raw;

/* -----------------------------------------------------------
   0) BASE: all-time clean facts + store dims
   ----------------------------------------------------------- */
-- Денний базовий факт (clean, без аномалій)
WITH base AS (
  SELECT
    s.store_id,
    s.region,
    s.date::date,
    s.opening_date::date,
    s.revenue::NUMERIC AS revenue,
    s.transactions::NUMERIC AS transactions,
    CASE WHEN s.transactions > 0 THEN s.revenue / NULLIF(s.transactions,0) END AS aov
  FROM core.v_sales_clean_alltime s
  -- keep_core = 1 уже застосовано у в’юсі
),

/* -----------------------------------------------------------
   1) STORE-WEEKS: агрегуємо денні дані у тижні від відкриття
   ----------------------------------------------------------- */
store_weeks AS (
  SELECT
    b.store_id,
    b.region,
    DATE_TRUNC('week', b.opening_date)::date AS cohort_week0,
    DATE_TRUNC('month', b.opening_date)::date AS cohort_month,
    FLOOR( GREATEST((b.date - b.opening_date), 0) / 7 )::int AS weeks_since_open,
    DATE_TRUNC('week', b.date)::date AS week,
    SUM(b.revenue)        AS week_revenue,
    SUM(b.transactions)   AS week_tx,
    CASE WHEN SUM(b.transactions) > 0
         THEN SUM(b.revenue)/NULLIF(SUM(b.transactions),0)
    END AS week_aov
  FROM base b
  WHERE b.date >= b.opening_date  -- ігноруємо pre-open
  GROUP BY b.store_id, b.region, cohort_week0, cohort_month, weeks_since_open, DATE_TRUNC('week', b.date)
),

/* -----------------------------------------------------------
   2) NETWORK BENCHMARKS (median per-store KPIs over last 90d)
   ----------------------------------------------------------- */
last90_store_agg AS (
  SELECT
    s.store_id,
    s.region,
    SUM(s.revenue) AS rev90,
    SUM(s.transactions) AS tx90,
    CASE WHEN SUM(s.transactions) > 0
         THEN SUM(s.revenue)/NULLIF(SUM(s.transactions),0)
    END AS aov90
  FROM core.v_sales_last90 s
  GROUP BY s.store_id, s.region
),
benchmarks AS (
  SELECT
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY aov90) AS median_aov90_store,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY tx90)  AS median_tx90_store
  FROM last90_store_agg
),

/* -----------------------------------------------------------
   3) TIME-TO-BENCHMARK: перший тиждень, коли магазин >= медіан
   ----------------------------------------------------------- */
ttb_candidates AS (
  SELECT
    sw.store_id,
    sw.region,
    sw.cohort_month,
    sw.weeks_since_open,
    sw.week_aov,
    sw.week_tx
  FROM store_weeks sw
),
ttb_flagged AS (
  SELECT
    t.*,
    CASE WHEN t.week_aov IS NOT NULL AND t.week_tx IS NOT NULL
              AND t.week_aov >= (SELECT median_aov90_store FROM benchmarks)
              AND t.week_tx  >= (SELECT median_tx90_store  FROM benchmarks)
         THEN 1 ELSE 0 END AS meets_benchmark
  FROM ttb_candidates t
),
ttb_first AS (
  SELECT
    store_id,
    region,
    cohort_month,
    MIN(weeks_since_open) FILTER (WHERE meets_benchmark = 1) AS weeks_to_benchmark
  FROM ttb_flagged
  GROUP BY store_id, region, cohort_month
),

/* -----------------------------------------------------------
   4) COHORT RAMP: середні криві по когорті (тижні від відкриття)
   ----------------------------------------------------------- */
cohort_ramp AS (
  SELECT
    sw.cohort_month,
    sw.weeks_since_open,
    AVG(sw.week_revenue)    AS avg_week_revenue,
    AVG(sw.week_tx)         AS avg_week_tx,
    AVG(sw.week_aov)        AS avg_week_aov,
    COUNT(DISTINCT sw.store_id) AS stores_active
  FROM store_weeks sw
  GROUP BY sw.cohort_month, sw.weeks_since_open
),

/* -----------------------------------------------------------
   5) REGIONAL RAMP: середні криві по регіонах
   ----------------------------------------------------------- */
regional_ramp AS (
  SELECT
    sw.region,
    sw.weeks_since_open,
    AVG(sw.week_revenue) AS avg_week_revenue,
    AVG(sw.week_tx)      AS avg_week_tx,
    AVG(sw.week_aov)     AS avg_week_aov,
    COUNT(DISTINCT sw.store_id) AS stores_active
  FROM store_weeks sw
  GROUP BY sw.region, sw.weeks_since_open
),

/* -----------------------------------------------------------
   6) TTB SUMMARIES (розподіли часу-до-бенчмарку)
   ----------------------------------------------------------- */
ttb_region_summary AS (
  SELECT
    region,
    COUNT(*) FILTER (WHERE weeks_to_benchmark IS NOT NULL) AS stores_hit_bench,
    COUNT(*) AS stores_total,
    AVG(weeks_to_benchmark) AS avg_weeks_to_bench,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY weeks_to_benchmark) AS p50_weeks_to_bench,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY weeks_to_benchmark) AS p90_weeks_to_bench
  FROM ttb_first
  GROUP BY region
),
ttb_cohort_summary AS (
  SELECT
    cohort_month,
    COUNT(*) FILTER (WHERE weeks_to_benchmark IS NOT NULL) AS stores_hit_bench,
    COUNT(*) AS stores_total,
    AVG(weeks_to_benchmark) AS avg_weeks_to_bench,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY weeks_to_benchmark) AS p50_weeks_to_bench,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY weeks_to_benchmark) AS p90_weeks_to_bench
  FROM ttb_first
  GROUP BY cohort_month
)

/* -----------------------------------------------------------
   7) MATERIALIZE/VIEWS (за бажанням) + FINAL SELECT
   ----------------------------------------------------------- */
-- Зручно зберегти як в’юхи:
-- (Розкоментуй, якщо хочеш створити persistent views)

-- DROP VIEW IF EXISTS core.v_store_weeks;
-- CREATE VIEW core.v_store_weeks AS
-- SELECT * FROM store_weeks;

-- DROP VIEW IF EXISTS core.v_cohort_ramp;
-- CREATE VIEW core.v_cohort_ramp AS
-- SELECT * FROM cohort_ramp;

-- DROP VIEW IF EXISTS core.v_regional_ramp;
-- CREATE VIEW core.v_regional_ramp AS
-- SELECT * FROM regional_ramp;

-- DROP VIEW IF EXISTS core.v_time_to_benchmark;
-- CREATE VIEW core.v_time_to_benchmark AS
-- SELECT * FROM ttb_first;

-- Повертаємо бандли JSON (зручно для швидкої перевірки/BI):
SELECT
  -- Бенчмарки мережі
  (SELECT JSON_BUILD_OBJECT(
      'median_aov90_store', median_aov90_store,
      'median_tx90_store',  median_tx90_store
    )
   FROM benchmarks
  ) AS network_benchmarks,

  -- Ramp-up по когортам
  (SELECT JSON_AGG(t) FROM (
     SELECT cohort_month, weeks_since_open,
            avg_week_revenue, avg_week_tx, avg_week_aov, stores_active
     FROM cohort_ramp
     ORDER BY cohort_month, weeks_since_open
  ) t) AS cohort_ramp_curves,

  -- Ramp-up по регіонам
  (SELECT JSON_AGG(t) FROM (
     SELECT region, weeks_since_open,
            avg_week_revenue, avg_week_tx, avg_week_aov, stores_active
     FROM regional_ramp
     ORDER BY region, weeks_since_open
  ) t) AS regional_ramp_curves,

  -- Time-to-benchmark по магазинах (granular)
  (SELECT JSON_AGG(t) FROM (
     SELECT store_id, region, cohort_month, weeks_to_benchmark
     FROM ttb_first
     ORDER BY weeks_to_benchmark NULLS LAST
  ) t) AS store_time_to_benchmark,

  -- Агрегати TTB по регіонах
  (SELECT JSON_AGG(t) FROM (
     SELECT region, stores_hit_bench, stores_total,
            avg_weeks_to_bench, p50_weeks_to_bench, p90_weeks_to_bench
     FROM ttb_region_summary
     ORDER BY region
  ) t) AS ttb_region_summary,

  -- Агрегати TTB по когортам
  (SELECT JSON_AGG(t) FROM (
     SELECT cohort_month, stores_hit_bench, stores_total,
            avg_weeks_to_bench, p50_weeks_to_bench, p90_weeks_to_bench
     FROM ttb_cohort_summary
     ORDER BY cohort_month
  ) t) AS ttb_cohort_summary;
