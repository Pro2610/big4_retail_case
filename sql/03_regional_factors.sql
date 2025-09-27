/* ===========================================================
   03_regional_factors.sql
   Goal: Deep-dive on regional drivers
   Depends on: core.v_sales_last90, core.v_regions, core.v_sales_clean_alltime
   DB: PostgreSQL (see BigQuery notes at bottom)
   =========================================================== */

SET search_path = public, core, stg, raw;

/* -----------------------------------------------------------
   0) BASE WINDOW (last 90 days), safe AOV
   ----------------------------------------------------------- */
WITH base AS (
  SELECT
    s.date,
    s.region,
    s.store_id,
    s.revenue::NUMERIC AS revenue,
    s.transactions::NUMERIC AS transactions,
    CASE WHEN s.transactions > 0 THEN s.revenue / NULLIF(s.transactions, 0) END AS aov,
    s.population,
    s.avg_income
  FROM core.v_sales_last90 s
),

/* -----------------------------------------------------------
   1) REGION AGGREGATES (per 90-day window)
   ----------------------------------------------------------- */
region_agg AS (
  SELECT
    region,
    SUM(revenue) AS revenue,
    SUM(transactions) AS transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS aov,
    AVG(population) AS population,     -- stable across days
    AVG(avg_income) AS avg_income
  FROM base
  GROUP BY region
),

/* -----------------------------------------------------------
   2) NETWORK BENCHMARKS (weighted)
   ----------------------------------------------------------- */
network_bench AS (
  SELECT
    SUM(revenue) AS net_revenue,
    SUM(transactions) AS net_transactions,
    CASE WHEN SUM(transactions) > 0
         THEN SUM(revenue)/NULLIF(SUM(transactions),0)
    END AS net_aov
  FROM base
),

/* -----------------------------------------------------------
   3) PER-CAPITA NORMALIZATION + INCOME BINS
   ----------------------------------------------------------- */
with_percap_bins AS (
  SELECT
    r.region,
    r.revenue,
    r.transactions,
    r.aov,
    r.population,
    r.avg_income,
    -- per-capita revenue for the window (approx: total / population)
    CASE WHEN r.population > 0 THEN r.revenue / r.population END AS revenue_per_capita,
    -- income binning (quintiles over regions)
    NTILE(5) OVER (ORDER BY r.avg_income) AS income_quintile
  FROM region_agg r
),

/* -----------------------------------------------------------
   4) INCOME ↔ AOV (elasticity proxy)
   ----------------------------------------------------------- */
income_aov AS (
  SELECT
    region,
    avg_income,
    aov
  FROM with_percap_bins
),
income_aov_stats AS (
  SELECT
    COUNT(*) AS n_regions,
    CORR(avg_income, aov) AS corr_income_aov,
    REGR_SLOPE(aov, avg_income) AS slope_aov_per_income,
    REGR_INTERCEPT(aov, avg_income) AS intercept
  FROM income_aov
),

/* -----------------------------------------------------------
   5) DECOMPOSITION: Revenue gap vs Network
      Revenue = AOV * Transactions
      ΔRevenue ≈ (AOV_region - AOV_net) * Tx_net  +  (Tx_region - Tx_net) * AOV_net
      + cross-term (small; we’ll also show exact delta for reference)
   ----------------------------------------------------------- */
decomp AS (
  SELECT
    r.region,
    r.revenue,
    r.transactions,
    r.aov,
    n.net_revenue,
    n.net_transactions,
    n.net_aov,
    -- exact gap
    r.revenue - (n.net_revenue / (SELECT COUNT(*) FROM region_agg)) AS gap_vs_equal_share, -- vs equal share baseline (optional)
    -- mix-effect decomposition vs network averages
    (r.aov - n.net_aov) * n.net_transactions     AS aov_component,
    (r.transactions - n.net_transactions) * n.net_aov AS tx_component,
    -- cross term (for transparency)
    (r.aov - n.net_aov) * (r.transactions - n.net_transactions) AS cross_component
  FROM region_agg r
  CROSS JOIN network_bench n
),

/* -----------------------------------------------------------
   6) INCOME BINS SUMMARY (quintiles)
   ----------------------------------------------------------- */
bin_summary AS (
  SELECT
    income_quintile,
    COUNT(*) AS regions_in_bin,
    AVG(avg_income) AS bin_avg_income,
    AVG(aov) AS bin_avg_aov,
    AVG(revenue) AS bin_avg_revenue,
    AVG(transactions) AS bin_avg_tx,
    AVG(revenue_per_capita) AS bin_avg_rev_per_capita
  FROM with_percap_bins
  GROUP BY income_quintile
),

/* -----------------------------------------------------------
   7) POPULATION SIZE BINS (terciles) & density normalization
   ----------------------------------------------------------- */
pop_binned AS (
  SELECT
    r.*,
    NTILE(3) OVER (ORDER BY population) AS population_tercile
  FROM region_agg r
),
pop_summary AS (
  SELECT
    population_tercile,
    COUNT(*) AS regions_in_bin,
    AVG(population) AS bin_avg_population,
    AVG(aov) AS bin_avg_aov,
    AVG(revenue) AS bin_avg_revenue,
    AVG(transactions) AS bin_avg_tx,
    AVG(CASE WHEN population > 0 THEN revenue / population END) AS bin_avg_rev_per_capita
  FROM pop_binned
  GROUP BY population_tercile
)

/* -----------------------------------------------------------
   8) FINAL SELECT (JSON bundles)
   ----------------------------------------------------------- */
SELECT
  /* Region table with per-capita and income bins */
  (SELECT JSON_AGG(t) FROM (
     SELECT region, revenue, transactions, aov, population, avg_income,
            revenue_per_capita, income_quintile
     FROM with_percap_bins
     ORDER BY aov DESC
  ) t) AS regions_percap_incomebins,

  /* Income ↔ AOV regression metrics */
  (SELECT JSON_BUILD_OBJECT(
      'n_regions', n_regions,
      'corr_income_aov', corr_income_aov,
      'slope_aov_per_income', slope_aov_per_income,
      'intercept', intercept
    )
   FROM income_aov_stats
  ) AS income_aov_regression,

  /* Decomposition of revenue gap vs network averages */
  (SELECT JSON_AGG(t) FROM (
     SELECT region, revenue, transactions, aov,
            net_revenue, net_transactions, net_aov,
            aov_component, tx_component, cross_component
     FROM decomp
     ORDER BY aov_component + tx_component + cross_component DESC
  ) t) AS regional_revenue_decomposition,

  /* Income quintile summaries */
  (SELECT JSON_AGG(t) FROM (
     SELECT income_quintile, regions_in_bin, bin_avg_income,
            bin_avg_aov, bin_avg_revenue, bin_avg_tx, bin_avg_rev_per_capita
     FROM bin_summary
     ORDER BY income_quintile
  ) t) AS income_quintile_summary,

  /* Population tercile summaries */
  (SELECT JSON_AGG(t) FROM (
     SELECT population_tercile, regions_in_bin, bin_avg_population,
            bin_avg_aov, bin_avg_revenue, bin_avg_tx, bin_avg_rev_per_capita
     FROM pop_summary
     ORDER BY population_tercile
  ) t) AS population_tercile_summary;
