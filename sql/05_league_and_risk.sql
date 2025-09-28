/* ===========================================================
   05_league_and_risk.sql
   Goal: Store league (quartiles/deciles) + risk scoring & watchlist
   Depends on: core.v_sales_last90, core.v_sales_clean_alltime, core.v_stores
   DB: PostgreSQL (notes for BigQuery at bottom)
   =========================================================== */

SET search_path = public, core, stg, raw;

/* -----------------------------------------------------------
   0) BASE WINDOW (last 90 days) aggregated to store level
   ----------------------------------------------------------- */
WITH base AS (
  SELECT
    s.store_id,
    s.region,
    SUM(s.revenue)::NUMERIC       AS revenue90,
    SUM(s.transactions)::NUMERIC  AS tx90,
    CASE WHEN SUM(s.transactions) > 0
         THEN SUM(s.revenue)/NULLIF(SUM(s.transactions),0)
    END::NUMERIC                  AS aov90
  FROM core.v_sales_last90 s
  GROUP BY s.store_id, s.region
),

/* -----------------------------------------------------------
   1) REGION STATISTICS for z-scores (dispersion within region)
   ----------------------------------------------------------- */
region_stats AS (
  SELECT
    region,
    AVG(revenue90)             AS avg_rev_reg,
    STDDEV_POP(revenue90)      AS sd_rev_reg,
    AVG(tx90)                  AS avg_tx_reg,
    STDDEV_POP(tx90)           AS sd_tx_reg,
    AVG(aov90)                 AS avg_aov_reg,
    STDDEV_POP(aov90)          AS sd_aov_reg
  FROM base
  GROUP BY region
),

/* -----------------------------------------------------------
   2) STORE Z-SCORES vs region peers
   ----------------------------------------------------------- */
store_z AS (
  SELECT
    b.store_id,
    b.region,
    b.revenue90,
    b.tx90,
    b.aov90,
    (b.revenue90 - r.avg_rev_reg) / NULLIF(r.sd_rev_reg,0)  AS z_rev,
    (b.tx90      - r.avg_tx_reg)  / NULLIF(r.sd_tx_reg,0)   AS z_tx,
    (b.aov90     - r.avg_aov_reg) / NULLIF(r.sd_aov_reg,0)  AS z_aov
  FROM base b
  JOIN region_stats r USING (region)
),

/* -----------------------------------------------------------
   3) STORE LEAGUE: quartiles & deciles within region by revenue90
   ----------------------------------------------------------- */
store_league AS (
  SELECT
    s.*,
    NTILE(4)  OVER (PARTITION BY region ORDER BY revenue90 DESC) AS q_rev_in_region,
    NTILE(10) OVER (PARTITION BY region ORDER BY revenue90 DESC) AS d_rev_in_region,
    NTILE(4)  OVER (PARTITION BY region ORDER BY aov90 DESC)     AS q_aov_in_region,
    NTILE(4)  OVER (PARTITION BY region ORDER BY tx90 DESC)      AS q_tx_in_region
  FROM store_z s
),

/* -----------------------------------------------------------
   4) RISK SCORING
      - idea: penalize sustained underperformance vs region peers
      - weights: revenue 0.5, tx 0.3, aov 0.2 (tuneable)
      - clamp z-scores to [-3, +3], invert (negative worse), map to [0..100]
   ----------------------------------------------------------- */
scored AS (
  SELECT
    l.*,
    GREATEST(LEAST(COALESCE(z_rev,0),  3), -3) AS z_rev_c,
    GREATEST(LEAST(COALESCE(z_tx,0),   3), -3) AS z_tx_c,
    GREATEST(LEAST(COALESCE(z_aov,0),  3), -3) AS z_aov_c
  FROM store_league l
),
risk AS (
  SELECT
    s.*,
    /* Weighted negative z (worse = higher raw_risk) */
    (0.5 * (-s.z_rev_c) + 0.3 * (-s.z_tx_c) + 0.2 * (-s.z_aov_c)) AS raw_risk,
    /* Normalize to 0..100 via sigmoid-ish mapping */
    ROUND( 100 * (1.0 / (1.0 + EXP(- raw_risk)) ), 1) AS risk_score
  FROM scored s
),

/* -----------------------------------------------------------
   5) WATCHLIST rules
      A store is "at risk" if:
        - risk_score >= 70
        - AND in bottom quartile by revenue within region
        - OR both z_tx and z_aov are below -0.5 (broad weakness)
   ----------------------------------------------------------- */
watchlist AS (
  SELECT
    r.*,
    CASE
      WHEN (r.risk_score >= 70 AND r.q_rev_in_region = 4)
        OR (COALESCE(r.z_tx,0) < -0.5 AND COALESCE(r.z_aov,0) < -0.5)
      THEN 1 ELSE 0
    END AS is_watchlist
  FROM risk r
),

/* -----------------------------------------------------------
   6) REGION SUMMARIES: % at-risk, dispersion, and tails
   ----------------------------------------------------------- */
region_summary AS (
  SELECT
    region,
    COUNT(*) AS stores,
    AVG(risk_score) AS avg_risk_score,
    SUM(CASE WHEN is_watchlist=1 THEN 1 ELSE 0 END) AS at_risk_cnt,
    100.0 * SUM(CASE WHEN is_watchlist=1 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS at_risk_pct,
    -- dispersion signals
    PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY revenue90) AS p10_rev,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY revenue90) AS p90_rev,
    (PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY revenue90) /
     NULLIF(PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY revenue90),0)) AS dispersion_ratio
  FROM watchlist
  GROUP BY region
),

/* -----------------------------------------------------------
   7) OUTPUT BUNDLES
   ----------------------------------------------------------- */
top_bottom_stores AS (
  SELECT
    region, store_id, revenue90, tx90, aov90,
    q_rev_in_region, q_tx_in_region, q_aov_in_region,
    z_rev, z_tx, z_aov, risk_score, is_watchlist
  FROM watchlist
),
region_boards AS (
  SELECT
    region,
    -- Top-10 leaders by revenue90
    (SELECT JSON_AGG(t) FROM (
       SELECT store_id, revenue90, tx90, aov90, risk_score
       FROM top_bottom_stores tbs
       WHERE tbs.region = rs.region
       ORDER BY revenue90 DESC
       LIMIT 10
    ) t) AS top10_leaders,
    -- Bottom-10 by risk (highest risk_score)
    (SELECT JSON_AGG(t) FROM (
       SELECT store_id, revenue90, tx90, aov90, risk_score, q_rev_in_region, z_rev, z_tx, z_aov
       FROM top_bottom_stores tbs
       WHERE tbs.region = rs.region
       ORDER BY risk_score DESC
       LIMIT 10
    ) t) AS top10_watchlist
  FROM region_summary rs
)

SELECT
  -- Full per-store table (league + z + risk)
  (SELECT JSON_AGG(t) FROM (
     SELECT * FROM top_bottom_stores
     ORDER BY region, risk_score DESC, revenue90 ASC
  ) t) AS store_league_risk,

  -- Region-level summary board
  (SELECT JSON_AGG(t) FROM (
     SELECT region, stores, avg_risk_score, at_risk_cnt, at_risk_pct, dispersion_ratio
     FROM region_summary
     ORDER BY at_risk_pct DESC
  ) t) AS region_risk_summary,

  -- Region boards: leaders & watchlist
  (SELECT JSON_AGG(t) FROM region_boards t) AS region_boards_json;
