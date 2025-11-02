-- ================================================================
-- Agentic Portfolio Manager Demo: Silver & Gold Transformations (Regenerated)
-- Catalog/Schema: sas_demo.saswata_sengupta_agneticportfoliomanager
-- Purpose: Build silver cleaned tables and gold aggregates/KPIs to surface
--          regime shift (NVDA), policy alignment, breaches, execution costs,
--          portfolio exposure, PnL, unified QA bridge, and investor report metadata alignment.
-- Notes:
-- - Valid Spark SQL (Databricks) only. All statements terminated with ';'
-- - Never create views; always CREATE OR REPLACE TABLE
-- - Silver preserves atomic granularity; Gold provides reusable aggregates
-- - References raw tables exactly as provided. Prefer raw_raw_* canonical names when present.
-- ================================================================

USE CATALOG sas_demo;
USE SCHEMA saswata_sengupta_agneticportfoliomanager;

-- =============================
-- SILVER TABLES (CLEAN/DERIVED)
-- =============================

-- 1) silver_factset_vectors
-- Derive 90-day rolling baselines per ticker for key factors and vector_shift_ratio fields.
CREATE OR REPLACE TABLE silver_factset_vectors AS
WITH base AS (
  SELECT
    ticker,
    CAST(date AS DATE) AS date,
    momentum_score,
    earnings_quality_score,
    valuation_score,
    volatility_score,
    news_sentiment_score
  FROM raw_raw_factset_factor_vectors
  WHERE ticker IS NOT NULL AND date IS NOT NULL
),
baseline AS (
  SELECT
    ticker,
    date,
    momentum_score,
    earnings_quality_score,
    valuation_score,
    volatility_score,
    news_sentiment_score,
    AVG(momentum_score) OVER (
      PARTITION BY ticker ORDER BY date
      RANGE BETWEEN INTERVAL 90 DAYS PRECEDING AND CURRENT ROW
    ) AS baseline_90d_momentum,
    AVG(earnings_quality_score) OVER (
      PARTITION BY ticker ORDER BY date
      RANGE BETWEEN INTERVAL 90 DAYS PRECEDING AND CURRENT ROW
    ) AS baseline_90d_quality,
    AVG(volatility_score) OVER (
      PARTITION BY ticker ORDER BY date
      RANGE BETWEEN INTERVAL 90 DAYS PRECEDING AND CURRENT ROW
    ) AS baseline_90d_volatility,
    AVG(news_sentiment_score) OVER (
      PARTITION BY ticker ORDER BY date
      RANGE BETWEEN INTERVAL 90 DAYS PRECEDING AND CURRENT ROW
    ) AS baseline_90d_sentiment
  FROM base
)
SELECT
  ticker,
  date,
  momentum_score,
  earnings_quality_score,
  valuation_score,
  volatility_score,
  news_sentiment_score,
  baseline_90d_momentum,
  baseline_90d_quality,
  baseline_90d_volatility,
  baseline_90d_sentiment,
  CASE WHEN baseline_90d_momentum    IS NOT NULL AND baseline_90d_momentum    != 0 THEN momentum_score         / baseline_90d_momentum    END AS vector_shift_ratio_momentum,
  CASE WHEN baseline_90d_quality     IS NOT NULL AND baseline_90d_quality     != 0 THEN earnings_quality_score / baseline_90d_quality     END AS vector_shift_ratio_quality,
  CASE WHEN baseline_90d_volatility  IS NOT NULL AND baseline_90d_volatility  != 0 THEN volatility_score       / baseline_90d_volatility  END AS vector_shift_ratio_volatility,
  CASE WHEN baseline_90d_sentiment   IS NOT NULL AND baseline_90d_sentiment   != 0 THEN news_sentiment_score   / baseline_90d_sentiment   END AS vector_shift_ratio_sentiment
FROM baseline;

ALTER TABLE silver_factset_vectors SET TBLPROPERTIES ('comment' = 'FactSet vectors cleaned and enriched with 90-day baselines and vector_shift_ratio per factor to quantify regime shifts (e.g., NVDA volatility ~1.9x baseline around 2025-06-18).');

-- 2) silver_positions
-- Normalize sectors, compute sleeve daily market value, concentration percent, and flags.
CREATE OR REPLACE TABLE silver_positions AS
WITH base AS (
  SELECT
    position_id,
    CAST(date AS DATE) AS date,
    ticker,
    sleeve,
    CASE
      WHEN LOWER(sector) LIKE 'semiconductor%' THEN 'Semiconductors'
      WHEN LOWER(sector) LIKE 'software%'      THEN 'Software'
      WHEN LOWER(sector) LIKE 'hardware%'      THEN 'Hardware'
      ELSE COALESCE(sector, 'Other')
    END AS sector,
    quantity,
    market_value_usd,
    delta,
    beta,
    COALESCE(concentration_flag, FALSE) AS concentration_flag
  FROM raw_raw_internal_positions
  WHERE date IS NOT NULL AND ticker IS NOT NULL AND sleeve IS NOT NULL
),
agg_sleeve_mv AS (
  SELECT date, sleeve, SUM(market_value_usd) AS sleeve_market_value_usd
  FROM base
  GROUP BY date, sleeve
)
SELECT
  b.position_id,
  b.date,
  b.ticker,
  b.sleeve,
  b.sector,
  b.quantity,
  b.market_value_usd,
  b.delta,
  b.beta,
  b.concentration_flag,
  a.sleeve_market_value_usd,
  CASE WHEN a.sleeve_market_value_usd IS NOT NULL AND a.sleeve_market_value_usd != 0 THEN b.market_value_usd / a.sleeve_market_value_usd END AS concentration_pct,
  CASE WHEN a.sleeve_market_value_usd IS NOT NULL AND a.sleeve_market_value_usd != 0 AND (b.market_value_usd / a.sleeve_market_value_usd) > 0.12 THEN TRUE ELSE FALSE END AS top_positions_flag
FROM base b
JOIN agg_sleeve_mv a
  ON a.date = b.date AND a.sleeve = b.sleeve;

ALTER TABLE silver_positions SET TBLPROPERTIES ('comment' = 'Daily internal positions normalized with sleeve-level market value, concentration_pct, and top_positions_flag (>12%). Used for exposure, sector weights, and concentration KPIs.');

-- 3) silver_orders_execs
-- Clean enums, derive date, urgency flag, and keep granular microstructure fields.
CREATE OR REPLACE TABLE silver_orders_execs AS
SELECT
  order_id,
  parent_order_id,
  CAST(timestamp AS TIMESTAMP) AS timestamp,
  CAST(date AS DATE) AS date,
  ticker,
  sleeve,
  UPPER(side) AS side,
  CASE
    WHEN LOWER(order_type) IN ('market') THEN 'market'
    WHEN LOWER(order_type) IN ('marketable_limit','marketable') THEN 'marketable_limit'
    ELSE 'limit'
  END AS order_type,
  UPPER(venue) AS venue,
  quote_spread_bps,
  top_of_book_depth_shares,
  fill_qty,
  avg_fill_price,
  slippage_bps,
  CASE WHEN LOWER(order_type) IN ('market','marketable_limit') THEN TRUE ELSE FALSE END AS urgency_flag
FROM raw_raw_internal_orders_executions
WHERE ticker IS NOT NULL AND sleeve IS NOT NULL AND timestamp IS NOT NULL;

ALTER TABLE silver_orders_execs SET TBLPROPERTIES ('comment' = 'Atomic orders/executions cleaned with standardized enums, date field, and urgency_flag (market or marketable_limit). Retains microstructure fields (spread, depth, slippage) for execution cost analytics.');

-- 4) silver_risk_policy_changes
-- Normalize scope fields, add change_date_date and change_magnitude.
CREATE OR REPLACE TABLE silver_risk_policy_changes AS
SELECT
  policy_id,
  CAST(change_date AS TIMESTAMP) AS change_date,
  CAST(change_date AS DATE) AS change_date_date,
  scope_sleeve,
  COALESCE(scope_sector, 'All') AS scope_sector,
  LOWER(limit_type) AS limit_type,
  old_threshold,
  new_threshold,
  (old_threshold - new_threshold) AS change_magnitude,
  notes
FROM raw_raw_risk_policy_changes
WHERE change_date IS NOT NULL AND scope_sleeve IS NOT NULL AND limit_type IS NOT NULL;

ALTER TABLE silver_risk_policy_changes SET TBLPROPERTIES ('comment' = 'Risk policy change log standardized with date helpers and change_magnitude = old_threshold - new_threshold. Critical entry 2025-06-18 tightens VAR/gamma for Growth-US.');

-- 5) silver_risk_breaches
-- Standardize enums and derive week_start.
CREATE OR REPLACE TABLE silver_risk_breaches AS
SELECT
  breach_id,
  CAST(date AS DATE) AS date,
  sleeve,
  LOWER(limit_type) AS limit_type,
  breach_count,
  severity_score,
  DATE_TRUNC('WEEK', CAST(date AS DATE)) AS week_start
FROM raw_raw_risk_limit_breaches
WHERE date IS NOT NULL AND sleeve IS NOT NULL AND limit_type IS NOT NULL;

ALTER TABLE silver_risk_breaches SET TBLPROPERTIES ('comment' = 'Daily risk limit breach records standardized with week_start. Used for alignment to policy changes and anomaly windows (e.g., VAR/gamma clusters 2025-06-18..2025-06-20).');

-- =============================
-- GOLD TABLES (AGGREGATIONS)
-- =============================

-- A) gold_date_spine
-- Daily trading calendar covering union of silver sources in 2025-04-01..2025-10-21
CREATE OR REPLACE TABLE gold_date_spine AS
WITH minmax AS (
  SELECT
    MIN(min_date) AS min_d,
    MAX(max_date) AS max_d
  FROM (
    SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM silver_positions UNION ALL
    SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM silver_orders_execs UNION ALL
    SELECT MIN(date) AS min_date, MAX(date) AS max_date FROM silver_risk_breaches
  )
)
SELECT
  d AS date,
  DAYOFWEEK(d) AS dow,
  CASE WHEN DAYOFWEEK(d) IN (1,7) THEN FALSE ELSE TRUE END AS is_weekday,
  DATE_TRUNC('MONTH', d) AS month
FROM (
  SELECT EXPLODE(SEQUENCE((SELECT min_d FROM minmax), (SELECT max_d FROM minmax), INTERVAL 1 DAY)) AS d
);

ALTER TABLE gold_date_spine SET TBLPROPERTIES ('comment' = 'Continuous daily trading spine across the demo window to scaffold joins and ensure zero-activity days are preserved.');

-- B) gold_vector_timeseries
-- Per date, ticker: expose vector_shift_ratio and event_window flag.
CREATE OR REPLACE TABLE gold_vector_timeseries AS
SELECT
  v.date,
  v.ticker,
  v.momentum_score,
  v.earnings_quality_score,
  v.valuation_score,
  v.volatility_score,
  v.news_sentiment_score,
  v.baseline_90d_momentum,
  v.baseline_90d_quality,
  v.baseline_90d_volatility,
  v.baseline_90d_sentiment,
  v.vector_shift_ratio_momentum,
  v.vector_shift_ratio_quality,
  v.vector_shift_ratio_volatility,
  v.vector_shift_ratio_sentiment,
  CASE WHEN v.date BETWEEN DATE('2025-06-15') AND DATE('2025-06-22') THEN TRUE ELSE FALSE END AS event_window
FROM silver_factset_vectors v
JOIN gold_date_spine d ON d.date = v.date;

ALTER TABLE gold_vector_timeseries SET TBLPROPERTIES ('comment' = 'Daily per-ticker vectors with 90-day baselines and shift ratios; includes event_window flag. NVDA expected volatility ratio ~1.9x on 2025-06-18 with quality dipping ~22%.');

-- C) gold_portfolio_exposure
-- Aggregate positions daily by date, sleeve, sector, ticker; include normalization fields.
CREATE OR REPLACE TABLE gold_portfolio_exposure AS
SELECT
  p.date,
  p.sleeve,
  p.sector,
  p.ticker,
  SUM(p.market_value_usd) AS market_value_usd,
  AVG(p.concentration_pct) AS avg_concentration_pct,
  AVG(CASE WHEN p.top_positions_flag THEN 1.0 ELSE 0.0 END) AS top_positions_share,
  AVG(p.delta) AS avg_delta,
  AVG(p.beta) AS avg_beta,
  MAX(p.sleeve_market_value_usd) AS sleeve_market_value_usd
FROM silver_positions p
GROUP BY p.date, p.sleeve, p.sector, p.ticker;

ALTER TABLE gold_portfolio_exposure SET TBLPROPERTIES ('comment' = 'Daily exposure by sleeve/sector/ticker: market_value_usd, concentration share, delta, beta. Supports concentration metrics, sector weights, and attribution of semis de-risking.');

-- D) gold_execution_costs_daily
-- Aggregate orders/executions to daily per date, sleeve, ticker with turnover proxy and cost metrics.
CREATE OR REPLACE TABLE gold_execution_costs_daily AS
WITH daily AS (
  SELECT
    date,
    sleeve,
    ticker,
    SUM(fill_qty) AS shares_traded,
    AVG(slippage_bps) AS avg_slippage_bps,
    AVG(quote_spread_bps) AS avg_quote_spread_bps,
    AVG(CASE WHEN urgency_flag THEN 1.0 ELSE 0.0 END) AS marketable_order_share,
    AVG(top_of_book_depth_shares) AS avg_top_of_book_depth_shares
  FROM silver_orders_execs
  GROUP BY date, sleeve, ticker
),
position_latest AS (
  -- Use positions quantity as shares_outstanding proxy per ticker+sleeve on that date
  SELECT date, sleeve, ticker, SUM(quantity) AS sleeve_ticker_quantity
  FROM silver_positions
  GROUP BY date, sleeve, ticker
)
SELECT
  d.date,
  d.sleeve,
  d.ticker,
  d.shares_traded,
  CASE WHEN p.sleeve_ticker_quantity IS NOT NULL AND p.sleeve_ticker_quantity != 0 THEN d.shares_traded / p.sleeve_ticker_quantity END AS turnover_pct,
  d.avg_slippage_bps,
  d.avg_quote_spread_bps,
  d.marketable_order_share,
  d.avg_top_of_book_depth_shares
FROM daily d
LEFT JOIN position_latest p
  ON p.date = d.date AND p.sleeve = d.sleeve AND p.ticker = d.ticker;

ALTER TABLE gold_execution_costs_daily SET TBLPROPERTIES ('comment' = 'Daily execution costs by sleeve/ticker with turnover_pct proxy (shares_traded / sleeve-position shares). Captures event spike: turnover +35%, slippage ~18bps, spreads wider, depth lower, and urgency share higher.');

-- E) gold_risk_breach_alignment
-- Align risk breaches to policy changes by date proximity (same day or next trading day) and scope.
CREATE OR REPLACE TABLE gold_risk_breach_alignment AS
WITH breaches AS (
  SELECT date, sleeve, limit_type, breach_count, severity_score FROM silver_risk_breaches
),
policies AS (
  SELECT change_date_date AS change_date, scope_sleeve AS sleeve, limit_type, change_magnitude FROM silver_risk_policy_changes
),
joined AS (
  SELECT
    b.date AS breach_date,
    b.sleeve,
    b.limit_type,
    b.breach_count,
    b.severity_score,
    p.change_date,
    p.change_magnitude,
    CASE
      WHEN p.change_date = b.date THEN TRUE
      WHEN p.change_date = date_add(b.date, -1) THEN TRUE
      WHEN p.change_date = date_add(b.date, 1) THEN TRUE
      ELSE FALSE
    END AS alignment_flag,
    ABS(DATEDIFF(b.date, p.change_date)) AS time_delta_days
  FROM breaches b
  LEFT JOIN policies p
    ON p.sleeve = b.sleeve AND p.limit_type = b.limit_type
)
SELECT
  breach_date AS date,
  sleeve,
  limit_type,
  SUM(breach_count) AS total_breach_count,
  AVG(severity_score) AS avg_severity,
  MAX(CASE WHEN alignment_flag THEN change_magnitude ELSE NULL END) AS aligned_change_magnitude,
  MIN(CASE WHEN alignment_flag THEN time_delta_days ELSE NULL END) AS min_time_delta_days,
  AVG(CASE WHEN alignment_flag THEN 1.0 ELSE 0.0 END) AS alignment_rate
FROM joined
GROUP BY breach_date, sleeve, limit_type;

ALTER TABLE gold_risk_breach_alignment SET TBLPROPERTIES ('comment' = 'Daily breaches aligned to policy changes by sleeve and limit_type with proximity logic (same/adjacent day). Provides total_breach_count, avg_severity, alignment_rate, and change magnitudes to quantify correlation during 2025-06-18..06-20.');

-- F) gold_pnl_timeseries
-- Approximate sleeve-level daily PnL% using market_value changes and momentum as a proxy for returns when price series absent.
CREATE OR REPLACE TABLE gold_pnl_timeseries AS
WITH mv AS (
  SELECT date, sleeve, SUM(market_value_usd) AS sleeve_mv_usd
  FROM silver_positions
  GROUP BY date, sleeve
),
returns_proxy AS (
  -- Use average momentum per sleeve-weighted tickers as a proxy for return signal
  SELECT p.date, p.sleeve,
         AVG(v.momentum_score) AS sleeve_momentum_proxy
  FROM silver_positions p
  JOIN silver_factset_vectors v
    ON v.date = p.date AND v.ticker = p.ticker
  GROUP BY p.date, p.sleeve
),
joined AS (
  SELECT
    m.date,
    m.sleeve,
    m.sleeve_mv_usd,
    r.sleeve_momentum_proxy,
    LAG(m.sleeve_mv_usd) OVER (PARTITION BY m.sleeve ORDER BY m.date) AS prev_mv
  FROM mv m
  LEFT JOIN returns_proxy r
    ON r.date = m.date AND r.sleeve = m.sleeve
)
SELECT
  date,
  sleeve,
  CASE WHEN prev_mv IS NOT NULL AND prev_mv != 0 THEN (sleeve_mv_usd - prev_mv) / prev_mv END AS pnl_pct,
  SUM(CASE WHEN prev_mv IS NOT NULL AND prev_mv != 0 THEN (sleeve_mv_usd - prev_mv) / prev_mv ELSE 0.0 END)
    OVER (PARTITION BY sleeve ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_pnl_pct,
  sleeve_momentum_proxy
FROM joined;

ALTER TABLE gold_pnl_timeseries SET TBLPROPERTIES ('comment' = 'Sleeve-level PnL% derived from daily market value changes, with momentum proxy to align narrative (-2.3% drawdown late June for Growth-US). Includes cumulative_pnl_pct for drawdown tracking.');

-- G) gold_agent_qa_bridge
-- Denormalized daily analytical bridge combining vectors, breaches, orders (costs), exposure, and PnL.
CREATE OR REPLACE TABLE gold_agent_qa_bridge AS
WITH vec AS (
  SELECT date, ticker,
         vector_shift_ratio_momentum,
         vector_shift_ratio_quality,
         vector_shift_ratio_volatility,
         vector_shift_ratio_sentiment
  FROM gold_vector_timeseries
),
exposure AS (
  SELECT date, sleeve, sector, ticker,
         market_value_usd,
         avg_concentration_pct AS concentration_pct
  FROM gold_portfolio_exposure
),
exec AS (
  SELECT date, sleeve, ticker,
         turnover_pct,
         avg_slippage_bps,
         avg_quote_spread_bps,
         marketable_order_share
  FROM gold_execution_costs_daily
),
breach AS (
  SELECT date, sleeve,
         SUM(total_breach_count) AS breach_count
  FROM gold_risk_breach_alignment
  GROUP BY date, sleeve
),
policy AS (
  SELECT change_date_date AS date, scope_sleeve AS sleeve,
         MAX(change_magnitude) AS policy_change_magnitude
  FROM silver_risk_policy_changes
  GROUP BY change_date_date, scope_sleeve
),
pnl AS (
  SELECT date, sleeve, pnl_pct FROM gold_pnl_timeseries
)
SELECT
  COALESCE(e.date, x.date) AS date,
  COALESCE(e.ticker, v.ticker) AS ticker,
  e.sleeve,
  e.sector,
  v.vector_shift_ratio_momentum,
  v.vector_shift_ratio_quality,
  v.vector_shift_ratio_volatility,
  v.vector_shift_ratio_sentiment,
  b.breach_count,
  p.policy_change_magnitude,
  x.turnover_pct,
  x.avg_slippage_bps AS slippage_bps,
  x.avg_quote_spread_bps AS quote_spread_bps,
  x.marketable_order_share,
  e.concentration_pct,
  pn.pnl_pct
FROM exposure e
LEFT JOIN vec v ON v.date = e.date AND v.ticker = e.ticker
LEFT JOIN exec x ON x.date = e.date AND x.sleeve = e.sleeve AND x.ticker = e.ticker
LEFT JOIN breach b ON b.date = e.date AND b.sleeve = e.sleeve
LEFT JOIN policy p ON p.date = e.date AND p.sleeve = e.sleeve
LEFT JOIN pnl pn ON pn.date = e.date AND pn.sleeve = e.sleeve;

ALTER TABLE gold_agent_qa_bridge SET TBLPROPERTIES ('comment' = 'Unified daily bridge for Q&A: date, ticker, sleeve, sector, vector shift ratios, breach_count, policy_change_magnitude, turnover_pct, slippage_bps, quote_spread_bps, marketable_order_share, concentration_pct, pnl_pct. Supports executive questions and governance citations (Investor_Report collection).');

-- H) KPI single-row counters for dashboards

-- 1. gold_kpi_event_window_stats
-- Provides magnitudes in the event window for Growth-US (turnover, slippage, breaches) and NVDA volatility shift.
CREATE OR REPLACE TABLE gold_kpi_event_window_stats AS
WITH ev AS (
  SELECT DATE('2025-06-15') AS start_d, DATE('2025-06-22') AS end_d
),
turnover AS (
  SELECT AVG(turnover_pct) AS avg_turnover_pct_event,
         AVG(avg_slippage_bps) AS avg_slippage_bps_event
  FROM gold_execution_costs_daily, ev
  WHERE date BETWEEN ev.start_d AND ev.end_d AND sleeve = 'Growth-US'
),
breaches AS (
  SELECT SUM(total_breach_count) AS total_breaches_event
  FROM gold_risk_breach_alignment, ev
  WHERE date BETWEEN ev.start_d AND ev.end_d AND sleeve = 'Growth-US'
),
vol_nvda AS (
  SELECT MAX(vector_shift_ratio_volatility) AS max_nvda_vol_ratio
  FROM gold_vector_timeseries, ev
  WHERE ticker = 'NVDA' AND date BETWEEN ev.start_d AND ev.end_d
)
SELECT
  (SELECT avg_turnover_pct_event FROM turnover) AS avg_turnover_pct_event,
  (SELECT avg_slippage_bps_event FROM turnover) AS avg_slippage_bps_event,
  (SELECT total_breaches_event FROM breaches) AS total_breaches_event,
  (SELECT max_nvda_vol_ratio FROM vol_nvda) AS max_nvda_vol_ratio;

ALTER TABLE gold_kpi_event_window_stats SET TBLPROPERTIES ('comment' = 'Single-row KPIs summarizing event window: Growth-US turnover/slippage, total breaches, and NVDA max volatility ratio.');

-- 2. gold_kpi_latest_pnl_drawdown
-- Latest cumulative PnL per sleeve to show drawdown.
CREATE OR REPLACE TABLE gold_kpi_latest_pnl_drawdown AS
WITH latest AS (SELECT MAX(date) AS max_d FROM gold_pnl_timeseries)
SELECT
  sleeve,
  FIRST(cumulative_pnl_pct) AS latest_cumulative_pnl_pct
FROM (
  SELECT sleeve, cumulative_pnl_pct
  FROM gold_pnl_timeseries, latest
  WHERE date = latest.max_d
)
GROUP BY sleeve;

ALTER TABLE gold_kpi_latest_pnl_drawdown SET TBLPROPERTIES ('comment' = 'Per-sleeve latest cumulative PnL%, used to surface drawdown (-2.3% in Growth-US in late June) and recovery.');

-- 3. gold_breach_contributors
-- Ranked contributors (ticker/sector) to breaches and turnover during event window.
CREATE OR REPLACE TABLE gold_breach_contributors AS
WITH ev AS (
  SELECT DATE('2025-06-15') AS start_d, DATE('2025-06-22') AS end_d
),
turn AS (
  SELECT e.date, e.sleeve, e.ticker, e.turnover_pct
  FROM gold_execution_costs_daily e, ev
  WHERE e.date BETWEEN ev.start_d AND ev.end_d AND e.sleeve = 'Growth-US'
),
br AS (
  SELECT b.date, b.sleeve, b.total_breach_count
  FROM gold_risk_breach_alignment b, ev
  WHERE b.date BETWEEN ev.start_d AND ev.end_d AND b.sleeve = 'Growth-US'
)
SELECT
  p.date,
  p.sleeve,
  p.sector,
  p.ticker,
  COALESCE(t.turnover_pct, 0.0) AS turnover_pct,
  COALESCE(b.total_breach_count, 0) AS breach_count
FROM gold_portfolio_exposure p
LEFT JOIN turn t ON t.date = p.date AND t.sleeve = p.sleeve AND t.ticker = p.ticker
LEFT JOIN br   b ON b.date = p.date AND b.sleeve = p.sleeve
WHERE p.sleeve = 'Growth-US' AND p.date BETWEEN DATE('2025-06-15') AND DATE('2025-06-22');

ALTER TABLE gold_breach_contributors SET TBLPROPERTIES ('comment' = 'Event-window breakdown of turnover and breach counts by ticker/sector within Growth-US to identify contributors (e.g., NVDA, AMD in Semiconductors).');

-- 4. gold_policy_change_log_table
-- Detailed policy change log for root-cause table visualization.
CREATE OR REPLACE TABLE gold_policy_change_log_table AS
SELECT
  change_date_date,
  scope_sleeve,
  limit_type,
  old_threshold,
  new_threshold,
  (old_threshold - new_threshold) AS change_magnitude,
  notes
FROM silver_risk_policy_changes;

ALTER TABLE gold_policy_change_log_table SET TBLPROPERTIES ('comment' = 'Root-cause table source: change_date_date, scope_sleeve, limit_type, thresholds, change_magnitude, notes. References 2025-06-18 tightening; aligns with Investor_Report governance context.');

-- 5. gold_weekly_breach_counts_by_type
-- Weekly breach counts by sleeve and limit_type for bar visualization.
CREATE OR REPLACE TABLE gold_weekly_breach_counts_by_type AS
SELECT
  DATE_TRUNC('WEEK', date) AS week_start,
  sleeve,
  limit_type,
  SUM(total_breach_count) AS weekly_breach_count
FROM gold_risk_breach_alignment
GROUP BY DATE_TRUNC('WEEK', date), sleeve, limit_type;

ALTER TABLE gold_weekly_breach_counts_by_type SET TBLPROPERTIES ('comment' = 'Weekly breach counts grouped by sleeve and limit_type to visualize clustering around 2025-06-15..2025-06-22.');

-- 6. gold_latest_aum_counter
-- AUM ($B) latest month counter from sleeve market_value_usd.
CREATE OR REPLACE TABLE gold_latest_aum_counter AS
WITH latest_month AS (
  SELECT MAX(DATE_TRUNC('MONTH', date)) AS lm FROM gold_portfolio_exposure
),
by_sleeve AS (
  SELECT sleeve, SUM(market_value_usd) AS sleeve_mv
  FROM gold_portfolio_exposure, latest_month
  WHERE DATE_TRUNC('MONTH', date) = latest_month.lm
  GROUP BY sleeve
)
SELECT SUM(sleeve_mv) / 1000000000.0 AS aum_billion
FROM by_sleeve;

ALTER TABLE gold_latest_aum_counter SET TBLPROPERTIES ('comment' = 'Single-row counter: latest month firm AUM in billions computed from sleeve market_value_usd.');

-- 7. gold_trailing_30d_execution_costs
-- Trailing 30-day average execution costs for counter visualization.
CREATE OR REPLACE TABLE gold_trailing_30d_execution_costs AS
WITH latest AS (SELECT MAX(date) AS max_d FROM gold_execution_costs_daily)
SELECT
  AVG(avg_slippage_bps) AS trailing30_avg_slippage_bps,
  AVG(avg_quote_spread_bps) AS trailing30_avg_quote_spread_bps
FROM gold_execution_costs_daily, latest
WHERE date BETWEEN date_sub(latest.max_d, 30) AND latest.max_d;

ALTER TABLE gold_trailing_30d_execution_costs SET TBLPROPERTIES ('comment' = 'Single-row counter: trailing 30d averages of slippage_bps and quote_spread_bps for execution quality.');

-- 8. gold_open_breaches_7d
-- Open Breaches (7d) counter.
CREATE OR REPLACE TABLE gold_open_breaches_7d AS
WITH latest AS (SELECT MAX(date) AS max_d FROM gold_risk_breach_alignment)
SELECT
  SUM(total_breach_count) AS breach_count_7d
FROM gold_risk_breach_alignment, latest
WHERE date BETWEEN date_sub(latest.max_d, 7) AND latest.max_d;

ALTER TABLE gold_open_breaches_7d SET TBLPROPERTIES ('comment' = 'Single-row counter: total breach_count in the last 7 trading days from gold_risk_breach_alignment.');

-- 9. gold_execution_costs_vs_spreads_timeseries
-- Daily time series of execution costs for line visualization.
CREATE OR REPLACE TABLE gold_execution_costs_vs_spreads_timeseries AS
SELECT
  date,
  sleeve,
  ticker,
  avg_slippage_bps,
  avg_quote_spread_bps
FROM gold_execution_costs_daily;

ALTER TABLE gold_execution_costs_vs_spreads_timeseries SET TBLPROPERTIES ('comment' = 'Daily time series with avg_slippage_bps and avg_quote_spread_bps, filterable by sleeve and ticker to show event cost uplift.');
