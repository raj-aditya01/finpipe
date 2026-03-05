-- =============================================================================
-- sql/002_analytics_queries.sql — Learn SQL by Running These Queries
-- =============================================================================
--
-- WHAT YOU LEARN HERE:
--   1. Aggregations — SUM, AVG, COUNT, MIN, MAX
--   2. Window Functions — RANK, LAG, LEAD, MOVING AVERAGE (interview favorites!)
--   3. CTEs (Common Table Expressions) — readable subqueries
--   4. JOINs — connecting fact and dimension tables
--   5. Date arithmetic — intervals, date_trunc, date parts
--   6. CASE/WHEN — conditional logic in SQL
--
-- RUN THESE one by one in Snowflake UI or via the CLI.
-- Each query has comments explaining the SQL concept.
-- =============================================================================


-- =============================================================================
-- QUERY 1: Basic aggregation — Average close price per stock
-- =============================================================================
-- CONCEPT: GROUP BY + aggregate functions
-- GROUP BY = "for each unique value of X, compute Y"
-- =============================================================================

SELECT 
    symbol,
    exchange_code,
    COUNT(*)                        AS total_trading_days,
    ROUND(AVG(close_price), 2)      AS avg_close_price,
    ROUND(MIN(close_price), 2)      AS min_close_price,
    ROUND(MAX(close_price), 2)      AS max_close_price,
    SUM(volume)                     AS total_volume
FROM fact_stock_prices
GROUP BY symbol, exchange_code
ORDER BY avg_close_price DESC;


-- =============================================================================
-- QUERY 2: Latest price per stock (most recent trading day)
-- =============================================================================
-- CONCEPT: Subquery with MAX() to find the latest date
-- This is a very common pattern in data engineering.
-- =============================================================================

SELECT f.*
FROM fact_stock_prices f
INNER JOIN (
    SELECT symbol, exchange_code, MAX(trade_date) AS latest_date
    FROM fact_stock_prices
    GROUP BY symbol, exchange_code
) latest ON f.symbol = latest.symbol 
        AND f.exchange_code = latest.exchange_code
        AND f.trade_date = latest.latest_date
ORDER BY f.symbol;


-- =============================================================================
-- QUERY 3: Window Function — Day-over-Day price change
-- =============================================================================
-- CONCEPT: LAG() window function
-- LAG(column, N) = value of `column` from N rows ago (within the window)
--
-- Window functions are THE MOST IMPORTANT SQL concept for data engineering.
-- They compute values across rows WITHOUT collapsing them like GROUP BY.
--
-- INTERVIEW QUESTION: "Calculate day-over-day price change"
-- Answer: Use LAG() to get previous day's price, then subtract.
-- =============================================================================

SELECT 
    symbol,
    exchange_code,
    trade_date,
    close_price,
    
    -- LAG(close_price, 1) = close_price from 1 row ago (previous trading day)
    -- PARTITION BY symbol = restart the window for each stock
    -- ORDER BY trade_date = define "previous" as earlier date
    LAG(close_price, 1) OVER (
        PARTITION BY symbol, exchange_code 
        ORDER BY trade_date
    ) AS prev_close,
    
    -- Day-over-day change in rupees
    close_price - LAG(close_price, 1) OVER (
        PARTITION BY symbol, exchange_code 
        ORDER BY trade_date
    ) AS daily_change,
    
    -- Day-over-day change as percentage
    ROUND(
        (close_price - LAG(close_price, 1) OVER (
            PARTITION BY symbol, exchange_code 
            ORDER BY trade_date
        )) / NULLIF(LAG(close_price, 1) OVER (
            PARTITION BY symbol, exchange_code 
            ORDER BY trade_date
        ), 0) * 100
    , 2) AS daily_change_pct

FROM fact_stock_prices
WHERE symbol = 'TCS' AND exchange_code = 'NSE'
ORDER BY trade_date DESC
LIMIT 30;


-- =============================================================================
-- QUERY 4: Moving Average (7-day and 30-day) — Technical Analysis
-- =============================================================================
-- CONCEPT: Window function with ROWS BETWEEN (sliding window)
-- 
-- Moving averages smooth out daily noise to show the trend.
-- - 7-day MA: short-term trend (traders use this)
-- - 30-day MA: medium-term trend
-- When short MA crosses above long MA = "Golden Cross" (bullish signal)
-- When short MA crosses below long MA = "Death Cross" (bearish signal)
-- =============================================================================

SELECT 
    symbol,
    trade_date,
    close_price,
    
    -- 7-day moving average: average of last 7 trading days (including today)
    ROUND(AVG(close_price) OVER (
        PARTITION BY symbol, exchange_code
        ORDER BY trade_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW  -- 6 before + current = 7 days
    ), 2) AS ma_7day,
    
    -- 30-day moving average
    ROUND(AVG(close_price) OVER (
        PARTITION BY symbol, exchange_code
        ORDER BY trade_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS ma_30day,
    
    -- Volume 7-day moving average (is trading activity increasing?)
    ROUND(AVG(volume) OVER (
        PARTITION BY symbol, exchange_code
        ORDER BY trade_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 0) AS avg_volume_7day

FROM fact_stock_prices
WHERE symbol = 'TCS' AND exchange_code = 'NSE'
ORDER BY trade_date DESC
LIMIT 60;


-- =============================================================================
-- QUERY 5: RANK — Top gainers and losers per day
-- =============================================================================
-- CONCEPT: RANK() / ROW_NUMBER() / DENSE_RANK() window functions
--
-- INTERVIEW QUESTION: "Find the top 5 gaining stocks per day"
-- ROW_NUMBER: 1, 2, 3, 4, 5 (no ties)
-- RANK:       1, 2, 2, 4, 5 (ties share same rank, skip next)
-- DENSE_RANK: 1, 2, 2, 3, 4 (ties share rank, don't skip)
-- =============================================================================

WITH daily_changes AS (
    SELECT
        symbol,
        exchange_code,
        trade_date,
        close_price,
        change_pct,
        -- Rank by highest gain (change_pct DESC) per date
        RANK() OVER (
            PARTITION BY trade_date, exchange_code
            ORDER BY change_pct DESC
        ) AS gain_rank,
        -- Rank by biggest loss (change_pct ASC = most negative first)
        RANK() OVER (
            PARTITION BY trade_date, exchange_code
            ORDER BY change_pct ASC
        ) AS loss_rank
    FROM fact_stock_prices
    WHERE change_pct IS NOT NULL
)
SELECT *
FROM daily_changes
WHERE gain_rank <= 5 OR loss_rank <= 5
ORDER BY trade_date DESC, gain_rank;


-- =============================================================================
-- QUERY 6: CTE + JOIN — Monthly performance by sector
-- =============================================================================
-- CONCEPT: CTEs (WITH clause) + dimension JOINs
-- 
-- CTEs are "named subqueries" that make complex SQL readable.
-- Think of them as temporary variables in SQL.
-- 
-- This query joins fact_stock_prices → dim_symbol (for sector info)
-- and groups by month + sector to show sector-level performance.
-- =============================================================================

WITH monthly_prices AS (
    -- Step 1: Get first and last close price per stock per month
    SELECT
        f.symbol,
        f.exchange_code,
        DATE_TRUNC('MONTH', f.trade_date)           AS month_start,
        FIRST_VALUE(f.close_price) OVER (
            PARTITION BY f.symbol, f.exchange_code, DATE_TRUNC('MONTH', f.trade_date)
            ORDER BY f.trade_date
        ) AS month_open,
        LAST_VALUE(f.close_price) OVER (
            PARTITION BY f.symbol, f.exchange_code, DATE_TRUNC('MONTH', f.trade_date)
            ORDER BY f.trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS month_close
    FROM fact_stock_prices f
),
monthly_returns AS (
    -- Step 2: Calculate monthly return per stock
    SELECT DISTINCT
        symbol,
        exchange_code,
        month_start,
        month_open,
        month_close,
        ROUND((month_close - month_open) / NULLIF(month_open, 0) * 100, 2) AS monthly_return_pct
    FROM monthly_prices
)
-- Step 3: Join to dimension table for sector info, aggregate by sector
SELECT
    d.sector,
    mr.month_start,
    COUNT(DISTINCT mr.symbol)                     AS stocks_in_sector,
    ROUND(AVG(mr.monthly_return_pct), 2)          AS avg_sector_return_pct,
    ROUND(MIN(mr.monthly_return_pct), 2)          AS worst_stock_return_pct,
    ROUND(MAX(mr.monthly_return_pct), 2)          AS best_stock_return_pct
FROM monthly_returns mr
JOIN dim_symbol d ON mr.symbol = d.symbol AND mr.exchange_code = d.exchange_code
WHERE d.sector IS NOT NULL AND d.sector != ''
GROUP BY d.sector, mr.month_start
ORDER BY mr.month_start DESC, avg_sector_return_pct DESC;


-- =============================================================================
-- QUERY 7: Date dimension JOIN — Trading days analysis
-- =============================================================================
-- CONCEPT: Using the date dimension for calendar intelligence
-- This shows why a date dimension table is valuable.
-- =============================================================================

SELECT
    dd.month_name,
    dd.year,
    dd.fiscal_quarter,
    COUNT(DISTINCT f.trade_date)    AS trading_days,
    COUNT(DISTINCT f.symbol)        AS stocks_traded,
    ROUND(AVG(f.close_price), 2)    AS avg_close_across_all,
    SUM(f.volume)                   AS total_volume
FROM fact_stock_prices f
JOIN dim_date dd ON f.trade_date = dd.date_key
WHERE dd.is_weekend = FALSE
GROUP BY dd.month_name, dd.year, dd.fiscal_quarter, dd.month_number
ORDER BY dd.year DESC, dd.month_number DESC;
