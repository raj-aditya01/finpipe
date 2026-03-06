-- =============================================================================
-- sql/001_create_star_schema.sql — Star Schema for Stock Market Data
-- =============================================================================
--
-- WHAT YOU LEARN HERE:
--   1. Star Schema — the foundation of every data warehouse
--   2. Fact vs Dimension tables — what goes where
--   3. DDL (Data Definition Language) — CREATE TABLE, constraints
--   4. Snowflake-specific features — VARIANT, CLUSTER BY, COMMENT
--   5. Data types — choosing the right type for each column
--
-- STAR SCHEMA IN 30 SECONDS:
--   A star schema has ONE central "fact" table surrounded by "dimension" tables.
--   
--     dim_date ──→ fact_stock_prices ←── dim_symbol
--                         ↑
--                    dim_exchange
--
--   - Fact table: EVENTS with NUMBERS (prices, volumes) — grows every day
--   - Dimension tables: DESCRIPTORS (company name, sector) — rarely change
--   - Why "star"? If you draw the schema, it looks like a star ⭐
--
-- WHY STAR SCHEMA (not one big table)?
--   1. Query speed — dimensions are small, facts are big. Joins are fast.
--   2. Storage — "Tata Consultancy Services" stored once, not in every row
--   3. Maintainability — company changes sector? Update ONE row in dim_symbol
--   4. Analytics — GROUP BY sector works naturally with dimensions
--   5. Industry standard — every BI tool expects this structure
--
-- HOW TO RUN:
--   Copy-paste into Snowflake UI (worksheets), OR:
--   Run via CLI: finpipe sql execute --file sql/001_create_star_schema.sql
-- =============================================================================


-- Use the database and schema from your .env
-- The connector already uses the database/schema from your .env
-- No hardcoded USE DATABASE here — it respects your SNOWFLAKE_DATABASE setting


-- =============================================================================
-- DIMENSION: dim_exchange — Stock Exchanges
-- =============================================================================
-- Dimension tables describe the "who/what/where" of your data.
-- This one describes which exchanges exist and their properties.
--
-- SLOWLY CHANGING DIMENSION (SCD):
--   Exchange data rarely changes. When it does, you have options:
--   - Type 1: Overwrite old value (simple, lose history)
--   - Type 2: Add new row with effective_date (keep history)
--   We use Type 1 here (simple). For a portfolio tracking app, use Type 2.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_exchange (
    -- IDENTITY auto-generates unique IDs: 1, 2, 3...
    -- This is the SURROGATE KEY — a meaningless number used only for joins.
    -- We don't use the exchange name as PK because PKs should never change.
    exchange_id     INTEGER AUTOINCREMENT PRIMARY KEY,
    
    -- NATURAL KEY — the real-world identifier. Has a UNIQUE constraint.
    exchange_code   VARCHAR(10) NOT NULL UNIQUE,    -- "NSE", "BSE"
    exchange_name   VARCHAR(100) NOT NULL,          -- "National Stock Exchange"
    country         VARCHAR(50) DEFAULT 'India',
    currency        VARCHAR(10) DEFAULT 'INR',
    timezone        VARCHAR(50) DEFAULT 'Asia/Kolkata',
    
    -- Audit columns — track when rows were created/updated
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    
    -- COMMENT: Snowflake-specific feature — adds metadata visible in SHOW TABLES
) COMMENT = 'Dimension: Stock exchanges (NSE, BSE)';


-- =============================================================================
-- DIMENSION: dim_symbol — Stock Symbols / Companies
-- =============================================================================
-- Each row = one stock listing. A company can have multiple listings
-- (e.g., TCS on NSE and TCS on BSE are separate rows).
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_symbol (
    symbol_id       INTEGER AUTOINCREMENT PRIMARY KEY,
    
    symbol          VARCHAR(20) NOT NULL,           -- "TCS", "RELIANCE"
    company_name    VARCHAR(200) DEFAULT '',
    sector          VARCHAR(100) DEFAULT '',
    industry        VARCHAR(100) DEFAULT '',
    isin            VARCHAR(20) DEFAULT '',          -- International Securities ID
    exchange_code   VARCHAR(10) NOT NULL,            -- FK to dim_exchange
    
    -- Is this stock currently active/tradeable?
    is_active       BOOLEAN DEFAULT TRUE,
    listed_date     DATE,
    
    -- Audit
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Composite UNIQUE — same symbol on same exchange = one row
    UNIQUE(symbol, exchange_code)
    
) COMMENT = 'Dimension: Stock symbols and company metadata';


-- =============================================================================
-- DIMENSION: dim_date — Calendar / Date Dimension
-- =============================================================================
-- The date dimension is the MOST IMPORTANT dimension in analytics.
-- Instead of deriving "what quarter is 2026-03-05?" in every query,
-- you pre-compute it once and JOIN to get it.
--
-- INTERVIEW QUESTION: "Why not just use the date column directly?"
--   Answer: A date dimension lets you:
--   1. Add business logic: is_trading_day, is_holiday, fiscal_quarter
--   2. Query by week/month/quarter without complex date functions
--   3. Handle missing dates (weekends have no stock data but exist in dim)
--   4. Support multiple calendars (fiscal year starts in April in India)
--
-- PRO TIP: Pre-populate this for the next 10 years. It's only ~3650 rows.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        DATE PRIMARY KEY,                -- 2026-03-05
    
    -- Calendar attributes
    day_of_week     INTEGER,                         -- 1=Monday ... 7=Sunday
    day_name        VARCHAR(10),                     -- "Wednesday"
    day_of_month    INTEGER,                         -- 5
    day_of_year     INTEGER,                         -- 64
    
    week_of_year    INTEGER,                         -- 10
    month_number    INTEGER,                         -- 3
    month_name      VARCHAR(10),                     -- "March"
    quarter         INTEGER,                         -- 1
    year            INTEGER,                         -- 2026
    
    -- Business attributes
    is_weekend      BOOLEAN,                         -- True for Sat/Sun
    is_trading_day  BOOLEAN DEFAULT TRUE,            -- False for holidays
    is_month_end    BOOLEAN DEFAULT FALSE,
    is_quarter_end  BOOLEAN DEFAULT FALSE,
    
    -- Indian fiscal year (April to March)
    fiscal_year     INTEGER,                         -- 2026 (FY2025-26)
    fiscal_quarter  INTEGER                          -- Q4 (Jan-Mar)
    
) COMMENT = 'Dimension: Calendar with trading day flags';


-- =============================================================================
-- FACT: fact_stock_prices — Daily Stock Price Data
-- =============================================================================
-- This is the CENTRAL table — every query touches this.
-- It grows daily: ~5000 stocks × 2 exchanges × 250 trading days = 2.5M rows/year.
--
-- DESIGN DECISIONS:
--   1. Composite PK (symbol + exchange + date) — prevents duplicate rows
--   2. CLUSTER BY (trade_date) — Snowflake organizes data by date for fast scans
--   3. No foreign keys enforced — Snowflake supports them for documentation only
--      (it doesn't enforce them for performance reasons)
--   4. All prices are NUMBER(18,4) — 4 decimal places, handles ₹0.0001 to ₹9999999999999.9999
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact_stock_prices (
    -- Composite key — uniquely identifies each row
    symbol          VARCHAR(20) NOT NULL,
    exchange_code   VARCHAR(10) NOT NULL,
    trade_date      DATE NOT NULL,
    
    -- Price data (OHLCV = Open, High, Low, Close, Volume)
    open_price      NUMBER(18,4),
    high_price      NUMBER(18,4),
    low_price       NUMBER(18,4),
    close_price     NUMBER(18,4),
    
    -- Volume
    volume          NUMBER(18,0) DEFAULT 0,
    
    -- Computed columns (pre-calculated for fast analytics)
    daily_range     NUMBER(18,4),     -- high - low
    change_pct      NUMBER(10,4),     -- (close - open) / open * 100
    
    -- Metadata
    source          VARCHAR(20) DEFAULT 'yfinance',   -- Where data came from
    loaded_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    -- Composite primary key: one row per symbol per exchange per date
    PRIMARY KEY (symbol, exchange_code, trade_date)
    
) 
-- CLUSTER BY tells Snowflake how to physically organize data on disk.
-- Since most queries filter/sort by date, clustering by date makes scans faster.
-- Snowflake handles this automatically in the background (unlike manual partitioning).
CLUSTER BY (trade_date)
COMMENT = 'Fact: Daily OHLCV stock prices from NSE/BSE';


-- =============================================================================
-- SEED: Insert exchange dimension data
-- =============================================================================
-- MERGE = "upsert" — INSERT if not exists, UPDATE if exists.
-- This is IDEMPOTENT — running it 100 times has the same result as running once.
-- Critical for data pipelines that may re-run on failures.
-- =============================================================================

MERGE INTO dim_exchange AS target
USING (
    SELECT 'NSE' AS exchange_code, 'National Stock Exchange of India' AS exchange_name UNION ALL
    SELECT 'BSE', 'Bombay Stock Exchange'
) AS source
ON target.exchange_code = source.exchange_code
WHEN NOT MATCHED THEN
    INSERT (exchange_code, exchange_name)
    VALUES (source.exchange_code, source.exchange_name);


-- =============================================================================
-- SEED: Populate date dimension (2020-01-01 to 2030-12-31)
-- =============================================================================
-- This uses Snowflake's GENERATOR to create rows.
-- GENERATOR(ROWCOUNT => 4018) creates 4018 rows (about 11 years of dates).
-- =============================================================================

MERGE INTO dim_date AS target
USING (
    SELECT
        DATEADD(DAY, seq4(), '2020-01-01'::DATE)                    AS date_key,
        DAYOFWEEKISO(DATEADD(DAY, seq4(), '2020-01-01'::DATE))      AS day_of_week,
        DAYNAME(DATEADD(DAY, seq4(), '2020-01-01'::DATE))           AS day_name,
        DAY(DATEADD(DAY, seq4(), '2020-01-01'::DATE))               AS day_of_month,
        DAYOFYEAR(DATEADD(DAY, seq4(), '2020-01-01'::DATE))         AS day_of_year,
        WEEKOFYEAR(DATEADD(DAY, seq4(), '2020-01-01'::DATE))        AS week_of_year,
        MONTH(DATEADD(DAY, seq4(), '2020-01-01'::DATE))             AS month_number,
        MONTHNAME(DATEADD(DAY, seq4(), '2020-01-01'::DATE))         AS month_name,
        QUARTER(DATEADD(DAY, seq4(), '2020-01-01'::DATE))           AS quarter,
        YEAR(DATEADD(DAY, seq4(), '2020-01-01'::DATE))              AS year,
        DAYOFWEEKISO(DATEADD(DAY, seq4(), '2020-01-01'::DATE)) IN (6, 7)  AS is_weekend,
        DAYOFWEEKISO(DATEADD(DAY, seq4(), '2020-01-01'::DATE)) NOT IN (6, 7) AS is_trading_day,
        -- Indian fiscal year: April to March
        CASE 
            WHEN MONTH(DATEADD(DAY, seq4(), '2020-01-01'::DATE)) >= 4 
            THEN YEAR(DATEADD(DAY, seq4(), '2020-01-01'::DATE)) + 1
            ELSE YEAR(DATEADD(DAY, seq4(), '2020-01-01'::DATE))
        END AS fiscal_year,
        CASE
            WHEN MONTH(DATEADD(DAY, seq4(), '2020-01-01'::DATE)) BETWEEN 4 AND 6 THEN 1
            WHEN MONTH(DATEADD(DAY, seq4(), '2020-01-01'::DATE)) BETWEEN 7 AND 9 THEN 2
            WHEN MONTH(DATEADD(DAY, seq4(), '2020-01-01'::DATE)) BETWEEN 10 AND 12 THEN 3
            ELSE 4
        END AS fiscal_quarter
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
) AS source
ON target.date_key = source.date_key
WHEN NOT MATCHED THEN
    INSERT (date_key, day_of_week, day_name, day_of_month, day_of_year,
            week_of_year, month_number, month_name, quarter, year,
            is_weekend, is_trading_day, fiscal_year, fiscal_quarter)
    VALUES (source.date_key, source.day_of_week, source.day_name, source.day_of_month, 
            source.day_of_year, source.week_of_year, source.month_number, source.month_name,
            source.quarter, source.year, source.is_weekend, source.is_trading_day,
            source.fiscal_year, source.fiscal_quarter);
