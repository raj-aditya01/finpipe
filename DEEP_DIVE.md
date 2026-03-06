# finpipe — Complete Technical Deep Dive

> Everything built, every concept learned, every bug squashed.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Architecture & Data Flow](#2-architecture--data-flow)
3. [Module-by-Module Breakdown](#3-module-by-module-breakdown)
   - [3.1 Configuration (config/)](#31-configuration-config)
   - [3.2 Connectors (connectors/)](#32-connectors-connectors)
   - [3.3 Models (models/)](#33-models-models)
   - [3.4 Ingestion (ingestion/)](#34-ingestion-ingestion)
   - [3.5 CLI (cli.py)](#35-cli-clipy)
   - [3.6 SQL (sql/)](#36-sql-sql)
   - [3.7 Tests (tests/)](#37-tests-tests)
   - [3.8 Docker (Dockerfile, docker-compose.yml)](#38-docker)
4. [OOP Concepts Used](#4-oop-concepts-used)
5. [Data Engineering Concepts](#5-data-engineering-concepts)
6. [Python Concepts & Patterns](#6-python-concepts--patterns)
7. [Concurrency & Multithreading](#7-concurrency--multithreading)
8. [SQL & Snowflake Concepts](#8-sql--snowflake-concepts)
9. [Testing Concepts](#9-testing-concepts)
10. [Docker Concepts](#10-docker-concepts)
11. [All Errors Faced & How They Were Fixed](#11-all-errors-faced--how-they-were-fixed)
12. [What Each File Does (Quick Reference)](#12-what-each-file-does-quick-reference)

---

## 1. Project Overview

**finpipe** is a full-stack data engineering pipeline that:
1. **Fetches** real-time stock data from Yahoo Finance (multithreaded)
2. **Validates** data using Pydantic models
3. **Loads** data into Snowflake via MERGE (idempotent upsert)
4. **Queries** data through a CLI
5. **Tests** everything with 30 unit tests
6. **Dockerizes** the entire app for deployment

### Tech Stack

| Layer | Technology | Why |
|-------|-----------|-----|
| Config | Pydantic Settings v2 | Type-safe `.env` loading, auto-validation |
| Database | Snowflake + SQLAlchemy 2.0 | Cloud data warehouse, engine-agnostic ORM |
| Ingestion | yfinance + ThreadPoolExecutor | Free stock API + parallel downloads |
| Validation | Pydantic v2 | Data contracts, cross-field validation |
| CLI | Click | Production-grade command-line framework |
| Testing | pytest + unittest.mock | Unit tests with mocked external systems |
| Container | Docker (multi-stage) | Reproducible, portable deployment |
| Language | Python 3.12+ | Type hints, StrEnum, modern syntax |

### Directory Structure

```
finpipe/
├── finpipe/                    # Main package
│   ├── __init__.py             # Package marker
│   ├── __main__.py             # Entry point: python -m finpipe
│   ├── cli.py                  # Click CLI (5 commands)
│   ├── config/
│   │   ├── __init__.py         # Exports get_settings
│   │   └── settings.py         # Pydantic Settings (singleton)
│   ├── connectors/
│   │   ├── __init__.py         # Exports SnowflakeConnector
│   │   ├── base.py             # Abstract base class (ABC)
│   │   └── snowflake_connector.py  # Snowflake via SQLAlchemy
│   ├── models/
│   │   ├── __init__.py         # Exports StockPrice etc.
│   │   ├── enums.py            # StrEnum (Exchange, TimeFrame, etc.)
│   │   └── stock.py            # Pydantic data models
│   ├── ingestion/
│   │   ├── __init__.py         # Exports YFinanceFetcher, SnowflakeLoader
│   │   ├── yfinance_fetcher.py # Multithreaded stock fetcher
│   │   └── loader.py           # Snowflake bulk loader with MERGE
│   └── sql/
│       ├── 001_create_star_schema.sql  # DDL for star schema
│       └── 002_analytics_queries.sql   # Example analytics SQL
├── tests/
│   ├── __init__.py
│   ├── test_config.py          # 4 tests
│   ├── test_connectors.py      # 6 tests
│   ├── test_models.py          # 14 tests
│   └── test_ingestion.py       # 6 tests
├── .env                        # Snowflake credentials (gitignored)
├── .dockerignore               # Exclusions from Docker build
├── Dockerfile                  # Multi-stage Python build
├── docker-compose.yml          # Pre-configured service profiles
├── requirements.txt            # Python dependencies
└── README.md                   # Learning roadmap
```

---

## 2. Architecture & Data Flow

### End-to-End Pipeline

```
Yahoo Finance API          finpipe               Snowflake
──────────────────    ─────────────────    ───────────────────
                      ┌─────────────┐
  TCS.NS prices  ──→ │  yfinance   │
  RELIANCE.NS    ──→ │  fetcher    │ (5 threads)
  INFY.NS        ──→ │  (thread    │
  HDFCBANK.NS    ──→ │   pool)     │
  ICICIBANK.NS   ──→ └──────┬──────┘
                             │
                      ┌──────▼──────┐
                      │  Pydantic   │ validate + transform
                      │  models     │ DataFrame with typed columns
                      └──────┬──────┘
                             │
                      ┌──────▼──────┐    ┌─────────────────────┐
                      │  Snowflake  │──→ │ staging_table       │
                      │  Loader     │    │ (temp, batch INSERT)│
                      │  (MERGE)    │    └────────┬────────────┘
                      └─────────────┘             │ MERGE (upsert)
                                           ┌──────▼──────────────┐
                                           │ fact_stock_prices   │
                                           │ dim_exchange        │
                                           │ dim_symbol          │
                                           │ dim_date            │
                                           └─────────────────────┘
```

### How `python -m finpipe ingest` Works (Step by Step)

```
1. Python finds finpipe/__main__.py → calls cli()
2. Click parses --symbols, --period, --workers flags
3. Lazy imports: config → get_settings() reads .env, creates singleton
4. YFinanceFetcher created with ThreadPoolExecutor(max_workers=5)
5. executor.submit() sends each symbol to the thread pool
6. Each thread: yf.Ticker("TCS.NS").history(period="1mo") → DataFrame
7. All DataFrames combined with pd.concat()
8. SnowflakeConnector opened (context manager → connect())
9. SQLAlchemy creates engine with connection pool (pool_size=5)
10. SnowflakeLoader:
    a. CREATE staging table (same schema as fact)
    b. Batch INSERT (500 rows per statement) into staging
    c. MERGE staging → fact_stock_prices (upsert on symbol+exchange+date)
    d. DROP staging table
11. Context manager exits → disconnect() → engine.dispose()
12. CLI prints summary and exit code 0
```

---

## 3. Module-by-Module Breakdown

### 3.1 Configuration (config/)

**File:** `config/settings.py`

#### What It Does
Loads environment variables from `.env` into type-safe Python objects using Pydantic Settings v2.

#### Key Concepts

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **Pydantic Settings** | Auto-loads `.env` vars into typed fields | `Settings` class |
| **Singleton Pattern** | Only ONE `Settings` instance ever created | `@lru_cache` on `get_settings()` |
| **@property** | Computed attribute (looks like data, runs code) | `snowflake_uri`, `snowflake_safe_display` |
| **Field()** | Metadata + defaults for each setting | Every class attribute |
| **12-Factor App** | Config from environment, not hardcoded | `.env` file, never in code |

#### How It Works

```python
# .env file:
SNOWFLAKE_USER=ADITYA
SNOWFLAKE_PASSWORD=secret
SNOWFLAKE_ACCOUNT=WIJWAKI-RYC26370

# Python:
settings = get_settings()          # reads .env ONCE, caches forever
settings.snowflake_user            # → "ADITYA" (str, not Optional[str])
settings.snowflake_uri             # → "snowflake://ADITYA:secret@WIJWAKI..."
settings.snowflake_safe_display    # → "ADITYA@WIJWAKI.../STOCK_MARKET_DB/PUBLIC"
```

#### Why Not Just `os.getenv()`?

```python
# BAD: raw os.getenv
port = os.getenv("PORT")       # Returns "5432" (STRING, not int)
port + 1                       # TypeError!

# GOOD: Pydantic Settings
settings.cache_ttl_seconds     # Returns 300 (INT, auto-cast)
settings.cache_ttl_seconds + 1 # Works: 301
```

---

### 3.2 Connectors (connectors/)

**Files:** `connectors/base.py`, `connectors/snowflake_connector.py`

#### What It Does
Abstracts database connections behind a common interface. Currently implements Snowflake; could add Postgres/DuckDB later without changing calling code.

#### Key Concepts

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **Abstract Base Class (ABC)** | Contract that subclasses MUST follow | `BaseConnector` |
| **@abstractmethod** | Method with no body — subclass provides implementation | `connect()`, `disconnect()`, `execute_query()` |
| **Context Manager** | `with` statement for auto-cleanup | `__enter__`/`__exit__` |
| **Dependency Injection** | Pass `settings` from outside, don't create inside | `SnowflakeConnector.__init__(settings)` |
| **Connection Pooling** | Reuse DB connections instead of open/close per query | `create_engine(pool_size=5)` |
| **Template Method Pattern** | Base defines structure, subclass fills details | `BaseConnector` → `SnowflakeConnector` |

#### The ABC Pattern

```python
# base.py — defines the contract
class BaseConnector(ABC):
    @abstractmethod
    def connect(self) -> None: ...      # Subclass MUST implement
    @abstractmethod
    def disconnect(self) -> None: ...
    @abstractmethod
    def execute_query(self, query, params=None) -> list[dict]: ...
    
    # Context manager is FREE — all subclasses get it
    def __enter__(self):
        self.connect()
        return self
    def __exit__(self, ...):
        self.disconnect()
```

```python
# snowflake_connector.py — fills in the details
class SnowflakeConnector(BaseConnector):
    def connect(self):         # Snowflake-specific connection code
    def disconnect(self):      # engine.dispose()
    def execute_query(self):   # text(query) + row._mapping
```

```python
# Usage — context manager handles connect/disconnect automatically
with SnowflakeConnector(settings) as conn:
    rows = conn.execute_query("SELECT 1")
# disconnect() called automatically, even if exception occurs
```

#### Three Query Methods

| Method | Returns | Use For |
|--------|---------|---------|
| `execute_query()` | `list[dict]` | SELECT queries, need individual rows |
| `execute_query_df()` | `pd.DataFrame` | Analytics, plotting, export |
| `execute_non_query()` | `int` (rowcount) | INSERT, UPDATE, DELETE, CREATE, MERGE |

---

### 3.3 Models (models/)

**Files:** `models/enums.py`, `models/stock.py`

#### What It Does
Defines the DATA CONTRACTS — what shape data must have at every stage of the pipeline. Rejects invalid data at creation time.

#### Key Concepts

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **Pydantic BaseModel** | Validated data container (like struct in C/Go) | `StockPrice`, `StockSymbol` |
| **StrEnum** | Named constants that ARE strings | `Exchange`, `TimeFrame` |
| **@field_validator** | Hook that transforms/validates a single field | `symbol_must_be_uppercase` |
| **@model_validator** | Hook that validates ACROSS fields (cross-field) | `validate_price_consistency` |
| **@property** | Computed attribute derived from other fields | `daily_range`, `change_pct` |
| **Value Object** | Object that carries data, no identity of its own | `IngestionResult` |
| **model_dump()** | Convert Pydantic model to plain dict | `to_row()` |

#### Data Validation in Action

```python
# Auto-uppercase
StockSymbol(symbol="tcs", exchange="nse")
# → symbol="TCS", exchange="NSE"

# Reject empty
StockSymbol(symbol="", exchange="NSE")
# → ValueError: Symbol cannot be empty

# Reject invalid exchange
StockSymbol(symbol="TCS", exchange="NYSE")
# → ValueError: Exchange must be one of {'NSE', 'BSE'}

# Cross-field validation
StockPrice(symbol="TCS", trade_date=..., high_price=90, low_price=95, ...)
# → ValueError: high_price (90) cannot be less than low_price (95)

# Negative prices rejected via Field(ge=0)
StockPrice(..., open_price=-100, ...)
# → ValueError: Input should be >= 0
```

#### Star Schema Model Mapping

```
Python Model          →    Snowflake Table
─────────────────────      ────────────────────
StockPrice            →    fact_stock_prices (FACT — events with numbers)
StockSymbol           →    dim_symbol        (DIMENSION — descriptors)
Exchange enum         →    dim_exchange      (DIMENSION — reference data)
(computed in SQL)     →    dim_date          (DIMENSION — calendar)
```

---

### 3.4 Ingestion (ingestion/)

**Files:** `ingestion/yfinance_fetcher.py`, `ingestion/loader.py`

#### YFinanceFetcher — What It Does
Downloads stock price data from Yahoo Finance for multiple symbols **concurrently** using a thread pool.

#### Key Concepts

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **ThreadPoolExecutor** | Fixed pool of reusable threads | `fetch_multiple()` |
| **Futures** | Promise of a future result | `executor.submit() → Future` |
| **as_completed()** | Yield results as they finish (not in order) | Progress logging |
| **Lock (Mutex)** | Protect shared state from race conditions | `self._lock` for counters |
| **Lazy Import** | Import module only when function runs | `import yfinance as yf` inside method |
| **Composition** | Class HAS-A ThreadPoolExecutor (not IS-A) | `__init__` creates pool |

#### Threading Flow

```
Main Thread                    Thread Pool (max_workers=5)
───────────                    ──────────────────────────
submit(TCS.NS)  ──────────→   Thread 1: yf.Ticker("TCS.NS").history()
submit(RELIANCE.NS)  ──────→  Thread 2: yf.Ticker("RELIANCE.NS").history()
submit(INFY.NS)  ──────────→  Thread 3: yf.Ticker("INFY.NS").history()
submit(HDFCBANK.NS)  ──────→  Thread 4: yf.Ticker("HDFCBANK.NS").history()
submit(ICICIBANK.NS)  ─────→  Thread 5: yf.Ticker("ICICIBANK.NS").history()
                               ↓
as_completed() ←─── INFY done first (1.2s)
as_completed() ←─── TCS done second (1.5s)
as_completed() ←─── ... (not in submission order!)
```

#### Why Threading (Not Multiprocessing or Async)?

| Approach | Best For | Our Task |
|----------|----------|----------|
| **Threading** | I/O-bound (network, disk) | ✅ Downloading from Yahoo = network I/O |
| **Multiprocessing** | CPU-bound (math, ML) | ❌ Not doing heavy computation |
| **Async/Await** | 10K+ concurrent I/O | ❌ Only 5-15 stocks at once |

#### SnowflakeLoader — What It Does
Loads a DataFrame into Snowflake using the staging → MERGE pattern for idempotent writes.

#### The MERGE Pattern (Upsert)

```
Step 1: CREATE staging_table (temporary)
Step 2: Batch INSERT DataFrame rows into staging (500 rows/batch)
Step 3: MERGE staging → fact_stock_prices
        ├── IF row exists (same symbol+exchange+date) → UPDATE prices
        └── IF row doesn't exist → INSERT new row
Step 4: DROP staging_table (cleanup)
```

This is **idempotent** — running ingest twice with the same data produces the same result (no duplicates).

---

### 3.5 CLI (cli.py)

**File:** `cli.py`

#### What It Does
Provides 5 terminal commands to interact with the pipeline. Built with Click (used by Flask, dbt, pip).

#### Commands

| Command | What | Example |
|---------|------|---------|
| `health` | Test Snowflake connection | `python -m finpipe health` |
| `tables` | List all tables (with optional row counts) | `python -m finpipe tables --count` |
| `query` | Run ad-hoc SQL | `python -m finpipe query "SELECT * FROM fact_stock_prices LIMIT 5"` |
| `ingest` | Fetch Yahoo Finance → load into Snowflake | `python -m finpipe ingest -s TCS.NS -p 3mo` |
| `schema` | Execute DDL SQL to create star schema | `python -m finpipe schema --yes` |

#### Key Concepts

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **Click @cli.group()** | Group of related subcommands | Main `cli()` function |
| **Lazy imports** | Import inside function, not at top | Each command imports its deps |
| **@click.option()** | Named flags: `--period 3mo` | `ingest` command |
| **@click.argument()** | Positional arg: `query "SELECT ..."` | `query` command |
| **@click.confirmation_option()** | Prompt "Are you sure?" | `schema` command |
| **`python -m finpipe`** | Package execution via `__main__.py` | Entry point |

---

### 3.6 SQL (sql/)

**File:** `sql/001_create_star_schema.sql`

#### What It Does
Creates the star schema tables in Snowflake: 3 dimension tables + 1 fact table + seed data.

#### Star Schema Diagram

```
┌──────────────┐       ┌───────────────────────┐       ┌──────────────┐
│ dim_date     │──────>│ fact_stock_prices      │<──────│ dim_symbol   │
│              │       │                        │       │              │
│ date_key PK  │       │ symbol          PK     │       │ symbol_id PK │
│ day_of_week  │       │ exchange_code   PK     │       │ symbol       │
│ month_number │       │ trade_date      PK     │       │ company_name │
│ quarter      │       │                        │       │ sector       │
│ fiscal_year  │       │ open_price             │       │ exchange_code│
│ is_weekend   │       │ high_price             │       └──────────────┘
│ is_trading   │       │ low_price              │
└──────────────┘       │ close_price            │       ┌──────────────┐
                       │ volume                 │<──────│ dim_exchange │
                       │ daily_range            │       │              │
                       │ change_pct             │       │ exchange_id  │
                       │ source                 │       │ exchange_code│
                       │ loaded_at              │       │ exchange_name│
                       └───────────────────────┘       │ currency     │
                                                       └──────────────┘
```

#### Key SQL Concepts

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **Star Schema** | Fact table surrounded by dimension tables | Overall design |
| **Surrogate Key** | Auto-generated ID (meaningless number) | `exchange_id AUTOINCREMENT` |
| **Natural Key** | Real-world identifier | `exchange_code UNIQUE` |
| **Composite PK** | Multiple columns together form the key | `PRIMARY KEY (symbol, exchange_code, trade_date)` |
| **CLUSTER BY** | How Snowflake physically organizes data on disk | `CLUSTER BY (trade_date)` |
| **MERGE (Upsert)** | INSERT if new, UPDATE if exists | Seed data for exchanges + dates |
| **GENERATOR** | Create N rows from nothing (Snowflake-specific) | Date dimension: 4018 rows |
| **Fiscal Year** | Indian FY runs April→March (FY2025-26) | Computed in dim_date |
| **Idempotent DDL** | `CREATE TABLE IF NOT EXISTS` — safe to re-run | All CREATE statements |

#### Actual Tables in Snowflake

| Table | Type | Rows | Description |
|-------|------|------|-------------|
| `dim_exchange` | Dimension | 2 | NSE, BSE |
| `dim_symbol` | Dimension | 0 | Auto-populated during ingestion |
| `dim_date` | Dimension | 4,018 | 2020-01-01 to 2030-12-31 |
| `fact_stock_prices` | Fact | Growing | Daily OHLCV prices |

---

### 3.7 Tests (tests/)

**Files:** `test_config.py`, `test_connectors.py`, `test_models.py`, `test_ingestion.py`

30 unit tests total. Run with: `pytest tests/ -v`

#### Test Breakdown

| File | Tests | What's Tested |
|------|-------|---------------|
| `test_config.py` | 4 | .env loading, URI format, password masking, defaults |
| `test_connectors.py` | 6 | ABC enforcement, connect/disconnect, context manager, query results |
| `test_models.py` | 14 | Validation, computed properties, enums, edge cases |
| `test_ingestion.py` | 6 | API mocking, threading, error handling, DataFrame shapes |

#### Testing Concepts Used

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **monkeypatch** | pytest fixture to set/unset env vars | `test_config.py` |
| **MagicMock** | Fake object that records all calls | Mock Settings, mock engine |
| **@patch()** | Replace a real import with a mock | `@patch("yfinance.Ticker")` |
| **pytest.raises** | Assert that code raises a specific exception | Validation tests |
| **Arrange-Act-Assert** | Test structure: setup → do → verify | All tests follow this |

---

### 3.8 Docker

**Files:** `Dockerfile`, `docker-compose.yml`, `.dockerignore`

#### What It Does
Packages finpipe into a portable container that runs anywhere Docker is installed — no Python install needed.

#### Key Docker Concepts

| Concept | What It Means | Where Used |
|---------|---------------|------------|
| **Multi-stage build** | Stage 1 compiles, Stage 2 runs (smaller image) | `FROM ... AS builder` → `FROM ...` |
| **Layer caching** | Unchanged layers skip rebuild | `COPY requirements.txt` before `COPY finpipe/` |
| **Non-root user** | Security: never run as root in production | `USER finpipe` |
| **ENTRYPOINT** | Command prefix for all `docker run` invocations | `python -m finpipe` |
| **CMD** | Default args if none given | `["--help"]` |
| **--env-file** | Pass secrets at runtime (never bake into image) | `docker run --env-file .env` |
| **.dockerignore** | Exclude files from build context (like .gitignore) | `venv/`, `.env`, `tests/` |
| **extends** | Docker Compose service inheritance | All services extend `finpipe` |

#### Multi-Stage Build Explained

```dockerfile
# Stage 1: BUILDER — has gcc, g++, build tools (large)
FROM python:3.12-slim AS builder
RUN apt-get install gcc g++ libffi-dev    # Build tools for C extensions
RUN pip install -r requirements.txt        # Compile snowflake-connector etc.

# Stage 2: RUNTIME — clean Python only (small)
FROM python:3.12-slim                      # Fresh, no build tools
COPY --from=builder /install /usr/local    # Only the compiled packages
COPY finpipe/ ./finpipe/                   # Only the app code
```

**Why?** The builder stage might be 800MB (with gcc, headers, etc.). The runtime stage is ~250MB. Build tools aren't needed to *run* the app.

#### Layer Caching Strategy

```dockerfile
COPY requirements.txt .          # Rarely changes → cached layer
RUN pip install -r requirements.txt  # Only re-runs if requirements.txt changed

COPY finpipe/ ./finpipe/         # Changes often (your code)
```

Docker rebuilds from the first changed layer DOWN. Since `requirements.txt` changes rarely, `pip install` is cached most of the time. Only the `COPY finpipe/` layer rebuilds when you edit code.

---

## 4. OOP Concepts Used

| # | Concept | Where | What It Does |
|---|---------|-------|-------------|
| 1 | **Inheritance** | `SnowflakeConnector(BaseConnector)` | Snowflake fills in the abstract methods defined by base |
| 2 | **Abstract Base Class** | `BaseConnector(ABC)` | Defines a contract — subclasses MUST implement `connect()`, `disconnect()`, etc. |
| 3 | **Encapsulation** | `_connection`, `_engine`, `_lock` | Private attributes (underscore prefix) — internal state hidden from outside |
| 4 | **Polymorphism** | Any `BaseConnector` subclass has the same interface | Code that works with `BaseConnector` works with any implementation |
| 5 | **Composition** | `YFinanceFetcher` HAS-A `ThreadPoolExecutor` | Uses another object instead of inheriting from it |
| 6 | **Dependency Injection** | `SnowflakeLoader(connector)`, `SnowflakeConnector(settings)` | Dependencies passed from outside, not created inside |
| 7 | **Singleton** | `@lru_cache` on `get_settings()` | Only ONE Settings object ever created |
| 8 | **Context Manager** | `__enter__`/`__exit__` | Safe resource cleanup with `with` statement |
| 9 | **Data Class** | `StockPrice(BaseModel)` | Class whose purpose is to hold validated data |
| 10 | **Computed Properties** | `@property daily_range`, `change_pct` | Look like attributes but run code (derived values) |
| 11 | **Value Object** | `IngestionResult` | Immutable result carrier — no identity, just data |
| 12 | **Template Method** | Base defines structure, subclass fills details | `BaseConnector` → `SnowflakeConnector` |

---

## 5. Data Engineering Concepts

| # | Concept | Where | Explanation |
|---|---------|-------|-------------|
| 1 | **ETL / ELT** | Whole pipeline | Extract (Yahoo Finance) → Transform (Pydantic) → Load (Snowflake) |
| 2 | **Star Schema** | SQL + models | Fact table (events) surrounded by dimension tables (descriptors) |
| 3 | **Fact Table** | `fact_stock_prices` | Stores measurable events (prices, volumes) — grows daily |
| 4 | **Dimension Table** | `dim_exchange`, `dim_symbol`, `dim_date` | Reference/lookup data — rarely changes |
| 5 | **MERGE / Upsert** | `loader.py` | INSERT if new, UPDATE if exists — prevents duplicates |
| 6 | **Idempotent Writes** | MERGE pattern | Running same data twice = same result, no corruption |
| 7 | **Staging Table** | `_write_to_staging()` | Temporary table for bulk load before merge |
| 8 | **Batch Processing** | 500 rows per INSERT | Balance between speed (fewer roundtrips) and reliability |
| 9 | **Data Validation** | Pydantic models | Reject bad data BEFORE it enters the database |
| 10 | **Data Contract** | `StockPrice`, `StockSymbol` | Formal definition of what data must look like |
| 11 | **Surrogate Key** | `exchange_id AUTOINCREMENT` | Meaningless ID for joins (vs natural key) |
| 12 | **Slowly Changing Dimension** | `dim_exchange` | Reference data that rarely changes — Type 1 (overwrite) |
| 13 | **Date Dimension** | `dim_date` | Pre-computed calendar with fiscal year, quarters, weekends |
| 14 | **Connection Pooling** | `pool_size=5, max_overflow=10` | Reuse connections instead of open/close per query |

---

## 6. Python Concepts & Patterns

| # | Concept | Where | Explanation |
|---|---------|-------|-------------|
| 1 | **Type Hints** | All function signatures | `def connect(self) -> None:` — IDE autocomplete + docs |
| 2 | **Pydantic v2** | models/ | Data validation, serialization, type coercion |
| 3 | **@field_validator** | `stock.py` | Transforms single field at creation (`"tcs"` → `"TCS"`) |
| 4 | **@model_validator** | `stock.py` | Cross-field check (high >= low) after all fields set |
| 5 | **@property** | settings, models | Computed attributes that look like data access |
| 6 | **@lru_cache** | `get_settings()` | Memoization — compute once, return cached result |
| 7 | **StrEnum** | `enums.py` | Named constants that behave as strings |
| 8 | **Lazy Import** | `import yfinance as yf` inside method | Only load heavy module when actually needed |
| 9 | **f-strings** | Everywhere | `f"Loading {count} rows"` — readable string formatting |
| 10 | **walrus operator (:=)** | Not used, but related | Assign + use in one expression |
| 11 | **Union types `\|`** | `dict \| None = None` | Modern Python 3.10+ optional typing |
| 12 | **model_dump()** | `to_row()` | Pydantic v2 method to convert model → dict |
| 13 | **Click library** | `cli.py` | Decorators to build CLI: `@click.command()`, `@click.option()` |
| 14 | **pathlib** | `cli.py` | `Path(__file__).parent / "sql" / "file.sql"` — cross-platform paths |
| 15 | **Context Manager Protocol** | `__enter__` / `__exit__` | Resource management with `with` statement |

---

## 7. Concurrency & Multithreading

### Concepts

| Concept | What | Where |
|---------|------|-------|
| **ThreadPoolExecutor** | Fixed pool of N reusable threads | `fetch_multiple()` |
| **Future** | Promise of a result that doesn't exist yet | `executor.submit() → Future` |
| **as_completed()** | Iterator that yields futures in completion order | Progress tracking |
| **Lock (Mutex)** | Only one thread can enter at a time | `self._lock` for `_success_count` |
| **Thread Safety** | No two threads modify the same data | Each thread gets its own symbol/DataFrame |
| **Immutable Arguments** | Strings passed to threads can't be modified | Thread-safe by nature |

### Why Thread Pool (Not Raw Threads)?

```python
# BAD: raw threads (500 stocks = 500 threads = OS crash)
for stock in stocks:
    Thread(target=fetch, args=(stock,)).start()

# GOOD: thread pool (max 5 at once, controlled)
with ThreadPoolExecutor(max_workers=5) as pool:
    pool.map(fetch, stocks)
```

### Thread Safety in finpipe

```python
# Safe: each thread gets immutable input (string)
executor.submit(self.fetch_single, "TCS.NS", ...)  # TCS.NS can't be modified

# Safe: each thread creates its own DataFrame (no sharing)
df = ticker.history(period=period)  # Local to this thread

# Needs Lock: shared counter modified by multiple threads
with self._lock:           # Only one thread enters this block at a time
    self._success_count += 1  # Safe increment
```

---

## 8. SQL & Snowflake Concepts

| Concept | SQL Example | Explanation |
|---------|-------------|-------------|
| **CREATE TABLE IF NOT EXISTS** | `CREATE TABLE IF NOT EXISTS dim_exchange (...)` | Idempotent DDL — safe to re-run |
| **AUTOINCREMENT** | `exchange_id INTEGER AUTOINCREMENT` | Auto-generate unique IDs |
| **UNIQUE constraint** | `exchange_code VARCHAR(10) NOT NULL UNIQUE` | Prevent duplicate natural keys |
| **Composite PK** | `PRIMARY KEY (symbol, exchange_code, trade_date)` | Multiple columns = one unique row |
| **CLUSTER BY** | `CLUSTER BY (trade_date)` | Snowflake organizes data by date on disk |
| **MERGE** | `MERGE INTO target USING source ON ... WHEN MATCHED ... WHEN NOT MATCHED` | Upsert pattern |
| **GENERATOR** | `TABLE(GENERATOR(ROWCOUNT => 4018))` | Create N rows from nothing |
| **seq4()** | `DATEADD(DAY, seq4(), '2020-01-01')` | Sequence numbers 0,1,2,...N |
| **INFORMATION_SCHEMA** | `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES` | Metadata about tables |
| **COMMENT** | `COMMENT = 'Dimension: Stock exchanges'` | Snowflake metadata annotation |
| **Parameterized queries** | `execute(text("SELECT ... WHERE x = :val"), {"val": v})` | Prevent SQL injection |
| **text()** | `from sqlalchemy import text` | Mark string as raw SQL for SQLAlchemy |

---

## 9. Testing Concepts

| Concept | What | Example |
|---------|------|---------|
| **pytest** | Python testing framework | `pytest tests/ -v` |
| **monkeypatch** | Override env vars for one test | `monkeypatch.setenv("SNOWFLAKE_USER", "test")` |
| **MagicMock** | Fake object that records calls | `mock_settings = MagicMock()` |
| **@patch()** | Replace a real import with a mock | `@patch("yfinance.Ticker")` |
| **pytest.raises** | Assert exception is raised | `with pytest.raises(ValueError):` |
| **setup_method** | Run before each test (fresh state) | `self.fetcher = YFinanceFetcher()` |
| **Arrange-Act-Assert** | Test structure | Setup data → call function → check result |
| **Unit vs Integration** | Unit = isolated, Integration = real systems | All current tests are unit (mocked) |

### Why Mock External Systems?

```python
# BAD: unit test hits real Yahoo Finance
def test_fetch():
    fetcher = YFinanceFetcher()
    df, result = fetcher.fetch_single("TCS.NS")  # Needs internet! Slow! Flaky!

# GOOD: mock yfinance, test OUR code in isolation
@patch("yfinance.Ticker")
def test_fetch(mock_ticker_cls):
    mock_ticker_cls.return_value.history.return_value = fake_df
    df, result = fetcher.fetch_single("TCS.NS")  # Fast! Offline! Reliable!
```

---

## 10. Docker Concepts

| Concept | What | finpipe Usage |
|---------|------|---------------|
| **Image** | Blueprint/template (read-only snapshot) | `docker build -t finpipe .` creates image |
| **Container** | Running instance of an image | `docker run finpipe health` creates container |
| **Multi-stage build** | Multiple FROM stages, only last one ships | Builder (gcc) → Runtime (no gcc) |
| **Layer** | Each instruction creates a cached layer | `COPY requirements.txt` = layer, `RUN pip install` = layer |
| **Layer caching** | Unchanged layers skip rebuild | requirements.txt changes rarely → pip install cached |
| **ENTRYPOINT** | Fixed command prefix | `python -m finpipe` — always runs this |
| **CMD** | Default args (overridable) | `["--help"]` if no args given |
| **--env-file** | Pass .env at runtime | Secrets never baked into image |
| **.dockerignore** | Exclude from build context | `venv/`, `.env`, `tests/`, `.git/` |
| **Non-root user** | Security — least privilege | `USER finpipe` (uid 1000) |
| **docker-compose** | Multi-service orchestration | Pre-configured commands for each CLI action |
| **extends** | Compose service inheritance | All services extend base `finpipe` service |

### Image Size Optimization

```
Without multi-stage: ~800MB (gcc, g++, headers, source, etc.)
With multi-stage:    ~250MB (Python + pip packages + app code only)
```

---

## 11. All Errors Faced & How They Were Fixed

### Bug 1: Column Casing — `KeyError: 'TABLE_NAME'`

**When:** Running `python -m finpipe health` and `python -m finpipe tables`

**Error:**
```
KeyError: 'TABLE_NAME'
```

**Root Cause:** SQLAlchemy returns column names from `row._mapping` as **lowercase** (e.g., `table_name`), but the code accessed them as **uppercase** (`TABLE_NAME`).

**Fix:** Changed all dict key access to use `.get()` with fallbacks:
```python
# BEFORE (broke):
name = r["TABLE_NAME"]

# AFTER (works):
name = r.get("TABLE_NAME") or r.get("table_name", "")
```

**Lesson:** Different versions of SQLAlchemy/Snowflake drivers may return keys in different cases. Always use `.get()` with fallbacks when reading database metadata.

---

### Bug 2: UTF-8 Encoding — `UnicodeDecodeError`

**When:** Running `python -m finpipe schema --yes`

**Error:**
```
UnicodeDecodeError: 'charmap' codec can't decode byte 0x9d in position 1234
```

**Root Cause:** The SQL file contained box-drawing characters (`═`, `─`, `│`) in comments. On **Windows**, Python's default file encoding is `cp1252` (Windows-1252), which cannot decode these UTF-8 characters.

**Fix:** Added explicit `encoding="utf-8"` when opening the SQL file:
```python
# BEFORE (broke on Windows):
with open(file, "r") as f:

# AFTER (works everywhere):
with open(file, "r", encoding="utf-8") as f:
```

**Lesson:** Always specify `encoding="utf-8"` when reading files on Windows. Python only defaults to UTF-8 on Linux/Mac.

---

### Bug 3: Hardcoded Database — `Object does not exist`

**When:** Running `python -m finpipe schema --yes`

**Error:**
```
SQL compilation error: Object 'STOCK_DATA' does not exist
```

**Root Cause:** The SQL file had `USE DATABASE STOCK_DATA;` hardcoded at the top, but the actual Snowflake database was `STOCK_MARKET_DB` (configured in `.env`).

**Fix:** Removed the `USE DATABASE` and `USE SCHEMA` statements from the SQL file. The connector already uses the database/schema from `.env`:
```sql
-- BEFORE (hardcoded, broke):
USE DATABASE STOCK_DATA;
USE SCHEMA PUBLIC;

-- AFTER (removed — connector handles it from .env):
-- The connector already uses the database/schema from your .env
```

**Lesson:** Never hardcode database/schema names in SQL files. Let the connection handle it via configuration.

---

### Bug 4: Relative File Path — `FileNotFoundError`

**When:** Running `python -m finpipe schema --yes`

**Error:**
```
FileNotFoundError: [Errno 2] No such file or directory: 'finpipe/sql/001_create_star_schema.sql'
```

**Root Cause:** The default SQL file path was a relative string `"finpipe/sql/..."`, which only works if you run the command from the project root. Running from any other directory breaks it.

**Fix:** Used `pathlib.Path(__file__)` to get the absolute path relative to the source file:
```python
# BEFORE (relative, fragile):
file = "finpipe/sql/001_create_star_schema.sql"

# AFTER (absolute, works from anywhere):
file = str(pathlib.Path(__file__).parent / "sql" / "001_create_star_schema.sql")
```

**Lesson:** Use `pathlib.Path(__file__).parent` to build paths relative to the current Python file, not relative to the working directory.

---

### Bug 5: SQL Splitting — `0 statements found`

**When:** Running `python -m finpipe schema --yes`

**Error:**
```
Executing 0 SQL statements...
Schema setup complete!
(but no tables were actually created)
```

**Root Cause:** The SQL splitting logic filtered out statements that "start with comments":
```python
# BEFORE (broken filter):
if not s.strip().startswith("--"):
    statements.append(s)
```
Every SQL chunk started with comment blocks (section headers), so ALL statements were dropped.

**Fix:** Changed the filter to check if the chunk contains any **actual SQL lines** (not just comments):
```python
# AFTER (check for real SQL inside each chunk):
sql_lines = [l for l in stripped.split("\n") 
             if l.strip() and not l.strip().startswith("--")]
if sql_lines:  # Has at least one non-comment line
    statements.append(stripped)
```

**Lesson:** When splitting SQL by `;`, don't filter based on what the chunk *starts* with. Check if it *contains* actual SQL lines.

---

### Bug 6: MERGE Returns 0 Rows — Data Goes to Staging But Not Fact Table

**When:** Running `python -m finpipe ingest`

**Error:**
```
Loaded 0 rows (0 merged)
(but data was in the staging table — it just didn't reach fact_stock_prices)
```

**Root Cause:** The MERGE statement was executed with `execute_query()` (designed for SELECT) instead of `execute_non_query()` (designed for DML like INSERT/UPDATE/MERGE).

`execute_query()` tries to read result rows and returns `[]` (empty list, since MERGE doesn't return rows). `execute_non_query()` returns the `rowcount` (number of rows affected) and calls `conn.commit()`.

**Fix:**
```python
# BEFORE (wrong — designed for SELECT):
rows = self._connector.execute_query(merge_sql)

# AFTER (correct — designed for DML):
rows_merged = self._connector.execute_non_query(merge_sql)
```

**Lesson:** 
- `execute_query()` = for SELECT (reads rows, no commit)
- `execute_non_query()` = for INSERT/UPDATE/DELETE/MERGE/DDL (returns rowcount, commits)

---

### Bug 7: Mocking yfinance Lazy Import — `AttributeError`

**When:** Running `pytest tests/test_ingestion.py`

**Error:**
```
AttributeError: <module 'yfinance'> has no attribute 'Ticker'
```

**Root Cause:** `yfinance` is imported **lazily** inside `fetch_single()`:
```python
def fetch_single(self, ...):
    import yfinance as yf  # Lazy import — only here, not at module top
    ticker = yf.Ticker(symbol)
```

The test tried to mock `finpipe.ingestion.yfinance_fetcher.yf.Ticker`, but `yf` doesn't exist at module level — it only exists inside the function.

**Fix:** Mock `yfinance.Ticker` directly (the actual module, not a local alias):
```python
# BEFORE (broke — yf doesn't exist at module level):
@patch("finpipe.ingestion.yfinance_fetcher.yf.Ticker")

# AFTER (works — patch the real module):
@patch("yfinance.Ticker")
```

**Lesson:** When a module is lazy-imported inside a function, patch the actual module path (`yfinance.Ticker`), not the local alias.

---

### Bug 8: DataFrame Truthiness — `ValueError: ambiguous truth value`

**When:** Running `pytest tests/test_ingestion.py`

**Error:**
```
ValueError: The truth value of a DataFrame is ambiguous. Use a.empty, a.bool(), a.item()...
```

**Root Cause:** Code used Python's `or` operator on a DataFrame:
```python
# BEFORE (broke):
history_return or pd.DataFrame()
```
Python tries to evaluate `bool(history_return)`, but DataFrames don't have a single truth value (they could have multiple rows/columns).

**Fix:** Use explicit `is not None` check:
```python
# AFTER (works):
if history_return is not None:
    mock_ticker.history.return_value = history_return
else:
    mock_ticker.history.return_value = pd.DataFrame()
```

**Lesson:** Never use `or`, `and`, `not`, `if df:` with Pandas DataFrames. Always use `df.empty`, `df is None`, `len(df) == 0`.

---

## 12. What Each File Does (Quick Reference)

| File | Purpose | Key Concept |
|------|---------|-------------|
| `__main__.py` | Entry point for `python -m finpipe` | Package execution |
| `cli.py` | 5 terminal commands (health, tables, query, ingest, schema) | Click CLI framework |
| `config/settings.py` | Type-safe .env loading + singleton | Pydantic Settings, @lru_cache |
| `connectors/base.py` | Abstract connector interface | ABC, context manager |
| `connectors/snowflake_connector.py` | Snowflake via SQLAlchemy 2.0 | Connection pooling, parameterized queries |
| `models/enums.py` | Named constants (Exchange, TimeFrame) | StrEnum |
| `models/stock.py` | Data validation models | Pydantic, validators, properties |
| `ingestion/yfinance_fetcher.py` | Multithreaded stock data download | ThreadPoolExecutor, Futures, Lock |
| `ingestion/loader.py` | Load DataFrames into Snowflake | Staging → MERGE (upsert), batch INSERT |
| `sql/001_create_star_schema.sql` | Star schema DDL + seed data | CREATE TABLE, MERGE, GENERATOR |
| `tests/test_config.py` | Test config loading (4 tests) | monkeypatch |
| `tests/test_connectors.py` | Test connector layer (6 tests) | MagicMock, @patch |
| `tests/test_models.py` | Test data validation (14 tests) | pytest.raises |
| `tests/test_ingestion.py` | Test fetcher + threading (6 tests) | Mock API, DataFrame assertions |
| `Dockerfile` | Multi-stage container build | Builder/runtime stages, non-root user |
| `docker-compose.yml` | Pre-configured Docker commands | Service profiles, extends |
| `.dockerignore` | Exclude files from Docker build | Like .gitignore for Docker |
| `requirements.txt` | Python dependencies | pip install target |
| `.env` | Snowflake credentials (gitignored!) | 12-Factor App config |

---

*Generated from the finpipe project — a data engineering learning pipeline built with Python, Snowflake, Docker, and 30 unit tests.*
