# ==============================================================================
# Financial Data Pipeline — Learn Data Engineering by Building
# ==============================================================================
#
# A comprehensive data engineering project using real stock market data.
# Each module teaches a core concept: OOP, SQL, Snowflake, Spark, 
# multithreading, data modeling, pipeline orchestration, and testing.
#
# LEARNING ROADMAP (implement in this order):
# ──────────────────────────────────────────
#
# Phase 1: Foundation (You are here)
# ───────────────────
#   1. config/         → Environment management, 12-factor app, Pydantic settings
#   2. connectors/     → Snowflake connector with OOP (classes, inheritance, context managers)
#   3. models/         → Data models with dataclasses + Pydantic (OOP, type safety)
#   4. sql/            → Raw SQL scripts for Snowflake (star schema, analytics)
#
# Phase 2: Data Ingestion
# ───────────────────────
#   5. ingestion/      → Pull data from APIs (yfinance, NSE)
#   6. multithreading  → Concurrent data fetching (threading, asyncio, pools)
#
# Phase 3: Processing (Spark)
# ───────────────────────────
#   7. processing/     → PySpark transformations, aggregations, window functions
#   8. pipelines/      → Orchestrate ingestion → processing → loading (ETL)
#
# Phase 4: Advanced
# ─────────────────
#   9. quality/        → Data validation, schema checks, great_expectations
#   10. tests/         → Unit tests, integration tests, fixtures
#   11. cli.py         → Command-line interface to run everything
#
# HOW TO RUN:
#   pip install -r requirements.txt
#   cp ../stock101/.env .env          # Reuse your Snowflake creds
#   python -m finpipe.cli health      # Test Snowflake connection
#   python -m finpipe.cli ingest      # Pull stock data
#   python -m finpipe.cli transform   # Run Spark transformations
#
# ==============================================================================
