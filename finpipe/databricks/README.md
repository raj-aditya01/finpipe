# Databricks Integration Guide — finpipe

## Overview

This module contains **Spark transformations** that run on **Databricks**, NOT locally.

Your local finpipe pipeline handles: **Yahoo Finance → Validate → Snowflake**
Databricks handles: **Snowflake (Bronze) → Silver → Gold (Delta Lake)**

```
LOCAL MACHINE                          DATABRICKS CLUSTER
┌──────────────────────┐              ┌──────────────────────────────┐
│ finpipe CLI          │              │ Spark Transforms             │
│                      │              │                              │
│ Extract (yfinance) ──┼──► Snowflake ──► Bronze → Silver → Gold    │
│ Validate (quality)   │  (fact table)│   (Delta Lake tables)        │
│ Load (MERGE)         │              │                              │
└──────────────────────┘              └──────────────────────────────┘
```

---

## Setup on Databricks

### 1. Connect Databricks to Snowflake

In your Databricks notebook:
```python
# Read from Snowflake into a Spark DataFrame
options = {
    "sfUrl": "WIJWAKI-RYC26370.snowflakecomputing.com",
    "sfUser": "ADITYA",
    "sfPassword": dbutils.secrets.get("snowflake", "password"),
    "sfDatabase": "STOCK_MARKET_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
}

bronze_df = (
    spark.read
    .format("snowflake")
    .options(**options)
    .option("query", "SELECT * FROM fact_stock_prices")
    .load()
)
```

### 2. Upload finpipe as a Library

Option A — **Databricks Repos** (recommended):
```
1. Push finpipe to GitLab/GitHub
2. Databricks → Repos → Add Repo → paste URL
3. In notebook: %pip install /Workspace/Repos/your-user/finpipe
```

Option B — **Wheel file**:
```bash
# Local machine
cd finpipe
pip install build
python -m build --wheel
# Upload dist/finpipe-*.whl to Databricks DBFS
```

### 3. Run Transforms

```python
from finpipe.databricks.transforms import SparkTransforms

st = SparkTransforms(spark)

# Bronze → Silver (deduplicate, type-cast, validate)
silver_df = st.bronze_to_silver(bronze_df)
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_stock_prices")

# Silver → Gold (moving averages, returns, volatility)
gold_df = st.silver_to_gold(silver_df)
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_stock_analytics")

# Sector summary (aggregated KPIs)
summary_df = st.compute_sector_summary(gold_df)
summary_df.write.format("delta").mode("overwrite").saveAsTable("gold_sector_summary")
```

### 4. Or Use Pure SQL (dbt-style)

The `transforms.py` file also exports SQL strings:
```python
from finpipe.databricks.transforms import BRONZE_TO_SILVER_SQL, SILVER_TO_GOLD_SQL

spark.sql(BRONZE_TO_SILVER_SQL)
spark.sql(SILVER_TO_GOLD_SQL)
```

---

## Medallion Architecture

| Layer  | Table                    | Purpose              | Update Frequency |
|--------|--------------------------|----------------------|------------------|
| Bronze | `fact_stock_prices`      | Raw ingested data    | Every pipeline run |
| Silver | `silver_stock_prices`    | Deduped + validated  | After Bronze      |
| Gold   | `gold_stock_analytics`   | Enriched analytics   | After Silver      |
| Gold   | `gold_sector_summary`    | KPI aggregations     | After Gold        |

---

## Delta Lake Features You Get for Free

1. **ACID Transactions** — no partial writes, no corrupted tables
2. **Time Travel** — query data as it was yesterday: `SELECT * FROM table VERSION AS OF 5`
3. **Schema Evolution** — add columns without breaking existing queries
4. **Z-Ordering** — physical data layout optimization for fast queries
5. **VACUUM** — clean up old versions to save storage

```sql
-- Optimize Gold table for common query patterns
OPTIMIZE gold_stock_analytics ZORDER BY (SYMBOL, TRADE_DATE);

-- Time travel: see data from 7 days ago
SELECT * FROM gold_stock_analytics TIMESTAMP AS OF '2024-01-01';
```
