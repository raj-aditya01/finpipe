# ==============================================================================
# databricks/transforms.py — Spark Transforms (Run on Databricks, NOT locally)
# ==============================================================================
#
# ⚠️  THIS CODE IS NOT MEANT TO RUN LOCALLY.
#     It requires a Spark cluster (Databricks, EMR, Dataproc, etc.)
#     Import these into a Databricks notebook or Databricks job.
#
# WHAT YOU LEARN HERE:
#   1. PySpark DataFrame API — distributed data processing
#   2. Window Functions  — rolling averages, ranks, lag/lead
#   3. Partitioning      — process each symbol independently
#   4. Delta Lake        — versioned, ACID-compliant storage
#   5. Medallion Architecture — Bronze → Silver → Gold layers
#
# MEDALLION ARCHITECTURE:
#   ┌─────────────────────────────────────────────────────────────┐
#   │                                                             │
#   │  BRONZE (raw)     → SILVER (cleaned)    → GOLD (analytics) │
#   │                                                             │
#   │  Raw ingestion      Deduplicated          Aggregated KPIs   │
#   │  Append-only        Type-cast             Pre-joined        │
#   │  Schema-on-read     Validated             Business-ready    │
#   │                     Standardized          Dashboard-ready   │
#   │                                                             │
#   │  fact_stock_prices  silver_stock_prices   gold_daily_summary│
#   │  (Snowflake)        (Delta Lake)          (Delta Lake)      │
#   │                                                             │
#   └─────────────────────────────────────────────────────────────┘
#
#   WHY? Each layer serves a different audience:
#     Bronze: Data Engineers (debug, reprocess)
#     Silver: Data Analysts (clean, reliable data)
#     Gold:   Business Users (KPIs, dashboards)
#
# HOW TO USE THIS IN DATABRICKS:
#   1. Upload this file to Databricks Workspace (or use Repos)
#   2. In a notebook cell:
#        from finpipe.databricks.transforms import SparkTransforms
#        st = SparkTransforms(spark)
#        silver_df = st.bronze_to_silver(bronze_df)
#        gold_df = st.silver_to_gold(silver_df)
#   3. Or use the SQL equivalents in transforms_sql.py
#
# ==============================================================================


class SparkTransforms:
    """
    Spark DataFrame transformations for the stock pipeline.

    These transform raw (bronze) stock data into analytics-ready (gold) data.
    Each method is a pure transform: DataFrame in → DataFrame out.

    DESIGN PRINCIPLES:
      1. No side effects — methods don't read/write storage
      2. Composable — chain transforms: bronze_to_silver(df) → silver_to_gold(df)
      3. Testable — pass in any DataFrame, check the output
      4. Lazy evaluation — Spark won't execute until you call .collect()/.write()

    WHY NOT USE PANDAS FOR THIS?
      Pandas: single machine, in-memory. Works for < 1M rows.
      Spark:  distributed across a cluster. Works for billions of rows.

      When your data grows beyond one machine's memory, Spark scales
      horizontally by adding more worker nodes. Same code, bigger data.

    REQUIRES: PySpark (available on Databricks clusters automatically)
    """

    def __init__(self, spark):
        """
        Initialize with a SparkSession.

        On Databricks, `spark` is pre-created in every notebook.
        In a standalone Spark app, you'd create it with SparkSession.builder.

        Args:
            spark: The active SparkSession instance.
        """
        self.spark = spark

    def bronze_to_silver(self, bronze_df):
        """
        Transform raw (bronze) data into cleaned (silver) data.

        WHAT THIS DOES:
          1. Deduplication — remove exact duplicates
          2. Type casting — ensure proper data types
          3. Null handling — filter or flag rows with critical nulls
          4. Standardization — consistent column names and formats

        PYSPARK vs PANDAS equivalent:
          Pandas:  df.drop_duplicates()
          Spark:   df.dropDuplicates()  ← runs on 100 machines in parallel

          Pandas:  df["price"].astype(float)
          Spark:   df.withColumn("price", col("price").cast("double"))

        Args:
            bronze_df: Raw Spark DataFrame from Bronze layer.

        Returns:
            Cleaned Spark DataFrame for Silver layer.
        """
        from pyspark.sql import functions as F

        silver = (
            bronze_df
            # 1. Deduplicate on natural key (symbol + exchange + date)
            .dropDuplicates(["SYMBOL", "EXCHANGE_CODE", "TRADE_DATE"])

            # 2. Cast types explicitly (schema enforcement)
            .withColumn("TRADE_DATE", F.to_date("TRADE_DATE"))
            .withColumn("OPEN_PRICE", F.col("OPEN_PRICE").cast("double"))
            .withColumn("HIGH_PRICE", F.col("HIGH_PRICE").cast("double"))
            .withColumn("LOW_PRICE", F.col("LOW_PRICE").cast("double"))
            .withColumn("CLOSE_PRICE", F.col("CLOSE_PRICE").cast("double"))
            .withColumn("VOLUME", F.col("VOLUME").cast("long"))

            # 3. Filter out rows with null critical fields
            .filter(
                F.col("SYMBOL").isNotNull()
                & F.col("TRADE_DATE").isNotNull()
                & F.col("CLOSE_PRICE").isNotNull()
            )

            # 4. Add metadata columns
            .withColumn("_silver_loaded_at", F.current_timestamp())
            .withColumn("_source", F.lit("finpipe"))
        )

        return silver

    def silver_to_gold(self, silver_df):
        """
        Transform cleaned (silver) data into analytics-ready (gold) data.

        WHAT THIS DOES:
          1. Moving averages — 7-day and 30-day (for trend analysis)
          2. Daily returns — percentage change from previous day
          3. Volatility — rolling standard deviation (risk measure)
          4. Rank — rank stocks by volume within each day

        WINDOW FUNCTIONS — The Heart of Analytics:
          A window function computes a value for each row based on a
          "window" of related rows. Unlike GROUP BY (which collapses rows),
          window functions KEEP all rows and ADD computed columns.

          Example: 7-day moving average for each stock
            Window = "last 7 rows, partitioned by symbol, ordered by date"
            For each row, AVG(close_price) over that window.

          SQL equivalent:
            AVG(close_price) OVER (
                PARTITION BY symbol
                ORDER BY trade_date
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ) AS ma_7d

        Args:
            silver_df: Cleaned Spark DataFrame from Silver layer.

        Returns:
            Analytics-ready Spark DataFrame for Gold layer.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        # Define window specs
        # PARTITION BY symbol = compute separately for each stock
        # ORDER BY trade_date = chronological order within each stock
        symbol_window = Window.partitionBy("SYMBOL").orderBy("TRADE_DATE")

        # Rolling windows (last N rows for the same symbol)
        window_7d = symbol_window.rowsBetween(-6, 0)   # Current + 6 preceding
        window_30d = symbol_window.rowsBetween(-29, 0)  # Current + 29 preceding

        # Daily window for ranking (all stocks on the same day)
        daily_window = Window.partitionBy("TRADE_DATE").orderBy(
            F.col("VOLUME").desc()
        )

        gold = (
            silver_df
            # 1. Moving averages (trend indicators)
            .withColumn("MA_7D", F.round(F.avg("CLOSE_PRICE").over(window_7d), 2))
            .withColumn("MA_30D", F.round(F.avg("CLOSE_PRICE").over(window_30d), 2))

            # 2. Daily return (% change from previous day)
            #    LAG(col, n) = value from n rows back in the window
            .withColumn(
                "PREV_CLOSE",
                F.lag("CLOSE_PRICE", 1).over(symbol_window),
            )
            .withColumn(
                "DAILY_RETURN_PCT",
                F.round(
                    (F.col("CLOSE_PRICE") - F.col("PREV_CLOSE"))
                    / F.col("PREV_CLOSE") * 100,
                    4,
                ),
            )

            # 3. Rolling volatility (std dev of daily returns over 30 days)
            #    Higher volatility = higher risk
            .withColumn(
                "VOLATILITY_30D",
                F.round(F.stddev("DAILY_RETURN_PCT").over(window_30d), 4),
            )

            # 4. Volume rank per day (1 = most traded stock that day)
            .withColumn("VOLUME_RANK", F.dense_rank().over(daily_window))

            # 5. Clean up helper columns
            .drop("PREV_CLOSE")

            # 6. Add gold layer metadata
            .withColumn("_gold_loaded_at", F.current_timestamp())
        )

        return gold

    def compute_sector_summary(self, gold_df):
        """
        Aggregate gold data into sector-level daily summaries.

        This is a typical GOLD-layer aggregation:
          - Group by sector + date
          - Compute average return, total volume, top performer

        WHY AGGREGATE?
          - Dashboards need pre-computed KPIs (not raw rows)
          - Aggregation on billions of rows is expensive — do it once, store it
          - Business users want "How did IT sector do today?" not raw tick data

        Args:
            gold_df: Gold-layer DataFrame (must have SECTOR column from dim join).

        Returns:
            Sector-level daily summary DataFrame.
        """
        from pyspark.sql import functions as F

        return (
            gold_df
            .filter(F.col("DAILY_RETURN_PCT").isNotNull())
            .groupBy("TRADE_DATE", "EXCHANGE_CODE")
            .agg(
                F.round(F.avg("DAILY_RETURN_PCT"), 4).alias("AVG_RETURN_PCT"),
                F.round(F.avg("CLOSE_PRICE"), 2).alias("AVG_CLOSE_PRICE"),
                F.sum("VOLUME").alias("TOTAL_VOLUME"),
                F.count("*").alias("STOCK_COUNT"),
                F.round(F.avg("VOLATILITY_30D"), 4).alias("AVG_VOLATILITY"),
            )
            .orderBy("TRADE_DATE")
        )


# ==============================================================================
# SQL EQUIVALENTS — For use in Databricks SQL or dbt
# ==============================================================================
# These SQL queries do the SAME thing as the PySpark code above.
# Use these if you prefer SQL or are using dbt for transformations.
# ==============================================================================

BRONZE_TO_SILVER_SQL = """
-- Silver layer: deduplicated, type-cast, validated
CREATE OR REPLACE TABLE silver_stock_prices AS
SELECT DISTINCT
    SYMBOL,
    EXCHANGE_CODE,
    CAST(TRADE_DATE AS DATE) AS TRADE_DATE,
    CAST(OPEN_PRICE AS DOUBLE) AS OPEN_PRICE,
    CAST(HIGH_PRICE AS DOUBLE) AS HIGH_PRICE,
    CAST(LOW_PRICE AS DOUBLE) AS LOW_PRICE,
    CAST(CLOSE_PRICE AS DOUBLE) AS CLOSE_PRICE,
    CAST(VOLUME AS BIGINT) AS VOLUME,
    CAST(DAILY_RANGE AS DOUBLE) AS DAILY_RANGE,
    CAST(CHANGE_PCT AS DOUBLE) AS CHANGE_PCT,
    SOURCE,
    CURRENT_TIMESTAMP() AS _silver_loaded_at,
    'finpipe' AS _source
FROM fact_stock_prices
WHERE SYMBOL IS NOT NULL
  AND TRADE_DATE IS NOT NULL
  AND CLOSE_PRICE IS NOT NULL
"""

SILVER_TO_GOLD_SQL = """
-- Gold layer: enriched with analytics columns
CREATE OR REPLACE TABLE gold_stock_analytics AS
SELECT
    *,
    -- 7-day moving average
    ROUND(AVG(CLOSE_PRICE) OVER (
        PARTITION BY SYMBOL
        ORDER BY TRADE_DATE
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS MA_7D,

    -- 30-day moving average
    ROUND(AVG(CLOSE_PRICE) OVER (
        PARTITION BY SYMBOL
        ORDER BY TRADE_DATE
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ), 2) AS MA_30D,

    -- Daily return percentage
    ROUND(
        (CLOSE_PRICE - LAG(CLOSE_PRICE, 1) OVER (
            PARTITION BY SYMBOL ORDER BY TRADE_DATE
        )) / NULLIF(LAG(CLOSE_PRICE, 1) OVER (
            PARTITION BY SYMBOL ORDER BY TRADE_DATE
        ), 0) * 100,
    4) AS DAILY_RETURN_PCT,

    -- Volume rank per day
    DENSE_RANK() OVER (
        PARTITION BY TRADE_DATE
        ORDER BY VOLUME DESC
    ) AS VOLUME_RANK,

    CURRENT_TIMESTAMP() AS _gold_loaded_at
FROM silver_stock_prices
"""

SECTOR_SUMMARY_SQL = """
-- Daily exchange-level summary
SELECT
    TRADE_DATE,
    EXCHANGE_CODE,
    ROUND(AVG(DAILY_RETURN_PCT), 4) AS AVG_RETURN_PCT,
    ROUND(AVG(CLOSE_PRICE), 2) AS AVG_CLOSE_PRICE,
    SUM(VOLUME) AS TOTAL_VOLUME,
    COUNT(*) AS STOCK_COUNT,
    ROUND(STDDEV(DAILY_RETURN_PCT), 4) AS RETURN_VOLATILITY
FROM gold_stock_analytics
WHERE DAILY_RETURN_PCT IS NOT NULL
GROUP BY TRADE_DATE, EXCHANGE_CODE
ORDER BY TRADE_DATE
"""
