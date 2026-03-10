# Databricks notebook source
# MAGIC %md
# MAGIC # finpipe Medallion Architecture
# MAGIC Bronze (Snowflake) → Silver (Delta) → Gold (Delta)

# COMMAND ----------
# MAGIC %pip install /Workspace/Users/adityaofficialuse@gmail.com/finpipe

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from finpipe.databricks.transforms import SparkTransforms

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Read Bronze Data from Snowflake

# COMMAND ----------
# Snowflake connection options
# Store credentials in Databricks Secrets:
# databricks secrets create-scope snowflake
# databricks secrets put-secret snowflake password

snowflake_options = {
    "sfUrl": "WIJWAKI-RYC26370.snowflakecomputing.com",
    "sfUser": "ADITYA",
    "sfPassword": dbutils.secrets.get("snowflake", "password"),  # Use secrets!
    "sfDatabase": "STOCK_MARKET_DB",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
}

# Read from Snowflake fact table (Bronze layer)
bronze_df = (
    spark.read
    .format("snowflake")
    .options(**snowflake_options)
    .option("query", "SELECT * FROM fact_stock_prices")
    .load()
)

print(f"Bronze records: {bronze_df.count()}")
bronze_df.show(5)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Bronze → Silver (Clean & Validate)

# COMMAND ----------
st = SparkTransforms(spark)

# Bronze → Silver (deduplicate, type-cast, validate)
silver_df = st.bronze_to_silver(bronze_df)

# Save to Delta Lake
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver_stock_prices")

print(f"Silver records: {silver_df.count()}")
silver_df.show(5)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Silver → Gold (Enrich with Analytics)

# COMMAND ----------
# Silver → Gold (moving averages, returns, volatility)
gold_df = st.silver_to_gold(silver_df)
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold_stock_analytics")

print(f"Gold records: {gold_df.count()}")
gold_df.show(5)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Compute Sector Summary (Gold Aggregations)

# COMMAND ----------
# Sector summary (aggregated KPIs)
summary_df = st.compute_sector_summary(gold_df)
summary_df.write.format("delta").mode("overwrite").saveAsTable("gold_sector_summary")

print(f"Sector summary records: {summary_df.count()}")
summary_df.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Completed!
# MAGIC 
# MAGIC Tables created:
# MAGIC - `silver_stock_prices` (cleaned data)
# MAGIC - `gold_stock_analytics` (enriched with technical indicators)
# MAGIC - `gold_sector_summary` (sector-level KPIs)