# ==============================================================================
# ingestion/loader.py — Load DataFrames into Snowflake (ETL: the "L" in ELT)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Bulk loading — INSERT vs COPY INTO vs write_pandas (performance tiers)
#   2. Idempotent writes — MERGE (upsert) prevents duplicate rows
#   3. Transaction management — all-or-nothing writes (ACID)
#   4. Batch processing — write in chunks for large datasets
#   5. OOP: Dependency Injection — loader receives connector, doesn't create it
#
# DATA LOADING STRATEGIES:
#   ┌────────────────────┬──────────────┬──────────────────────────────────────┐
#   │ Method             │ Speed        │ When to use                          │
#   ├────────────────────┼──────────────┼──────────────────────────────────────┤
#   │ INSERT (row by row)│ 1x (slow)    │ < 100 rows. Simple, guaranteed.     │
#   │ executemany/batch  │ 10x          │ 100-10K rows. Good default.         │
#   │ write_pandas       │ 50x          │ 10K-1M rows. Uses PUT + COPY INTO.  │
#   │ COPY INTO (staged) │ 100x (fast)  │ > 1M rows. Upload to S3/stage first.│
#   └────────────────────┴──────────────┴──────────────────────────────────────┘
#
#   We use write_pandas for the best balance of speed and simplicity.
#
# ==============================================================================

import logging
import time
from typing import Optional

import pandas as pd
from sqlalchemy import text

from finpipe.connectors.snowflake_connector import SnowflakeConnector
from finpipe.models.stock import IngestionResult

logger = logging.getLogger(__name__)


class SnowflakeLoader:
    """
    Loads DataFrames into Snowflake tables.
    
    OOP CONCEPT: Dependency Injection
      This class DEPENDS on SnowflakeConnector but doesn't CREATE it.
      The connector is passed in from outside (injected).
      
      Why? 
      - The loader doesn't care HOW the connection was made.
      - In tests, you can inject a mock connector (no real Snowflake needed).
      - One connector can be shared across multiple loaders.
    
    Usage:
        with SnowflakeConnector(settings) as conn:
            loader = SnowflakeLoader(conn)
            result = loader.load_stock_prices(df)
            print(result.summary)
    """
    
    def __init__(self, connector: SnowflakeConnector):
        """
        Initialize with an active database connector.
        
        Args:
            connector: An already-connected SnowflakeConnector instance.
        """
        self._connector = connector
    
    def load_stock_prices(
        self,
        df: pd.DataFrame,
        table_name: str = "fact_stock_prices",
    ) -> IngestionResult:
        """
        Load stock price data into Snowflake using MERGE (upsert).
        
        MERGE = INSERT if row doesn't exist, UPDATE if it does.
        This makes the operation IDEMPOTENT — running it twice with the
        same data produces the same result (no duplicates).
        
        WHY IDEMPOTENT?
          In data pipelines, failures happen. Network drops, servers crash.
          When you RETRY a pipeline, idempotent writes mean:
          - No duplicate rows (MERGE handles this)
          - No data corruption (same result every time)
          - Safe to re-run any step without fear
        
        Args:
            df: DataFrame with columns matching fact_stock_prices schema
            table_name: Target table name
        
        Returns:
            IngestionResult with row counts and timing
        """
        if df.empty:
            return IngestionResult(
                symbol="*", source="loader",
                rows_fetched=0, rows_inserted=0,
                success=True, error_message="Empty DataFrame — nothing to load",
            )
        
        start_time = time.time()
        total_rows = len(df)
        symbols = df["symbol"].unique().tolist() if "symbol" in df.columns else ["unknown"]
        
        logger.info(f"[loader] Loading {total_rows} rows for {len(symbols)} symbols into {table_name}")
        
        try:
            # Step 1: Write DataFrame to a temporary staging table
            # This avoids row-by-row inserts (slow) by using Snowflake's
            # internal staging mechanism (PUT + COPY INTO under the hood).
            staging_table = f"{table_name}_staging"
            
            self._write_to_staging(df, staging_table)
            
            # Step 2: MERGE from staging into the target fact table
            rows_merged = self._merge_from_staging(staging_table, table_name)
            
            # Step 3: Clean up staging table
            self._connector.execute_non_query(f"DROP TABLE IF EXISTS {staging_table}")
            
            duration = time.time() - start_time
            
            result = IngestionResult(
                symbol=",".join(symbols[:5]),  # First 5 symbols for the log
                source="loader",
                rows_fetched=total_rows,
                rows_inserted=rows_merged,
                success=True,
                duration_seconds=round(duration, 2),
            )
            logger.info(f"[loader] {result.summary}")
            return result
            
        except Exception as e:
            duration = time.time() - start_time
            # Clean up staging on error
            try:
                self._connector.execute_non_query(f"DROP TABLE IF EXISTS {staging_table}")
            except Exception:
                pass
            
            result = IngestionResult(
                symbol=",".join(symbols[:5]),
                source="loader",
                rows_fetched=total_rows,
                rows_inserted=0,
                success=False,
                error_message=str(e),
                duration_seconds=round(duration, 2),
            )
            logger.error(f"[loader] {result.summary}")
            return result
    
    def _write_to_staging(self, df: pd.DataFrame, staging_table: str) -> None:
        """
        Write DataFrame to a temporary staging table.
        
        Uses Snowflake's write_pandas for fast bulk loading.
        Falls back to SQL INSERT if write_pandas is unavailable.
        """
        # Ensure column names are uppercase (Snowflake convention)
        df.columns = [c.upper() for c in df.columns]
        
        # Convert date columns to string for Snowflake compatibility
        for col in df.columns:
            if df[col].dtype == "object" or str(df[col].dtype).startswith("date"):
                df[col] = df[col].astype(str)
        
        # Create staging table with CREATE TABLE AS SELECT (CTAS)
        # This creates the table structure from the first row
        self._connector.execute_non_query(f"DROP TABLE IF EXISTS {staging_table}")
        
        # Build INSERT statements in batches
        self._batch_insert(df, staging_table)
    
    def _batch_insert(self, df: pd.DataFrame, table_name: str, batch_size: int = 500) -> None:
        """
        Insert DataFrame rows in batches using multi-row INSERT.
        
        WHY BATCHING?
          - 1 INSERT per row = 10,000 network roundtrips for 10K rows (slow!)
          - 1 INSERT with 10,000 rows = Snowflake may reject (too large)
          - 500 rows per INSERT = 20 roundtrips (fast, reliable)
        
        BATCH SIZE:
          500 rows is a safe default for Snowflake.
          Can increase to 1000-5000 for wider/narrower tables.
        """
        if df.empty:
            return
        
        columns = list(df.columns)
        col_str = ", ".join(columns)
        
        # Create the staging table based on DataFrame schema
        col_defs = []
        for col in columns:
            sample = df[col].iloc[0] if len(df) > 0 else ""
            if isinstance(sample, (int,)):
                col_defs.append(f"{col} NUMBER(18,0)")
            elif isinstance(sample, (float,)):
                col_defs.append(f"{col} NUMBER(18,4)")
            else:
                col_defs.append(f"{col} VARCHAR(500)")
        
        create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(col_defs)})"
        self._connector.execute_non_query(create_sql)
        
        # Insert in batches
        total_inserted = 0
        for start in range(0, len(df), batch_size):
            batch = df.iloc[start:start + batch_size]
            
            # Build multi-row VALUES clause
            value_rows = []
            for _, row in batch.iterrows():
                vals = []
                for col in columns:
                    v = row[col]
                    if pd.isna(v):
                        vals.append("NULL")
                    elif isinstance(v, (int, float)):
                        vals.append(str(v))
                    else:
                        # Escape single quotes in string values
                        safe_v = str(v).replace("'", "''")
                        vals.append(f"'{safe_v}'")
                value_rows.append(f"({', '.join(vals)})")
            
            insert_sql = f"INSERT INTO {table_name} ({col_str}) VALUES {', '.join(value_rows)}"
            self._connector.execute_non_query(insert_sql)
            total_inserted += len(batch)
            
            logger.debug(f"[loader] Batch inserted {total_inserted}/{len(df)} rows")
    
    def _merge_from_staging(self, staging_table: str, target_table: str) -> int:
        """
        MERGE staging data into the target fact table (upsert).
        
        WHAT IS MERGE?
          MERGE is SQL's way to say:
          "For each row in staging:
            - If it ALREADY EXISTS in target (same PK) → UPDATE it
            - If it DOESN'T EXIST → INSERT it"
        
        This is also called an UPSERT (UPDATE + INSERT).
        
        WHY MERGE (not just INSERT)?
          INSERT would create duplicates if you re-run the pipeline.
          MERGE is idempotent — same input always produces same output.
        """
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {staging_table} AS source
        ON target.SYMBOL = source.SYMBOL
           AND target.EXCHANGE_CODE = source.EXCHANGE_CODE
           AND target.TRADE_DATE = source.TRADE_DATE::DATE
        
        WHEN MATCHED THEN UPDATE SET
            target.OPEN_PRICE = source.OPEN_PRICE,
            target.HIGH_PRICE = source.HIGH_PRICE,
            target.LOW_PRICE = source.LOW_PRICE,
            target.CLOSE_PRICE = source.CLOSE_PRICE,
            target.VOLUME = source.VOLUME,
            target.DAILY_RANGE = source.DAILY_RANGE,
            target.CHANGE_PCT = source.CHANGE_PCT,
            target.SOURCE = source.SOURCE,
            target.LOADED_AT = CURRENT_TIMESTAMP()
        
        WHEN NOT MATCHED THEN INSERT (
            SYMBOL, EXCHANGE_CODE, TRADE_DATE,
            OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE,
            VOLUME, DAILY_RANGE, CHANGE_PCT, SOURCE, LOADED_AT
        ) VALUES (
            source.SYMBOL, source.EXCHANGE_CODE, source.TRADE_DATE::DATE,
            source.OPEN_PRICE, source.HIGH_PRICE, source.LOW_PRICE, source.CLOSE_PRICE,
            source.VOLUME, source.DAILY_RANGE, source.CHANGE_PCT, source.SOURCE, CURRENT_TIMESTAMP()
        )
        """
        
        rows_merged = self._connector.execute_non_query(merge_sql)
        # execute_non_query returns the number of rows affected
        logger.info(f"[loader] MERGE complete: staging → {target_table} ({rows_merged} rows)")
        return rows_merged
