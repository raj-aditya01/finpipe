# ==============================================================================
# connectors/snowflake_connector.py — Snowflake Database Connector
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Inheritance — SnowflakeConnector extends BaseConnector
#   2. Context Managers — safe connection handling with `with`
#   3. Parameterized Queries — prevent SQL injection
#   4. Connection Pooling concept — reuse connections efficiently
#   5. Error Handling — specific exceptions for specific failures
#   6. Pandas integration — query results as DataFrames
#
# DESIGN PATTERN: Template Method
#   BaseConnector defines the STRUCTURE (connect, disconnect, execute).
#   SnowflakeConnector fills in the DETAILS (Snowflake-specific code).
#   If you make a PostgresConnector tomorrow, you fill in Postgres-specific code
#   but the STRUCTURE stays the same.
#
# HOW TO USE:
#   from finpipe.connectors import SnowflakeConnector
#   from finpipe.config import get_settings
#   
#   settings = get_settings()
#   
#   # Option 1: Context manager (recommended)
#   with SnowflakeConnector(settings) as sf:
#       df = sf.execute_query_df("SELECT * FROM stock_market_data LIMIT 10")
#       print(df)
#   
#   # Option 2: Manual connect/disconnect
#   sf = SnowflakeConnector(settings)
#   sf.connect()
#   results = sf.execute_query("SELECT CURRENT_TIMESTAMP()")
#   sf.disconnect()
#
# ==============================================================================

import logging
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from finpipe.connectors.base import BaseConnector
from finpipe.config.settings import Settings

logger = logging.getLogger(__name__)


class SnowflakeConnector(BaseConnector):
    """
    Snowflake database connector using SQLAlchemy.
    
    OOP CONCEPT: Inheritance
      This class INHERITS from BaseConnector, meaning:
      - It gets all of BaseConnector's methods for free (__enter__, __exit__, etc.)
      - It MUST implement all abstract methods (connect, disconnect, execute_query, etc.)
      - It CAN override non-abstract methods if needed
    
    WHY SQLAlchemy (not raw snowflake.connector)?
      1. SQLAlchemy is database-agnostic — same code works with Postgres, MySQL, etc.
      2. Connection pooling built-in (reuses connections instead of creating new ones)
      3. Parameterized queries prevent SQL injection
      4. Works with Pandas .read_sql() and .to_sql() directly
      5. LangChain, dbt, and most tools use SQLAlchemy under the hood
    """
    
    def __init__(self, settings: Settings):
        """
        Initialize with settings (Dependency Injection pattern).
        
        OOP CONCEPT: Dependency Injection
          Instead of: self.user = os.getenv("SNOWFLAKE_USER")  ← hard to test
          We do:      self._settings = settings                ← pass from outside
          
          This means:
          - In production: pass real Settings (reads .env)
          - In tests: pass fake Settings (no real database needed)
          - The class doesn't KNOW where settings come from → flexible
        
        super().__init__() calls BaseConnector.__init__() — the parent constructor.
        Without this, BaseConnector's __init__ never runs and _connection, _connected
        would never be set → AttributeError later.
        """
        super().__init__(name="snowflake")  # Call parent __init__
        self._settings = settings
        self._engine: Engine | None = None
    
    def connect(self) -> None:
        """
        Create SQLAlchemy engine and verify Snowflake connectivity.
        
        WHY create_engine() and not connect() directly?
          An Engine is a CONNECTION FACTORY. It manages a pool of connections.
          When you call engine.connect(), it:
          1. Checks if a free connection exists in the pool → reuse it
          2. If not → creates a new connection
          3. When done → returns it to the pool (not closed!)
          
          This is MUCH more efficient than open/close/open/close for every query.
        """
        if self._connected:
            logger.debug("[snowflake] Already connected, skipping")
            return
        
        logger.info(f"[snowflake] Connecting to {self._settings.snowflake_safe_display}")
        
        try:
            self._engine = create_engine(
                self._settings.snowflake_uri,
                # Pool settings — how many connections to keep open
                pool_size=5,          # Keep 5 connections in the pool
                max_overflow=10,      # Allow 10 extra under heavy load
                pool_timeout=30,      # Wait 30s for a free connection before error
                pool_recycle=1800,    # Recreate connections every 30min (prevent stale)
            )
            
            # Verify the connection actually works (fail fast)
            with self._engine.connect() as conn:
                result = conn.execute(text("SELECT CURRENT_TIMESTAMP()"))
                ts = result.scalar()
                logger.info(f"[snowflake] Connected successfully. Server time: {ts}")
            
            self._connected = True
            
        except Exception as e:
            self._connected = False
            self._engine = None
            logger.error(f"[snowflake] Connection FAILED: {e}")
            raise ConnectionError(
                f"Cannot connect to Snowflake at {self._settings.snowflake_safe_display}. "
                f"Check your .env credentials. Error: {e}"
            ) from e
    
    def disconnect(self) -> None:
        """
        Dispose the engine and close all pooled connections.
        
        engine.dispose() closes ALL connections in the pool.
        After this, any attempt to use old connections will fail.
        """
        if self._engine:
            self._engine.dispose()
            logger.info("[snowflake] Disconnected (engine disposed)")
        self._engine = None
        self._connected = False
    
    def execute_query(self, query: str, params: dict | None = None) -> list[dict[str, Any]]:
        """
        Execute SQL and return results as a list of dicts.
        
        Args:
            query: SQL string. Use :param_name for bind parameters.
                   Example: "SELECT * FROM stocks WHERE symbol = :sym"
            params: Dict of parameters. Example: {"sym": "TCS"}
        
        Returns:
            List of dicts: [{"symbol": "TCS", "close_price": 3842.50}, ...]
        
        SECURITY: Parameterized Queries
          NEVER do this: f"SELECT * FROM stocks WHERE symbol = '{user_input}'"
          This is SQL INJECTION — a user could type: ' OR 1=1; DROP TABLE stocks; --
          
          ALWAYS do this: execute_query("SELECT ... WHERE symbol = :sym", {"sym": user_input})
          SQLAlchemy escapes the parameter safely.
        
        WHY text()?
          SQLAlchemy's text() marks a string as raw SQL.
          Without it, SQLAlchemy tries to parse the SQL as ORM objects → errors.
        """
        self._ensure_connected()
        
        logger.debug(f"[snowflake] Executing: {query[:100]}...")
        
        with self._engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            # Convert SQLAlchemy Row objects to plain dicts
            rows = [dict(row._mapping) for row in result]
            logger.debug(f"[snowflake] Returned {len(rows)} rows")
            return rows
    
    def execute_query_df(self, query: str, params: dict | None = None) -> pd.DataFrame:
        """
        Execute SQL and return results as a Pandas DataFrame.
        
        WHY DataFrame?
          DataFrames are the LINGUA FRANCA of data engineering.
          Every tool speaks DataFrame:
          - Pandas: df = pd.read_sql(query, engine)
          - PySpark: spark_df = spark.createDataFrame(pandas_df)
          - Snowflake: write_pandas(conn, df, "table_name")
          - Plotly: fig = px.line(df, x="date", y="price")
          
          So we return DataFrames as the standard output format.
        """
        self._ensure_connected()
        
        logger.debug(f"[snowflake] Executing (DataFrame): {query[:100]}...")
        
        with self._engine.connect() as conn:
            df = pd.read_sql(text(query), conn, params=params or {})
            logger.debug(f"[snowflake] Returned DataFrame: {df.shape[0]} rows × {df.shape[1]} columns")
            return df
    
    def execute_non_query(self, query: str, params: dict | None = None) -> int:
        """
        Execute a SQL statement that doesn't return data (INSERT, UPDATE, DELETE, DDL).
        
        Returns:
            Number of rows affected.
        """
        self._ensure_connected()
        
        logger.debug(f"[snowflake] Executing non-query: {query[:100]}...")
        
        with self._engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            conn.commit()
            affected = result.rowcount
            logger.debug(f"[snowflake] Affected {affected} rows")
            return affected
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table or view exists in the current schema."""
        query = """
            SELECT COUNT(*) as cnt
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema
              AND TABLE_NAME = :table_name
        """
        rows = self.execute_query(query, {
            "schema": self._settings.snowflake_schema,
            "table_name": table_name.upper(),
        })
        # SQLAlchemy may return keys as lowercase — handle both casings
        if rows:
            row = rows[0]
            count = row.get("cnt") or row.get("CNT", 0)
            return count > 0
        return False
    
    def get_tables(self) -> list[str]:
        """List all tables and views in the current schema."""
        query = """
            SELECT TABLE_NAME, TABLE_TYPE
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = :schema
            ORDER BY TABLE_TYPE, TABLE_NAME
        """
        rows = self.execute_query(query, {"schema": self._settings.snowflake_schema})
        # SQLAlchemy may return column names as lowercase or uppercase
        # depending on driver version — handle both
        results = []
        for r in rows:
            name = r.get("TABLE_NAME") or r.get("table_name", "")
            ttype = r.get("TABLE_TYPE") or r.get("table_type", "")
            results.append(f"{name} ({ttype})")
        return results
    
    def get_row_count(self, table_name: str) -> int:
        """Get the row count of a table or view."""
        # Using identifier directly since table names come from our schema inspection,
        # not user input. For user-supplied names, validate against get_tables() first.
        rows = self.execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        if rows:
            row = rows[0]
            return row.get("cnt") or row.get("CNT", 0)
        return 0
    
    # ─── Private Helpers ─────────────────────────────────────────────────
    
    def _ensure_connected(self) -> None:
        """
        Guard method — raise if not connected.
        
        OOP CONCEPT: Encapsulation
          The underscore prefix means "private — for internal use only."
          External code should never call connector._ensure_connected().
          It's an implementation detail.
        """
        if not self._connected or not self._engine:
            raise ConnectionError(
                "Not connected to Snowflake. Call connect() first or use context manager: "
                "with SnowflakeConnector(settings) as conn: ..."
            )
