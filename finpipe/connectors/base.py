# ==============================================================================
# connectors/base.py — Abstract Base Connector (OOP: Abstraction & Inheritance)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Abstract Base Classes (ABC) — define a "contract" that subclasses MUST follow
#   2. Inheritance — SnowflakeConnector IS-A BaseConnector
#   3. Context Managers — the `with` statement for safe resource cleanup
#   4. Type hints — enforce method signatures
#   5. Docstrings — professional code documentation
#
# WHY ABSTRACT CLASSES?
#   Imagine you later add PostgresConnector, DuckDBConnector, BigQueryConnector.
#   ALL must have connect(), disconnect(), execute_query(), etc.
#   
#   Without ABC: each connector invents its own method names
#     snowflake.open()  vs  postgres.connect()  vs  duckdb.start()  ← chaos
#   
#   With ABC: BaseConnector says "you MUST implement connect()"
#     All connectors follow the SAME interface → interchangeable.
#     This is the "Liskov Substitution Principle" (the L in SOLID).
#
# OOP PRINCIPLES DEMONSTRATED:
#   - Abstraction: hide complex details behind simple interface
#   - Encapsulation: internal state (_connection, _connected) is private
#   - Inheritance: subclasses extend this base
#   - Polymorphism: any BaseConnector subclass works the same way
#
# ==============================================================================

from abc import ABC, abstractmethod
from typing import Any
import logging
import pandas as pd

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract base class for all database connectors.
    
    WHAT IS ABC?
      ABC = Abstract Base Class. You CANNOT create a BaseConnector() directly.
      You MUST create a subclass (like SnowflakeConnector) that implements
      all the @abstractmethod methods.
    
      Trying: BaseConnector()  → TypeError: Can't instantiate abstract class
      Correct: SnowflakeConnector()  → Works (it implements all abstract methods)
    
    WHAT IS A CONTEXT MANAGER?
      The `with` statement ensures cleanup happens even if code crashes:
      
      with SnowflakeConnector() as conn:    # calls __enter__ → connect()
          conn.execute_query("SELECT 1")    # use the connection
      # automatically calls __exit__ → disconnect() (even if error!)
      
      Without `with`, you'd need try/finally everywhere:
        conn = SnowflakeConnector()
        try:
            conn.connect()
            conn.execute_query("SELECT 1")
        finally:
            conn.disconnect()  # must remember this or leak connections!
    """
    
    def __init__(self, name: str = "base"):
        """
        OOP CONCEPT: Constructor (__init__).
        Called when you do: connector = SnowflakeConnector()
        
        The underscore prefix (_connection, _connected) is a Python convention
        meaning "private — don't access from outside the class."
        It's not enforced (Python trusts you), but it signals intent.
        """
        self._name = name             # Connector name for logging
        self._connection = None       # Will hold the actual DB connection
        self._connected = False       # Track state
        logger.debug(f"[{self._name}] Connector initialized")
    
    # ─── Abstract Methods (subclasses MUST implement these) ───────────────
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the database.
        
        @abstractmethod means: this method has NO implementation here.
        Any class inheriting BaseConnector MUST provide its own connect().
        If it doesn't → Python raises TypeError at class creation time.
        """
        pass
    
    @abstractmethod
    def disconnect(self) -> None:
        """Close the database connection and release resources."""
        pass
    
    @abstractmethod
    def execute_query(self, query: str, params: dict | None = None) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return results as a list of dicts.
        
        Args:
            query: SQL string to execute
            params: Optional dict of bind parameters (prevents SQL injection)
        
        Returns:
            List of dicts, each dict = one row {column_name: value}
        """
        pass
    
    @abstractmethod
    def execute_query_df(self, query: str, params: dict | None = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a Pandas DataFrame.
        
        DataFrames are the standard data structure in data engineering.
        Every tool (Spark, Snowflake, DuckDB) converts to/from DataFrames.
        """
        pass
    
    # ─── Concrete Methods (shared by all subclasses) ─────────────────────
    
    @property
    def is_connected(self) -> bool:
        """
        Check if connector is currently connected.
        
        @property makes this behave like an attribute:
          if connector.is_connected:    ← no parentheses needed
        """
        return self._connected
    
    def __enter__(self):
        """
        Context manager entry — called by `with` statement.
        
        `with SnowflakeConnector() as conn:` calls:
          1. __init__() first (creates object)
          2. __enter__() second (connects)
        """
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit — called when leaving `with` block.
        
        Called even if an exception occurred inside the `with` block.
        This guarantees cleanup (no leaked database connections).
        
        Args:
            exc_type: Exception class (or None if no error)
            exc_val:  Exception instance (or None)
            exc_tb:   Traceback (or None)
        
        Returns:
            False → re-raise the exception (don't swallow errors)
        """
        self.disconnect()
        return False  # Don't suppress exceptions
    
    def __repr__(self) -> str:
        """
        Developer-friendly string representation.
        
        OOP CONCEPT: __repr__ is called when you print() or inspect an object.
          print(connector)  → <SnowflakeConnector(connected=True)>
        
        __repr__ is for developers. __str__ is for users.
        When in doubt, implement __repr__ — Python uses it as fallback for __str__.
        """
        return f"<{self.__class__.__name__}(name={self._name}, connected={self._connected})>"
