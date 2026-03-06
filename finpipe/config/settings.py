# ==============================================================================
# config/settings.py — Type-Safe Configuration with Pydantic Settings
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Pydantic Settings — load .env into typed Python objects (not raw strings)
#   2. Singleton Pattern — only ONE settings object exists (OOP design pattern)
#   3. Validation — wrong types fail FAST at startup, not at runtime
#   4. 12-Factor App — config from environment, not hardcoded
#
# WHY NOT just os.getenv()?
#   os.getenv("SNOWFLAKE_PORT") returns "5432" (a STRING, not int).
#   You'd need int(os.getenv("SNOWFLAKE_PORT", "5432")) everywhere.
#   Pydantic does this automatically + validates + gives you autocomplete.
#
# HOW IT WORKS:
#   1. Pydantic reads your .env file (or real env vars in Docker)
#   2. Casts each value to the declared Python type (str, int, bool, etc.)
#   3. Raises a clear error if required vars are missing
#   4. You access settings.snowflake_user (not os.getenv("SNOWFLAKE_USER"))
#
# OOP CONCEPTS:
#   - Inheritance: Settings inherits from BaseSettings
#   - Class attributes with type hints (data modeling)
#   - Inner class (model_config) for meta-configuration
#   - Singleton pattern via functools.lru_cache
#   - Properties for computed values
#
# ==============================================================================

from functools import lru_cache
from pydantic_settings import BaseSettings
from pydantic import Field


class SnowflakeSettings(BaseSettings):
    """
    Snowflake connection settings.
    
    OOP CONCEPT: This is a 'data class' — a class whose primary purpose
    is to hold data (not behavior). Pydantic makes it validated + typed.
    
    Every field maps to an environment variable:
      snowflake_user → SNOWFLAKE_USER in .env
    
    Pydantic automatically reads .env (case-insensitive matching).
    """
    
    # Required fields — app won't start without these
    snowflake_user: str = Field(description="Snowflake login username")
    snowflake_password: str = Field(description="Snowflake login password")
    snowflake_account: str = Field(description="Account identifier (e.g., xy12345.us-east-1)")
    snowflake_database: str = Field(default="STOCK_DATA", description="Target database")
    snowflake_schema: str = Field(default="PUBLIC", description="Target schema")
    snowflake_warehouse: str = Field(default="COMPUTE_WH", description="Compute warehouse")
    snowflake_role: str = Field(default="ACCOUNTADMIN", description="Snowflake role")
    
    @property
    def connection_string(self) -> str:
        """
        Build the SQLAlchemy connection URI for Snowflake.
        
        OOP CONCEPT: A @property looks like an attribute but runs code.
          settings.connection_string  ← looks like data access
          But internally it builds the URI string every time.
        
        WHY a property (not a regular method)?
          - Reads like data: settings.connection_string
          - vs method call: settings.get_connection_string()
          - Properties are idiomatic Python for computed attributes.
        """
        return (
            f"snowflake://{self.snowflake_user}:{self.snowflake_password}"
            f"@{self.snowflake_account}/{self.snowflake_database}/{self.snowflake_schema}"
            f"?warehouse={self.snowflake_warehouse}&role={self.snowflake_role}"
        )
    
    @property
    def safe_display(self) -> str:
        """Connection info WITHOUT the password (for logging)."""
        return (
            f"snowflake://{self.snowflake_user}:***"
            f"@{self.snowflake_account}/{self.snowflake_database}/{self.snowflake_schema}"
        )


class Settings(BaseSettings):
    """
    Root settings — aggregates all config sections.
    
    OOP CONCEPT: Composition — Settings HAS-A SnowflakeSettings.
    (vs Inheritance where Settings IS-A SnowflakeSettings)
    
    Composition is preferred when combining unrelated configs:
      settings.snowflake.snowflake_user  ← clear which section
      vs settings.snowflake_user         ← ambiguous if you add AWS/Redis config later
    
    But for simplicity here, we flatten Snowflake fields into the root.
    """
    
    # --- Snowflake (flattened for quick access) ---
    snowflake_user: str = Field(description="Snowflake login username")
    snowflake_password: str = Field(description="Snowflake login password")
    snowflake_account: str = Field(description="Account identifier")
    snowflake_database: str = Field(default="STOCK_DATA")
    snowflake_schema: str = Field(default="PUBLIC")
    snowflake_warehouse: str = Field(default="COMPUTE_WH")
    snowflake_role: str = Field(default="ACCOUNTADMIN")
    
    # --- App settings ---
    log_level: str = Field(default="INFO", description="Logging level")
    cache_ttl_seconds: int = Field(default=300, description="Cache TTL in seconds")
    max_concurrent_downloads: int = Field(default=5, description="Max parallel stock downloads")
    
    # --- Pipeline settings ---
    # PIPELINE_ENV controls environment-specific behavior:
    #   dev:     verbose logging, small batches, relaxed quality checks
    #   staging: production-like settings, stricter quality checks
    #   prod:    optimized settings, strict quality checks, full retry
    pipeline_env: str = Field(default="dev", description="Environment: dev, staging, prod")
    pipeline_max_retries: int = Field(default=2, description="Max stage retries")
    pipeline_retry_delay: float = Field(default=5.0, description="Seconds between retries")
    pipeline_symbols: str = Field(
        default="TCS.NS,RELIANCE.NS,INFY.NS,HDFCBANK.NS,ICICIBANK.NS",
        description="Comma-separated stock symbols for pipeline",
    )
    
    # --- API Keys (optional) ---
    groq_api_key: str = Field(default="", description="Groq API key (for AI features)")
    
    @property
    def symbols_list(self) -> list[str]:
        """Parse comma-separated symbols into a list."""
        return [s.strip() for s in self.pipeline_symbols.split(",") if s.strip()]
    
    @property
    def snowflake_uri(self) -> str:
        """SQLAlchemy-compatible Snowflake connection URI."""
        return (
            f"snowflake://{self.snowflake_user}:{self.snowflake_password}"
            f"@{self.snowflake_account}/{self.snowflake_database}/{self.snowflake_schema}"
            f"?warehouse={self.snowflake_warehouse}&role={self.snowflake_role}"
        )
    
    @property
    def snowflake_safe_display(self) -> str:
        """Connection info for logging (no password)."""
        return (
            f"{self.snowflake_user}@{self.snowflake_account}"
            f"/{self.snowflake_database}/{self.snowflake_schema}"
        )
    
    model_config = {
        # Tell Pydantic to read from .env file
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        # Accept extra env vars without erroring (e.g., REDIS_HOST)
        "extra": "ignore",
    }


# ==============================================================================
# SINGLETON PATTERN — Only one Settings instance
# ==============================================================================
# WHY:
#   Reading .env and validating 20 variables is work. You don't want to do it
#   on every function call. lru_cache means: do it ONCE, reuse forever.
#
# OOP CONCEPT: Singleton — ensures a class has only ONE instance.
#   Normal:    Settings()  → creates a NEW object every time (wasteful)
#   Singleton: get_settings()  → returns the SAME object every time (efficient)
#
# functools.lru_cache is the Pythonic way to implement singletons.
# (vs the classic Java-style Singleton with __new__ override)
# ==============================================================================

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Get the application settings (singleton).
    
    First call: reads .env, validates, creates Settings object.
    Subsequent calls: returns the cached object instantly.
    
    Usage:
        from finpipe.config import get_settings
        settings = get_settings()
        print(settings.snowflake_database)  # "STOCK_DATA"
    """
    return Settings()
