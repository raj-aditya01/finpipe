# ==============================================================================
# models/stock.py — Stock Data Models (OOP: Dataclasses & Pydantic)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Pydantic Models — validated data containers (like structs in C/Go)
#   2. dataclasses vs Pydantic — when to use which
#   3. Serialization — convert objects to dict/JSON and back
#   4. Validation — reject bad data at creation time
#   5. Computed fields — derive values from other fields
#
# WHY DATA MODELS?
#   Without models (raw dicts):
#     stock = {"symbol": "TCS", "price": "3842.50"}
#     stock["pricee"]  ← typo → KeyError at runtime, not caught by IDE
#     stock["price"]   ← it's a STRING, not float → math fails silently
#
#   With models (Pydantic):
#     stock = StockPrice(symbol="TCS", close_price="3842.50")
#     stock.close_price  → 3842.50 (auto-converted to float!)
#     stock.pricee       → AttributeError caught by IDE immediately
#     StockPrice(symbol="", close_price=-1) → ValidationError
#
# DATA MODELING IN DATA ENGINEERING:
#   In production pipelines, data models define the SCHEMA of your data.
#   They are the CONTRACT between:
#     - Ingestion code (what it produces)
#     - Processing code (what it expects)
#     - Database tables (what they store)
#   If the contract breaks, validation catches it before bad data enters your DB.
#
# ==============================================================================

from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class StockSymbol(BaseModel):
    """
    A stock symbol with its metadata.
    
    This represents a DIMENSION in data warehousing terms.
    Dimensions are descriptive attributes — WHO/WHAT/WHERE.
    
    Example: TCS is listed on NSE with sector "IT Services".
    The symbol doesn't change day-to-day — it's reference data.
    """
    symbol: str = Field(description="Ticker symbol (e.g., TCS, RELIANCE, INFY)")
    company_name: str = Field(default="", description="Full company name")
    exchange: str = Field(default="NSE", description="Exchange: NSE or BSE")
    sector: str = Field(default="", description="Industry sector")
    isin: str = Field(default="", description="International Securities ID (unique)")
    
    @field_validator("symbol")
    @classmethod
    def symbol_must_be_uppercase(cls, v: str) -> str:
        """
        Validator — runs automatically when creating a StockSymbol.
        
        WHAT IS @field_validator?
          A hook that runs BEFORE the value is stored.
          StockSymbol(symbol="tcs") → validator converts to "TCS"
          StockSymbol(symbol="")    → validator raises ValueError
          
        WHY @classmethod?
          Validators are called on the CLASS, not an instance.
          The model doesn't exist yet when validation runs.
        """
        if not v.strip():
            raise ValueError("Symbol cannot be empty")
        return v.strip().upper()
    
    @field_validator("exchange")
    @classmethod
    def exchange_must_be_valid(cls, v: str) -> str:
        """Only allow known exchanges."""
        valid = {"NSE", "BSE"}
        upper = v.strip().upper()
        if upper not in valid:
            raise ValueError(f"Exchange must be one of {valid}, got '{v}'")
        return upper


class StockPrice(BaseModel):
    """
    A single day's stock price data.
    
    This represents a FACT in data warehousing terms.
    Facts are measurable events — WHEN did WHAT happen at WHAT value.
    
    STAR SCHEMA CONCEPT:
      fact_stock_prices (this model):
        trade_date, symbol, exchange, open, high, low, close, volume
        
      dim_symbols (StockSymbol model):
        symbol, company_name, sector, isin
        
      The fact table references dimensions via symbol + exchange (foreign keys).
      This is the foundation of data warehousing (Snowflake, Redshift, BigQuery).
    
    WHY STAR SCHEMA?
      ┌──────────────┐       ┌───────────────────────┐       ┌──────────────┐
      │ dim_date     │──────>│ fact_stock_prices      │<──────│ dim_symbol   │
      │ date         │       │ trade_date (FK)        │       │ symbol       │
      │ day_of_week  │       │ symbol (FK)            │       │ company_name │
      │ month        │       │ exchange               │       │ sector       │
      │ quarter      │       │ open_price             │       └──────────────┘
      │ is_holiday   │       │ high_price             │
      └──────────────┘       │ low_price              │
                             │ close_price            │
                             │ volume                 │
                             └───────────────────────┘
    
      - Easy to query: "Average close price by sector and month"
      - Easy to scale: facts grow (billions of rows), dimensions stay small
      - Standard: every analytics tool (Tableau, Looker, Power BI) expects this
    """
    # Identifiers (the "keys")
    symbol: str = Field(description="Stock ticker (e.g., TCS)")
    exchange: str = Field(default="NSE", description="Exchange name")
    trade_date: date = Field(description="Trading date")
    
    # Price data (the "facts" — measurable values)
    open_price: float = Field(ge=0, description="Opening price")
    high_price: float = Field(ge=0, description="Highest price of the day")
    low_price: float = Field(ge=0, description="Lowest price of the day")
    close_price: float = Field(ge=0, description="Closing price")
    
    # Volume
    volume: int = Field(ge=0, default=0, description="Shares traded")
    
    # Metadata
    source: str = Field(default="snowflake", description="Data source")
    fetched_at: datetime = Field(default_factory=datetime.utcnow, description="When this was fetched")
    
    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, v: str) -> str:
        return v.strip().upper()
    
    @model_validator(mode="after")
    def validate_price_consistency(self):
        """
        Cross-field validation — checks relationships BETWEEN fields.
        
        @model_validator runs AFTER all individual fields are validated.
        It receives the fully constructed model, so you can check:
          - high >= low  (always true for stock data)
          - high >= open and high >= close
          - low <= open and low <= close
        
        This catches data quality issues at the model level,
        BEFORE bad data enters your pipeline or database.
        """
        if self.high_price < self.low_price:
            raise ValueError(
                f"high_price ({self.high_price}) cannot be less than "
                f"low_price ({self.low_price}) for {self.symbol} on {self.trade_date}"
            )
        return self
    
    @property
    def daily_range(self) -> float:
        """Price range for the day (high - low)."""
        return round(self.high_price - self.low_price, 2)
    
    @property
    def change_from_open(self) -> float:
        """How much the price changed from open to close."""
        return round(self.close_price - self.open_price, 2)
    
    @property
    def change_pct(self) -> float:
        """Percentage change from open to close."""
        if self.open_price == 0:
            return 0.0
        return round((self.close_price - self.open_price) / self.open_price * 100, 2)
    
    def to_row(self) -> dict:
        """
        Convert to a flat dict suitable for DataFrame rows or SQL INSERT.
        
        model_dump() is Pydantic v2's method to convert to dict.
        (In Pydantic v1, this was .dict() — now deprecated)
        """
        data = self.model_dump()
        data["daily_range"] = self.daily_range
        data["change_pct"] = self.change_pct
        return data


class IngestionResult(BaseModel):
    """
    Result of a data ingestion operation.
    
    This is a VALUE OBJECT — it carries the result of an action,
    not a persistent entity. Used for logging and monitoring.
    """
    symbol: str
    source: str
    rows_fetched: int = 0
    rows_inserted: int = 0
    success: bool = True
    error_message: str = ""
    duration_seconds: float = 0.0
    
    @property
    def summary(self) -> str:
        status = "OK" if self.success else "FAILED"
        return (
            f"[{status}] {self.symbol} from {self.source}: "
            f"{self.rows_fetched} fetched, {self.rows_inserted} inserted "
            f"({self.duration_seconds:.2f}s)"
        )
