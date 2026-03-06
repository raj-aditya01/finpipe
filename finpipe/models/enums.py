# ==============================================================================
# models/enums.py — Enumerations (OOP: Type Safety)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Enums — a fixed set of named constants (better than raw strings)
#   2. Why enums prevent bugs
#   3. StrEnum — enums that behave like strings
#
# WHY ENUMS (not just strings)?
#   Without enums:
#     exchange = "NSE"   ← typo "nse" or "Nse" would silently work but break queries
#     status = "Open"    ← what are the valid statuses? No one knows without docs
#
#   With enums:
#     exchange = Exchange.NSE        ← autocomplete, type-safe
#     exchange = Exchange("INVALID") ← raises ValueError immediately
#     Exchange.NSE.value == "NSE"    ← still a string under the hood
#
# ==============================================================================

from enum import Enum, StrEnum


class Exchange(StrEnum):
    """
    Indian stock exchanges.
    
    StrEnum means each member IS a string:
      Exchange.NSE == "NSE"  → True
      str(Exchange.NSE)      → "NSE"
      
    You can use it directly in f-strings, SQL, etc.
    """
    NSE = "NSE"
    BSE = "BSE"


class MarketStatus(StrEnum):
    """Market trading status."""
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    PRE_MARKET = "PRE_MARKET"
    POST_MARKET = "POST_MARKET"


class TimeFrame(StrEnum):
    """Data granularity for historical queries."""
    DAILY = "1d"
    WEEKLY = "1wk"
    MONTHLY = "1mo"


class DataSource(StrEnum):
    """Where the data was fetched from."""
    SNOWFLAKE = "snowflake"
    YFINANCE = "yfinance"
    NSE_API = "nse_api"
    MANUAL = "manual"
