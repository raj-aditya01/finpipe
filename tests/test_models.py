# ==============================================================================
# tests/test_models.py — Unit Tests for Data Models
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Unit testing with pytest — the standard Python testing framework
#   2. Testing validation — ensure bad data is rejected
#   3. Testing properties — computed values
#   4. Testing edge cases — empty strings, negative numbers, etc.
#
# HOW TO RUN:
#   pytest tests/ -v              # Run all tests with verbose output
#   pytest tests/test_models.py   # Run just this file
#   pytest -k "test_stock_price"  # Run tests matching a name pattern
#   pytest --cov=finpipe          # Run with code coverage report
#
# ==============================================================================

from datetime import date
import pytest
from finpipe.models.stock import StockPrice, StockSymbol, IngestionResult
from finpipe.models.enums import Exchange


class TestStockSymbol:
    """Test suite for StockSymbol model."""
    
    def test_creates_valid_symbol(self):
        """Test that a valid symbol is created correctly."""
        sym = StockSymbol(symbol="tcs", exchange="nse")
        # Validators should uppercase both fields
        assert sym.symbol == "TCS"
        assert sym.exchange == "NSE"
    
    def test_rejects_empty_symbol(self):
        """Test that empty symbol raises ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            StockSymbol(symbol="", exchange="NSE")
    
    def test_rejects_invalid_exchange(self):
        """Test that invalid exchange raises ValueError."""
        with pytest.raises(ValueError, match="must be one of"):
            StockSymbol(symbol="TCS", exchange="NYSE")
    
    def test_strips_whitespace(self):
        """Test that whitespace is stripped from symbol."""
        sym = StockSymbol(symbol="  tcs  ", exchange="NSE")
        assert sym.symbol == "TCS"


class TestStockPrice:
    """Test suite for StockPrice model."""
    
    def test_creates_valid_price(self):
        """Test basic creation with valid data."""
        price = StockPrice(
            symbol="TCS",
            trade_date=date(2026, 3, 5),
            open_price=3800.0,
            high_price=3850.0,
            low_price=3780.0,
            close_price=3842.50,
            volume=1000000,
        )
        assert price.symbol == "TCS"
        assert price.close_price == 3842.50
    
    def test_rejects_negative_price(self):
        """Test that negative prices are rejected."""
        with pytest.raises(ValueError):
            StockPrice(
                symbol="TCS", trade_date=date(2026, 3, 5),
                open_price=-100, high_price=100,
                low_price=90, close_price=95,
            )
    
    def test_rejects_high_less_than_low(self):
        """Test cross-field validation: high must be >= low."""
        with pytest.raises(ValueError, match="cannot be less than"):
            StockPrice(
                symbol="TCS", trade_date=date(2026, 3, 5),
                open_price=100, high_price=90,   # high < low!
                low_price=95, close_price=92,
            )
    
    def test_daily_range_property(self):
        """Test computed daily_range property."""
        price = StockPrice(
            symbol="TCS", trade_date=date(2026, 3, 5),
            open_price=100, high_price=110,
            low_price=95, close_price=105,
        )
        assert price.daily_range == 15.0  # 110 - 95
    
    def test_change_pct_property(self):
        """Test computed change_pct property."""
        price = StockPrice(
            symbol="TCS", trade_date=date(2026, 3, 5),
            open_price=100, high_price=110,
            low_price=95, close_price=105,
        )
        assert price.change_pct == 5.0  # (105 - 100) / 100 * 100
    
    def test_to_row_includes_computed(self):
        """Test that to_row() includes computed fields."""
        price = StockPrice(
            symbol="TCS", trade_date=date(2026, 3, 5),
            open_price=100, high_price=110,
            low_price=95, close_price=105,
        )
        row = price.to_row()
        assert "daily_range" in row
        assert "change_pct" in row
        assert row["daily_range"] == 15.0


class TestIngestionResult:
    """Test suite for IngestionResult model."""
    
    def test_success_summary(self):
        result = IngestionResult(
            symbol="TCS", source="yfinance",
            rows_fetched=30, rows_inserted=30,
            success=True, duration_seconds=1.5,
        )
        assert "OK" in result.summary
        assert "TCS" in result.summary
    
    def test_failure_summary(self):
        result = IngestionResult(
            symbol="INVALID", source="yfinance",
            success=False, error_message="Not found",
        )
        assert "FAILED" in result.summary


class TestExchangeEnum:
    """Test suite for Exchange enum."""
    
    def test_enum_values(self):
        assert Exchange.NSE == "NSE"
        assert Exchange.BSE == "BSE"
    
    def test_enum_is_string(self):
        """StrEnum members should work as strings."""
        assert f"Exchange: {Exchange.NSE}" == "Exchange: NSE"
