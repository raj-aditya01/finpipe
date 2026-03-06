# ==============================================================================
# tests/test_ingestion.py — Unit Tests for Data Ingestion Layer
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Mocking external APIs (yfinance) — never call real APIs in unit tests
#   2. Testing multithreaded code — verify thread pool behavior
#   3. DataFrame assertions with pandas
#   4. Testing error handling — what happens when an API fails
#
# HOW YFinanceFetcher.fetch_single() WORKS:
#   It uses yf.Ticker(symbol).history() and returns a TUPLE:
#     (pd.DataFrame, IngestionResult)
#   NOT a list of StockPrice objects.
#
# ==============================================================================

from datetime import date
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest
from finpipe.ingestion.yfinance_fetcher import YFinanceFetcher, DEFAULT_NSE_SYMBOLS


def _make_history_df(
    open_val=3800.0, high_val=3850.0, low_val=3780.0,
    close_val=3842.0, volume=1000000, date_str="2026-03-05",
):
    """Helper: build a DataFrame that looks like yf.Ticker().history() output."""
    return pd.DataFrame({
        "Open": [open_val],
        "High": [high_val],
        "Low": [low_val],
        "Close": [close_val],
        "Volume": [volume],
    }, index=pd.DatetimeIndex([pd.Timestamp(date_str)], name="Date"))


def _make_mock_ticker(history_return=None, history_side_effect=None):
    """Create a mock yfinance.Ticker with pre-configured .history()."""
    mock_ticker = MagicMock()
    if history_side_effect:
        mock_ticker.history.side_effect = history_side_effect
    elif history_return is not None:
        mock_ticker.history.return_value = history_return
    else:
        mock_ticker.history.return_value = pd.DataFrame()
    return mock_ticker


def _patch_yfinance():
    """Patch yfinance at the import target used by the lazy import inside fetch_single."""
    # yfinance is imported lazily as `import yfinance as yf` inside the function,
    # so we patch the builtins __import__ or better yet, patch via sys.modules.
    return patch("yfinance.Ticker")


class TestYFinanceFetcher:
    """Test YFinanceFetcher with mocked yfinance calls."""

    def setup_method(self):
        """Called before each test — fresh fetcher instance."""
        self.fetcher = YFinanceFetcher(max_workers=2)

    @patch("yfinance.Ticker")
    def test_fetch_single_valid(self, mock_ticker_cls):
        """Test fetching a single symbol returns (DataFrame, IngestionResult)."""
        mock_ticker_cls.return_value = _make_mock_ticker(_make_history_df())

        df, result = self.fetcher.fetch_single("TCS.NS", period="1d")

        assert not df.empty
        assert result.success is True
        assert result.symbol == "TCS"
        assert result.rows_fetched == 1
        assert "close_price" in df.columns
        assert df.iloc[0]["close_price"] == 3842.0

    @patch("yfinance.Ticker")
    def test_fetch_single_empty(self, mock_ticker_cls):
        """Test that empty API response returns empty DataFrame + result."""
        mock_ticker_cls.return_value = _make_mock_ticker(pd.DataFrame())

        df, result = self.fetcher.fetch_single("INVALID.NS", period="1d")
        assert df.empty
        assert result.rows_fetched == 0

    @patch("yfinance.Ticker")
    def test_fetch_multiple_uses_thread_pool(self, mock_ticker_cls):
        """Test that fetch_multiple handles multiple symbols concurrently."""
        mock_ticker_cls.return_value = _make_mock_ticker(_make_history_df())

        symbols = ["TCS.NS", "INFY.NS"]
        results = self.fetcher.fetch_multiple(symbols, period="1d")

        # Should have a result tuple for each symbol
        assert len(results) == 2
        for df, result in results:
            assert result.success is True

    @patch("yfinance.Ticker")
    def test_fetch_to_dataframe(self, mock_ticker_cls):
        """Test that fetch_to_dataframe returns a combined DataFrame."""
        mock_ticker_cls.return_value = _make_mock_ticker(_make_history_df())

        df = self.fetcher.fetch_to_dataframe(["TCS.NS"], period="1d")

        assert isinstance(df, pd.DataFrame)
        assert "symbol" in df.columns
        assert "close_price" in df.columns

    @patch("yfinance.Ticker")
    def test_fetch_handles_api_error(self, mock_ticker_cls):
        """Test graceful handling when yfinance raises an exception."""
        mock_ticker_cls.return_value = _make_mock_ticker(
            history_side_effect=Exception("API down")
        )

        df, result = self.fetcher.fetch_single("TCS.NS", period="1d")
        assert df.empty
        assert result.success is False
        assert "API down" in result.error_message

    def test_default_symbols_have_ns_suffix(self):
        """Test that DEFAULT_NSE_SYMBOLS all have .NS suffix."""
        for sym in DEFAULT_NSE_SYMBOLS:
            assert sym.endswith(".NS"), f"{sym} missing .NS suffix"
