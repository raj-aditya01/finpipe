# ==============================================================================
# tests/test_utils.py — Tests for Retry and Watermark Utilities
# ==============================================================================

import json
import os
import tempfile
import time
from datetime import date

import pytest

from finpipe.utils.retry import retry_with_backoff
from finpipe.utils.watermark import WatermarkStore


# ==============================================================================
# Tests: retry_with_backoff
# ==============================================================================

class TestRetryWithBackoff:

    def test_no_retry_on_success(self):
        """Function that succeeds is called exactly once."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        result = succeed()
        assert result == "ok"
        assert call_count == 1

    def test_retries_on_failure(self):
        """Function that fails is retried."""
        call_count = 0

        @retry_with_backoff(max_retries=3, base_delay=0.01)
        def eventually_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("not yet")
            return "done"

        result = eventually_succeed()
        assert result == "done"
        assert call_count == 3

    def test_raises_after_max_retries(self):
        """If all retries fail, the last exception propagates."""
        @retry_with_backoff(max_retries=2, base_delay=0.01)
        def always_fail():
            raise ConnectionError("down")

        with pytest.raises(ConnectionError, match="down"):
            always_fail()

    def test_retryable_exceptions_filter(self):
        """Only specified exceptions are retried; others propagate immediately."""
        call_count = 0

        @retry_with_backoff(
            max_retries=3,
            base_delay=0.01,
            retryable_exceptions=(ConnectionError,),
        )
        def wrong_error():
            nonlocal call_count
            call_count += 1
            raise ValueError("not retryable")

        with pytest.raises(ValueError, match="not retryable"):
            wrong_error()
        assert call_count == 1  # No retries for non-matching exception

    def test_exponential_delay(self):
        """Verify delays increase exponentially (roughly)."""
        @retry_with_backoff(max_retries=3, base_delay=0.05, jitter=False)
        def fail_twice():
            if not hasattr(fail_twice, "_count"):
                fail_twice._count = 0
            fail_twice._count += 1
            if fail_twice._count < 3:
                raise ValueError("retry")
            return "ok"

        start = time.time()
        result = fail_twice()
        elapsed = time.time() - start
        assert result == "ok"
        # base_delay=0.05: attempt 1 waits 0.05s, attempt 2 waits 0.10s = 0.15s total
        assert elapsed >= 0.10  # At least some delay


# ==============================================================================
# Tests: WatermarkStore
# ==============================================================================

class TestWatermarkStore:

    @pytest.fixture
    def store(self, tmp_path):
        """Create a WatermarkStore with a temp directory."""
        return WatermarkStore(directory=tmp_path)

    def test_set_and_get(self, store):
        store.set("test.key", "2024-01-15")
        assert store.get("test.key") == "2024-01-15"

    def test_get_missing_returns_none(self, store):
        assert store.get("nonexistent") is None

    def test_get_date(self, store):
        store.set("date.key", "2024-06-15")
        d = store.get_date("date.key")
        assert d == date(2024, 6, 15)

    def test_get_date_missing_returns_none(self, store):
        assert store.get_date("missing") is None

    def test_delete(self, store):
        store.set("del.key", "value")
        store.delete("del.key")
        assert store.get("del.key") is None

    def test_reset(self, store):
        store.set("a", "1")
        store.set("b", "2")
        store.reset()
        assert store.all() == {}

    def test_all(self, store):
        store.set("x", "1")
        store.set("y", "2")
        all_wm = store.all()
        assert all_wm == {"x": "1", "y": "2"}

    def test_persistence(self, tmp_path):
        """Data survives across WatermarkStore instances (same directory)."""
        store1 = WatermarkStore(directory=tmp_path)
        store1.set("persist.key", "hello")

        store2 = WatermarkStore(directory=tmp_path)
        assert store2.get("persist.key") == "hello"

    def test_overwrite_value(self, store):
        store.set("key", "old")
        store.set("key", "new")
        assert store.get("key") == "new"


# ==============================================================================
# Tests: QualitySuite
# ==============================================================================

class TestQualityChecks:

    def _make_df(self):
        import pandas as pd
        return pd.DataFrame({
            "symbol": ["TCS", "INFY", "RELIANCE"],
            "price": [3800.0, 1500.0, 2400.0],
            "volume": [100000, 200000, 150000],
            "trade_date": ["2024-01-01", "2024-01-01", "2024-01-01"],
        })

    def test_not_null_passes(self):
        from finpipe.quality.checks import not_null
        check = not_null("symbol")
        result = check.run(self._make_df())
        assert result.passed

    def test_not_null_fails(self):
        from finpipe.quality.checks import not_null
        df = self._make_df()
        df.loc[0, "symbol"] = None
        result = not_null("symbol").run(df)
        assert not result.passed

    def test_unique_passes(self):
        from finpipe.quality.checks import unique
        check = unique("symbol")
        result = check.run(self._make_df())
        assert result.passed

    def test_unique_fails(self):
        from finpipe.quality.checks import unique
        import pandas as pd
        df = self._make_df()
        # Add a duplicate
        df = pd.concat([df, df.iloc[[0]]], ignore_index=True)
        result = unique("symbol").run(df)
        assert not result.passed

    def test_in_range_passes(self):
        from finpipe.quality.checks import in_range
        result = in_range("price", min_val=0, max_val=10000).run(self._make_df())
        assert result.passed

    def test_in_range_fails(self):
        from finpipe.quality.checks import in_range
        df = self._make_df()
        df.loc[0, "price"] = -100  # Negative
        result = in_range("price", min_val=0).run(df)
        assert not result.passed

    def test_row_count_between_passes(self):
        from finpipe.quality.checks import row_count_between
        result = row_count_between(min_rows=1, max_rows=10).run(self._make_df())
        assert result.passed

    def test_row_count_between_fails(self):
        from finpipe.quality.checks import row_count_between
        result = row_count_between(min_rows=10, max_rows=100).run(self._make_df())
        assert not result.passed

    def test_schema_match_passes(self):
        from finpipe.quality.checks import schema_match
        result = schema_match(["symbol", "price", "volume"]).run(self._make_df())
        assert result.passed

    def test_schema_match_fails(self):
        from finpipe.quality.checks import schema_match
        result = schema_match(["symbol", "MISSING_COL"]).run(self._make_df())
        assert not result.passed

    def test_suite_critical_passed(self):
        from finpipe.quality.checks import QualitySuite, not_null, Severity
        suite = QualitySuite("test")
        suite.add(not_null("symbol", severity=Severity.CRITICAL))
        suite.add(not_null("price", severity=Severity.CRITICAL))
        result = suite.run(self._make_df())
        assert result.critical_passed

    def test_suite_critical_failed(self):
        from finpipe.quality.checks import QualitySuite, not_null, Severity
        import pandas as pd
        suite = QualitySuite("test")
        suite.add(not_null("symbol", severity=Severity.CRITICAL))
        df = self._make_df()
        df.loc[0, "symbol"] = None
        result = suite.run(df)
        assert not result.critical_passed

    def test_custom_check(self):
        from finpipe.quality.checks import custom_check
        check = custom_check(
            name="positive_prices",
            check_fn=lambda df: (
                bool((df["price"] > 0).all()),
                "All prices positive" if (df["price"] > 0).all() else "Negative prices found",
                {},
            ),
        )
        result = check.run(self._make_df())
        assert result.passed
