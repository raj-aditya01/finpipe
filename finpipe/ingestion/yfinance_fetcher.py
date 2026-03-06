# ==============================================================================
# ingestion/yfinance_fetcher.py — Stock Data Fetcher with Multithreading
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Multithreading — download many stocks in parallel (5x faster)
#   2. Thread pools — manage a fixed number of threads efficiently
#   3. Thread safety — locks, atomic operations, shared state
#   4. Error handling — per-stock errors don't kill the whole pipeline
#   5. OOP — encapsulation, single responsibility, composition
#   6. Generator/Iterator pattern — yield results one by one (memory efficient)
#
# MULTITHREADING vs MULTIPROCESSING vs ASYNC:
#   ┌────────────────┬─────────────────┬─────────────────┬─────────────────┐
#   │ Approach       │ Best for        │ How it works    │ Used here?      │
#   ├────────────────┼─────────────────┼─────────────────┼─────────────────┤
#   │ Threading      │ I/O-bound tasks │ Multiple threads│ YES — network   │
#   │                │ (network, disk) │ share memory    │ calls to Yahoo  │
#   ├────────────────┼─────────────────┼─────────────────┼─────────────────┤
#   │ Multiprocessing│ CPU-bound tasks │ Multiple Python │ NO (use Spark   │
#   │                │ (math, parsing) │ processes       │ for CPU work)   │
#   ├────────────────┼─────────────────┼─────────────────┼─────────────────┤
#   │ Async/Await    │ Many I/O tasks  │ Single thread,  │ ALTERNATIVE     │
#   │                │ (10K+ network)  │ event loop      │ (shown later)   │
#   └────────────────┴─────────────────┴─────────────────┴─────────────────┘
#
#   Our task = download stock data from Yahoo Finance = NETWORK I/O
#   → Threading is the right choice.
#
# WHY THREAD POOL (not raw threads)?
#   Raw threads:
#     for stock in stocks:
#         thread = Thread(target=fetch, args=(stock,))
#         thread.start()  # Creates 500 threads for 500 stocks!
#     → Too many threads = OS overhead, crashes, rate limiting
#
#   Thread Pool:
#     with ThreadPoolExecutor(max_workers=5) as pool:
#         pool.map(fetch, stocks)
#     → Only 5 threads run at once. When one finishes, next stock starts.
#     → Controlled, efficient, no resource explosion.
#
# ==============================================================================

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from datetime import date, timedelta
from threading import Lock
from typing import Optional

import pandas as pd

from finpipe.models.stock import StockPrice, IngestionResult

logger = logging.getLogger(__name__)


# Default Indian stock symbols for testing
DEFAULT_NSE_SYMBOLS = [
    "TCS.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS",
    "HINDUNILVR.NS", "BHARTIARTL.NS", "ITC.NS", "SBIN.NS", "LT.NS",
    "BAJFINANCE.NS", "MARUTI.NS", "HCLTECH.NS", "AXISBANK.NS", "WIPRO.NS",
]


class YFinanceFetcher:
    """
    Fetches stock data from Yahoo Finance with concurrent downloads.
    
    OOP CONCEPTS:
      - Encapsulation: all fetching logic + state is inside this class
      - Single Responsibility: this class ONLY fetches data (doesn't store it)
      - Composition: uses ThreadPoolExecutor (has-a, not is-a)
    
    THREAD SAFETY CONCEPTS:
      - Lock: protects shared counters (_success_count, _error_count)
      - Immutable args: each thread gets its own symbol string (no sharing)
      - No shared mutable state: each thread returns its own DataFrame
    
    Usage:
        fetcher = YFinanceFetcher(max_workers=5)
        results = fetcher.fetch_multiple(["TCS.NS", "RELIANCE.NS"], period="1mo")
        for result in results:
            print(result.summary)
    """
    
    def __init__(self, max_workers: int = 5):
        """
        Args:
            max_workers: Maximum concurrent download threads.
                         5 is safe for Yahoo Finance rate limits.
                         Increase to 10-15 for faster downloads (may get rate limited).
        """
        self._max_workers = max_workers
        
        # Thread-safe counters (protected by a Lock)
        self._lock = Lock()
        self._success_count = 0
        self._error_count = 0
    
    def fetch_single(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        period: str = "1mo",
    ) -> tuple[pd.DataFrame, IngestionResult]:
        """
        Fetch historical data for a single stock.
        
        Args:
            symbol: Yahoo Finance symbol (e.g., "TCS.NS" for NSE, "TCS.BO" for BSE)
            start_date: Start of date range (overrides period)
            end_date: End of date range (default: today)
            period: Alternative to start/end — "1d", "5d", "1mo", "3mo", "1y", "max"
        
        Returns:
            Tuple of (DataFrame with OHLCV data, IngestionResult summary)
        
        WHY return a tuple?
            The DataFrame has the data, the IngestionResult has metadata about
            the fetch operation (success/failure, timing, row count).
            This separation lets the caller decide what to do with each.
        """
        import yfinance as yf  # Lazy import — only load when actually needed
        
        start_time = time.time()
        clean_symbol = symbol.strip().upper()
        
        # Determine exchange from Yahoo Finance suffix
        exchange = "NSE" if clean_symbol.endswith(".NS") else "BSE" if clean_symbol.endswith(".BO") else "UNKNOWN"
        base_symbol = clean_symbol.replace(".NS", "").replace(".BO", "")
        
        try:
            logger.info(f"[fetch] Downloading {clean_symbol}...")
            
            ticker = yf.Ticker(clean_symbol)
            
            # Fetch historical data
            if start_date:
                df = ticker.history(
                    start=start_date.isoformat(),
                    end=(end_date or date.today()).isoformat(),
                )
            else:
                df = ticker.history(period=period)
            
            if df.empty:
                duration = time.time() - start_time
                return pd.DataFrame(), IngestionResult(
                    symbol=base_symbol, source="yfinance",
                    rows_fetched=0, success=True,
                    error_message="No data returned (symbol may be invalid or market closed)",
                    duration_seconds=round(duration, 2),
                )
            
            # Transform Yahoo Finance format → our standard format
            df = df.reset_index()
            df = df.rename(columns={
                "Date": "trade_date",
                "Open": "open_price",
                "High": "high_price",
                "Low": "low_price",
                "Close": "close_price",
                "Volume": "volume",
            })
            
            # Add our columns
            df["symbol"] = base_symbol
            df["exchange_code"] = exchange
            df["source"] = "yfinance"
            df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.date
            
            # Compute derived columns
            df["daily_range"] = (df["high_price"] - df["low_price"]).round(4)
            df["change_pct"] = (
                (df["close_price"] - df["open_price"]) / df["open_price"] * 100
            ).round(4)
            
            # Keep only the columns we need (drop Dividends, Stock Splits, etc.)
            keep_cols = [
                "symbol", "exchange_code", "trade_date",
                "open_price", "high_price", "low_price", "close_price",
                "volume", "daily_range", "change_pct", "source",
            ]
            df = df[[c for c in keep_cols if c in df.columns]]
            
            duration = time.time() - start_time
            
            # Thread-safe counter update
            with self._lock:
                self._success_count += 1
            
            result = IngestionResult(
                symbol=base_symbol, source="yfinance",
                rows_fetched=len(df), success=True,
                duration_seconds=round(duration, 2),
            )
            logger.info(f"[fetch] {result.summary}")
            return df, result
            
        except Exception as e:
            duration = time.time() - start_time
            
            with self._lock:
                self._error_count += 1
            
            result = IngestionResult(
                symbol=base_symbol, source="yfinance",
                rows_fetched=0, success=False,
                error_message=str(e),
                duration_seconds=round(duration, 2),
            )
            logger.error(f"[fetch] {result.summary}")
            return pd.DataFrame(), result
    
    def fetch_multiple(
        self,
        symbols: list[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        period: str = "1mo",
    ) -> list[tuple[pd.DataFrame, IngestionResult]]:
        """
        Fetch data for multiple stocks CONCURRENTLY using a thread pool.
        
        THIS IS THE MULTITHREADING CORE.
        
        HOW IT WORKS:
          1. Create a ThreadPoolExecutor with N worker threads
          2. Submit ALL symbols as tasks to the pool
          3. The pool assigns tasks to free threads (max N at once)
          4. as_completed() yields futures as they finish (not in order!)
          5. We collect all results and return them
        
        THREAD SAFETY IN THIS METHOD:
          - Each thread gets its own `symbol` (immutable string — safe)
          - Each thread creates its own DataFrame (no shared data — safe)
          - Counters use self._lock (mutex — safe)
          - No two threads modify the same object simultaneously
        
        Args:
            symbols: List of Yahoo Finance symbols
            start_date: Optional start date
            end_date: Optional end date
            period: Period string if no date range given
        
        Returns:
            List of (DataFrame, IngestionResult) tuples
        """
        self._success_count = 0
        self._error_count = 0
        results = []
        
        total = len(symbols)
        logger.info(f"[fetch] Starting concurrent download of {total} symbols "
                     f"(max {self._max_workers} threads)")
        
        overall_start = time.time()
        
        # ── THE MULTITHREADING PART ──────────────────────────────────────
        # ThreadPoolExecutor manages a pool of reusable threads.
        # `with` ensures all threads are properly cleaned up when done.
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            
            # Submit all fetch tasks to the pool.
            # executor.submit() returns a Future — a promise of a future result.
            # The dict maps Future → symbol (so we know which symbol each future belongs to).
            future_to_symbol: dict[Future, str] = {
                executor.submit(
                    self.fetch_single, sym, start_date, end_date, period
                ): sym
                for sym in symbols
            }
            
            # as_completed() yields futures as they FINISH (not in submission order).
            # This is more efficient than waiting for all to complete:
            #   - If stock A takes 5s and stock B takes 1s, we get B's result first.
            #   - We can log progress in real-time.
            for i, future in enumerate(as_completed(future_to_symbol), 1):
                sym = future_to_symbol[future]
                try:
                    df, result = future.result()  # .result() blocks until this future is done
                    results.append((df, result))
                    logger.debug(f"[fetch] Progress: {i}/{total} complete")
                except Exception as e:
                    # This catches errors that happen IN the thread
                    logger.error(f"[fetch] Unexpected error for {sym}: {e}")
                    results.append((pd.DataFrame(), IngestionResult(
                        symbol=sym, source="yfinance",
                        success=False, error_message=str(e),
                    )))
        
        total_time = time.time() - overall_start
        logger.info(
            f"[fetch] Completed: {self._success_count} success, "
            f"{self._error_count} errors, {total_time:.2f}s total"
        )
        
        return results
    
    def fetch_to_dataframe(
        self,
        symbols: list[str],
        **kwargs,
    ) -> pd.DataFrame:
        """
        Convenience method: fetch multiple symbols and combine into one DataFrame.
        
        This is the typical usage — you want ALL data in one DataFrame
        for further processing, writing to Snowflake, or Spark transformation.
        """
        results = self.fetch_multiple(symbols, **kwargs)
        
        # Combine all successful DataFrames into one
        dfs = [df for df, result in results if result.success and not df.empty]
        
        if not dfs:
            logger.warning("[fetch] No data fetched for any symbol")
            return pd.DataFrame()
        
        combined = pd.concat(dfs, ignore_index=True)
        logger.info(f"[fetch] Combined DataFrame: {combined.shape[0]} rows × {combined.shape[1]} columns")
        return combined
