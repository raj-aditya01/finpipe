# ==============================================================================
# pipeline/stages/extract.py — Extract Stage (the "E" in ETL/ELT)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Concrete stage — implements the abstract PipelineStage contract
#   2. Incremental extraction — only fetch NEW data (watermark-based)
#   3. Integration — wires YFinanceFetcher into the pipeline framework
#   4. Idempotent extraction — safe to re-run without duplicates
#
# INCREMENTAL vs FULL LOAD:
#   Full load:  Fetch ALL data every run (simple but wasteful)
#   Incremental: Fetch only data SINCE last successful load (efficient)
#
#   Watermark = "I have data up to 2024-01-15. Next time, start from 2024-01-16."
#   This is how EVERY production pipeline works at scale.
#
#   Airflow:     execution_date → start of the window
#   Databricks:  Delta Live Tables → change data capture
#   Kafka:       Consumer offset → where you left off
#   Our project: WatermarkStore → stores last-loaded date per symbol
#
# ==============================================================================

import logging
from datetime import date, timedelta

import pandas as pd

from finpipe.pipeline.stage import PipelineStage, StageResult
from finpipe.pipeline.context import PipelineContext
from finpipe.ingestion.yfinance_fetcher import YFinanceFetcher
from finpipe.utils.watermark import WatermarkStore

logger = logging.getLogger(__name__)


class ExtractStage(PipelineStage):
    """
    Fetches stock data from Yahoo Finance.

    Supports two modes:
      1. INCREMENTAL (default): Uses watermarks to fetch only new data.
         Reads the last-loaded date from WatermarkStore, fetches from there.
      2. FULL LOAD: Fetches a fixed period (e.g., "1mo", "1y").
         Ignores watermarks. Used for initial load or backfill.

    The fetched DataFrame is stored in ctx.data["raw_prices"] for the
    next stage (ValidateStage) to pick up.

    OOP CONCEPTS:
      - Concrete class implementing abstract PipelineStage
      - Composition: uses YFinanceFetcher and WatermarkStore
      - Strategy: incremental vs full load is a runtime decision
    """

    def __init__(
        self,
        symbols: list[str],
        max_workers: int = 5,
        period: str = "1mo",
        incremental: bool = True,
    ):
        super().__init__(
            name="extract",
            description="Fetch stock data from Yahoo Finance",
        )
        self.symbols = symbols
        self.max_workers = max_workers
        self.period = period
        self.incremental = incremental
        self._watermarks = WatermarkStore()
        self._fetcher = YFinanceFetcher(max_workers=max_workers)

    def execute(self, ctx: PipelineContext) -> StageResult:
        """
        Fetch data for all symbols (incremental or full).

        Flow:
          1. Determine date range (from watermark or period)
          2. Fetch data for each symbol concurrently
          3. Combine into one DataFrame
          4. Store in ctx.data["raw_prices"]
          5. Return StageResult with row count
        """
        logger.info(
            f"[extract] Starting {'incremental' if self.incremental else 'full'} "
            f"extract for {len(self.symbols)} symbols"
        )

        all_dfs: list[pd.DataFrame] = []
        errors: list[str] = []

        if self.incremental:
            # Incremental: fetch per-symbol from its watermark
            for symbol in self.symbols:
                df, err = self._fetch_incremental(symbol)
                if not df.empty:
                    all_dfs.append(df)
                if err:
                    errors.append(err)
        else:
            # Full load: fetch all symbols for the given period
            results = self._fetcher.fetch_multiple(
                self.symbols, period=self.period
            )
            for df, result in results:
                if result.success and not df.empty:
                    all_dfs.append(df)
                elif not result.success:
                    errors.append(f"{result.symbol}: {result.error_message}")

        # Combine all DataFrames
        if all_dfs:
            combined = pd.concat(all_dfs, ignore_index=True)
        else:
            combined = pd.DataFrame()

        # Store in context for the next stage
        ctx.data["raw_prices"] = combined

        rows_out = len(combined)
        if errors:
            logger.warning(f"[extract] {len(errors)} symbols had errors: {errors[:3]}")

        if rows_out == 0 and not errors:
            return StageResult(
                success=True,
                message="No new data to extract (already up to date)",
                rows_out=0,
            )

        if rows_out == 0 and errors:
            return StageResult(
                success=False,
                message="Extraction failed for all symbols",
                error="; ".join(errors[:3]),
            )

        return StageResult(
            success=True,
            message=f"Extracted {rows_out} rows for {len(all_dfs)} symbols",
            rows_out=rows_out,
        )

    def _fetch_incremental(self, symbol: str) -> tuple[pd.DataFrame, str]:
        """
        Fetch data for one symbol starting from its watermark.

        If the watermark is today or yesterday, there's no new data to fetch.
        This avoids unnecessary API calls.
        """
        watermark_key = f"extract.{symbol}"
        last_date = self._watermarks.get_date(watermark_key)

        if last_date:
            start_date = last_date + timedelta(days=1)
            if start_date >= date.today():
                logger.debug(f"[extract] {symbol}: already up to date (watermark={last_date})")
                return pd.DataFrame(), ""
        else:
            # No watermark → first run, use the configured period
            df, result = self._fetcher.fetch_single(symbol, period=self.period)
            if result.success:
                return df, ""
            return pd.DataFrame(), f"{symbol}: {result.error_message}"

        # Fetch from watermark date to today
        df, result = self._fetcher.fetch_single(
            symbol,
            start_date=start_date,
            end_date=date.today(),
        )
        if result.success:
            return df, ""
        return pd.DataFrame(), f"{symbol}: {result.error_message}"
