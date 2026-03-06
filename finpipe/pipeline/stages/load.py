# ==============================================================================
# pipeline/stages/load.py — Load Stage (the "L" in ETL/ELT)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Loading validated data into Snowflake
#   2. Watermark update AFTER successful load (not before!)
#   3. Atomicity — if the load fails, watermark is NOT updated
#   4. Idempotent loads — MERGE ensures re-runs don't create duplicates
#
# WHY UPDATE WATERMARK AFTER LOAD?
#   If you update the watermark BEFORE loading:
#     1. Extract → set watermark to 2024-01-15 ✓
#     2. Load → FAILS (Snowflake down) ✗
#     3. Re-run → watermark says 2024-01-15, skips that data!
#     4. DATA LOSS — you never loaded Jan 15 but skipped it.
#
#   If you update the watermark AFTER loading:
#     1. Extract → fetch data up to 2024-01-15 ✓
#     2. Load → FAILS (Snowflake down) ✗
#     3. Re-run → watermark still says 2024-01-14 → re-fetches Jan 15 ✓
#     4. MERGE handles duplicates → no double-counting ✓
#
#   This is AT-LEAST-ONCE delivery. Combined with MERGE (idempotent writes),
#   it gives you EXACTLY-ONCE semantics for the data.
#
# ==============================================================================

import logging
from datetime import date

import pandas as pd

from finpipe.pipeline.stage import PipelineStage, StageResult
from finpipe.pipeline.context import PipelineContext
from finpipe.connectors.snowflake_connector import SnowflakeConnector
from finpipe.ingestion.loader import SnowflakeLoader
from finpipe.config.settings import Settings, get_settings
from finpipe.utils.watermark import WatermarkStore

logger = logging.getLogger(__name__)


class LoadStage(PipelineStage):
    """
    Loads validated data into Snowflake and updates watermarks.

    This stage:
      1. Reads ctx.data["validated_prices"] (from ValidateStage)
      2. Opens a Snowflake connection
      3. Calls SnowflakeLoader.load_stock_prices() (MERGE/upsert)
      4. On success: updates watermarks per symbol
      5. On failure: does NOT update watermarks (so next run retries)

    OOP CONCEPTS:
      - Composition: uses SnowflakeConnector + SnowflakeLoader
      - Dependency Injection: Settings injected (or uses singleton)
      - Single Responsibility: only loads, doesn't fetch or validate
    """

    def __init__(
        self,
        settings: Settings | None = None,
        table_name: str = "fact_stock_prices",
    ):
        super().__init__(
            name="load",
            description="Load validated data into Snowflake",
        )
        self._settings = settings or get_settings()
        self._table_name = table_name
        self._watermarks = WatermarkStore()

    def execute(self, ctx: PipelineContext) -> StageResult:
        """
        Load data into Snowflake and update watermarks.

        Flow:
          1. Get validated DataFrame from context
          2. Open Snowflake connection (context manager = auto-close)
          3. MERGE data into fact table
          4. Update watermark per symbol to the max trade_date loaded
          5. Return result with row counts
        """
        df = ctx.data.get("validated_prices")

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            return StageResult(
                success=True,
                message="No data to load (empty DataFrame)",
                rows_in=0,
                rows_out=0,
            )

        rows_in = len(df)
        logger.info(f"[load] Loading {rows_in} rows into {self._table_name}")

        try:
            with SnowflakeConnector(self._settings) as connector:
                loader = SnowflakeLoader(connector)
                result = loader.load_stock_prices(df, table_name=self._table_name)

            if not result.success:
                return StageResult(
                    success=False,
                    message="Snowflake load failed",
                    rows_in=rows_in,
                    rows_out=0,
                    error=result.error_message or "Unknown error",
                )

            # Update watermarks AFTER successful load
            self._update_watermarks(df)

            return StageResult(
                success=True,
                message=f"Loaded into {self._table_name}",
                rows_in=rows_in,
                rows_out=result.rows_inserted,
            )

        except Exception as exc:
            return StageResult(
                success=False,
                message="Snowflake connection/load error",
                rows_in=rows_in,
                rows_out=0,
                error=str(exc),
            )

    def _update_watermarks(self, df: pd.DataFrame) -> None:
        """
        Update watermark per symbol to the max trade_date loaded.

        WHY PER-SYMBOL WATERMARKS?
          Different symbols may have different data availability.
          TCS might have data through today. RELIANCE might lag by a day.
          Per-symbol watermarks ensure each symbol gets exactly the right
          incremental window next time.
        """
        if "symbol" not in df.columns or "trade_date" not in df.columns:
            return

        for symbol in df["symbol"].unique():
            symbol_df = df[df["symbol"] == symbol]
            max_date = symbol_df["trade_date"].max()

            # Ensure max_date is a date object
            if isinstance(max_date, str):
                max_date = date.fromisoformat(max_date)
            elif hasattr(max_date, "date"):
                max_date = max_date.date()

            watermark_key = f"extract.{symbol}.NS"
            self._watermarks.set(watermark_key, max_date.isoformat())
            logger.debug(f"[load] Watermark updated: {watermark_key} = {max_date}")
