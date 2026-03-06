# ==============================================================================
# pipeline/stages/validate.py — Data Quality Validation Stage
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Quality gates — halt the pipeline if data is bad
#   2. WARN vs CRITICAL — some issues are OK, some are fatal
#   3. Data contracts — codify what "good data" looks like
#   4. Shift-left testing — catch issues BEFORE loading into the database
#
# WHY VALIDATE BEFORE LOADING?
#   ┌──────────────────────────────────────────────────────────────────────┐
#   │ Without validation:                                                  │
#   │   Extract → Load → Users see bad data → Fire drill to fix → Hours  │
#   │                                                                      │
#   │ With validation:                                                     │
#   │   Extract → Validate → [HALT if bad] → Load (only clean data)      │
#   │                                                                      │
#   │ Production tools that do this:                                       │
#   │   - Great Expectations (Python library)                              │
#   │   - dbt tests (SQL-based assertions)                                 │
#   │   - Databricks DLT expectations (@dlt.expect)                        │
#   │   - Monte Carlo / Soda (data observability platforms)                │
#   └──────────────────────────────────────────────────────────────────────┘
#
# HOW THIS STAGE WORKS:
#   1. Reads ctx.data["raw_prices"] (from ExtractStage)
#   2. Runs a QualitySuite of checks against it
#   3. If all CRITICAL checks pass → success, pipeline continues
#   4. If any CRITICAL check fails → failure, pipeline halts
#   5. WARN checks are logged but don't block the pipeline
#
# ==============================================================================

import logging

import pandas as pd

from finpipe.pipeline.stage import PipelineStage, StageResult
from finpipe.pipeline.context import PipelineContext
from finpipe.quality.checks import (
    QualitySuite,
    QualityResult,
    not_null,
    unique,
    in_range,
    row_count_between,
    schema_match,
    custom_check,
    Severity,
)

logger = logging.getLogger(__name__)


class ValidateStage(PipelineStage):
    """
    Runs data quality checks on the extracted DataFrame.

    The quality suite is configurable — you can add custom checks
    for specific business rules.

    DATA QUALITY DIMENSIONS (industry standard):
      1. Completeness — Are all required fields present? (not_null)
      2. Uniqueness   — Are there duplicates? (unique)
      3. Validity     — Are values in expected ranges? (in_range)
      4. Freshness    — Is the data recent enough? (freshness)
      5. Schema       — Does the structure match expectations? (schema_match)
      6. Volume       — Is the row count reasonable? (row_count_between)

    OOP CONCEPTS:
      - Composition: uses QualitySuite (has-a, not is-a)
      - Open/Closed Principle: add new checks without modifying this class
    """

    def __init__(self, min_rows: int = 1, max_rows: int = 100_000):
        super().__init__(
            name="validate",
            description="Run data quality checks on extracted data",
        )
        self.min_rows = min_rows
        self.max_rows = max_rows

    def execute(self, ctx: PipelineContext) -> StageResult:
        """
        Validate ctx.data["raw_prices"].

        Flow:
          1. Build quality suite with standard stock data checks
          2. Run all checks
          3. Log results (pass/fail/warn)
          4. Return success only if all CRITICAL checks pass
        """
        df = ctx.data.get("raw_prices")

        if df is None or (isinstance(df, pd.DataFrame) and df.empty):
            return StageResult(
                success=True,
                message="No data to validate (empty DataFrame)",
                rows_in=0,
                rows_out=0,
            )

        rows_in = len(df)

        # Build quality suite — these are the DATA CONTRACTS for stock data
        suite = self._build_suite()
        result = suite.run(df)

        # Log each check result
        for check_result in result.results:
            level = logging.WARNING if not check_result.passed else logging.DEBUG
            logger.log(
                level,
                f"[validate] {check_result.check_name}: "
                f"{'PASS' if check_result.passed else 'FAIL'} "
                f"({check_result.severity}) — {check_result.message}",
            )

        # Store validation report in context for downstream use
        ctx.state["validation_summary"] = result.summary
        ctx.state["validation_passed"] = result.critical_passed

        # Pass validated data through (unchanged)
        ctx.data["validated_prices"] = df

        if not result.critical_passed:
            failed = [
                r.check_name for r in result.results
                if not r.passed and r.severity == Severity.CRITICAL
            ]
            return StageResult(
                success=False,
                message="Data quality checks failed",
                rows_in=rows_in,
                rows_out=0,
                error=f"Critical failures: {', '.join(failed)}",
            )

        passed = sum(1 for r in result.results if r.passed)
        total = len(result.results)
        return StageResult(
            success=True,
            message=f"Quality checks passed ({passed}/{total})",
            rows_in=rows_in,
            rows_out=rows_in,
        )

    def _build_suite(self) -> QualitySuite:
        """
        Build the standard quality suite for stock price data.

        WHY BUILD IT HERE (not in __init__)?
          So it can be re-created fresh each run with current parameters.
          In the future, you could make this configurable via YAML/JSON.

        EACH CHECK IS A DATA CONTRACT:
          not_null("symbol")         → Every row MUST have a symbol
          not_null("trade_date")     → Every row MUST have a date
          in_range("open_price", …)  → Prices must be non-negative
          row_count_between(…)       → Catch empty/huge extractions
          unique(["symbol", …])      → No duplicate rows per (symbol, date)
        """
        suite = QualitySuite("stock_prices_validation")

        # Critical checks — pipeline HALTS if these fail
        suite.add(not_null("symbol", severity=Severity.CRITICAL))
        suite.add(not_null("trade_date", severity=Severity.CRITICAL))
        suite.add(not_null("close_price", severity=Severity.CRITICAL))
        suite.add(in_range("open_price", min_val=0, severity=Severity.CRITICAL))
        suite.add(in_range("high_price", min_val=0, severity=Severity.CRITICAL))
        suite.add(in_range("low_price", min_val=0, severity=Severity.CRITICAL))
        suite.add(in_range("close_price", min_val=0, severity=Severity.CRITICAL))
        suite.add(in_range("volume", min_val=0, severity=Severity.CRITICAL))
        suite.add(
            row_count_between(
                min_rows=self.min_rows,
                max_rows=self.max_rows,
                severity=Severity.CRITICAL,
            )
        )

        # Warning checks — logged but pipeline continues
        suite.add(
            custom_check(
                name="unique(symbol+exchange+date)",
                check_fn=lambda df: (
                    not df.duplicated(subset=["symbol", "exchange_code", "trade_date"]).any(),
                    f"{df.duplicated(subset=['symbol', 'exchange_code', 'trade_date']).sum()} duplicates",
                    {},
                ),
                severity=Severity.WARN,
            )
        )
        suite.add(
            schema_match(
                expected_columns=[
                    "symbol", "exchange_code", "trade_date",
                    "open_price", "high_price", "low_price", "close_price",
                    "volume",
                ],
                severity=Severity.WARN,
            )
        )

        return suite
