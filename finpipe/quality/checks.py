# ==============================================================================
# quality/checks.py — Data Quality Check Framework
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Data quality gates — validate data BETWEEN pipeline stages
#   2. Check types — null, unique, range, freshness, schema, row count
#   3. Severity levels — WARN (log and continue) vs CRITICAL (stop pipeline)
#   4. Quality suite — run multiple checks and get a pass/fail report
#
# WHY DATA QUALITY CHECKS?
#   Without checks, bad data flows silently into your warehouse:
#     - NULL prices → dashboards show blanks
#     - Duplicate rows → reports double-count revenue
#     - Stale data → users make decisions on yesterday's numbers
#     - Wrong schema → joins fail, downstream pipelines break
#
#   Production pipelines add quality gates BETWEEN each stage:
#     Extract → [CHECK: not empty] → Transform → [CHECK: no nulls] → Load
#
#   If a check fails with severity=CRITICAL, the pipeline STOPS before
#   bad data reaches the warehouse. This is called "shift left" — catch
#   problems EARLY, not after they've corrupted your analytics.
#
# PRODUCTION EXAMPLES:
#   - dbt tests: not_null, unique, accepted_values, relationships
#   - Great Expectations: expect_column_values_to_not_be_null
#   - Databricks DLT: @expect("valid price", "close_price > 0")
#   - Soda Core: checks for each table in YAML
#
# OUR APPROACH:
#   We build a lightweight framework that mirrors these tools' concepts.
#   Each check is a function that takes a DataFrame and returns pass/fail.
#   A QualitySuite groups checks and runs them all, producing a report.
#
# ==============================================================================

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from enum import StrEnum
from typing import Callable

import pandas as pd

logger = logging.getLogger(__name__)


# ==============================================================================
# Data Types
# ==============================================================================

class Severity(StrEnum):
    """
    How bad is a quality failure?

    WARN:     Log the problem, but let the pipeline continue.
              Use for: minor issues, known data quirks, metrics you want to track.

    CRITICAL: Stop the pipeline immediately. Bad data must NOT reach the warehouse.
              Use for: null primary keys, duplicates, schema mismatches, stale data.

    PRODUCTION RULE OF THUMB:
      - Anything that would corrupt downstream reporting → CRITICAL
      - Anything that's ugly but survivable → WARN
    """
    WARN = "WARN"
    CRITICAL = "CRITICAL"


@dataclass
class QualityResult:
    """Result of a single quality check."""
    check_name: str
    passed: bool
    severity: Severity
    message: str
    details: dict = field(default_factory=dict)

    @property
    def icon(self) -> str:
        if self.passed:
            return "PASS"
        return "FAIL" if self.severity == Severity.CRITICAL else "WARN"

    @property
    def summary(self) -> str:
        return f"[{self.icon}] {self.check_name}: {self.message}"


@dataclass
class QualityCheck:
    """
    A single data quality check.

    This is a VALUE OBJECT — it describes WHAT to check, not HOW.
    The `run` method takes a DataFrame and returns a QualityResult.

    OOP CONCEPT: Strategy Pattern
      Each check is a strategy — a pluggable algorithm.
      The QualitySuite doesn't know what checks do internally.
      It just calls check.run(df) and collects results.
    """
    name: str
    description: str
    severity: Severity
    check_fn: Callable[[pd.DataFrame], tuple[bool, str, dict]]

    def run(self, df: pd.DataFrame) -> QualityResult:
        """Execute the check against a DataFrame."""
        try:
            passed, message, details = self.check_fn(df)
            return QualityResult(
                check_name=self.name,
                passed=passed,
                severity=self.severity,
                message=message,
                details=details,
            )
        except Exception as e:
            return QualityResult(
                check_name=self.name,
                passed=False,
                severity=self.severity,
                message=f"Check raised exception: {e}",
                details={"error": str(e)},
            )


# ==============================================================================
# Quality Suite — Run Multiple Checks
# ==============================================================================

class QualitySuite:
    """
    Collection of quality checks that run together.

    PRODUCTION PATTERN:
      In dbt, this is your `schema.yml` tests block.
      In Great Expectations, this is an ExpectationSuite.
      In Databricks DLT, this is the set of @expect decorators on a table.

    Usage:
        suite = QualitySuite("stock_prices")
        suite.add(not_null("close_price"))
        suite.add(in_range("close_price", min_val=0))
        suite.add(row_count_between(min_rows=1))

        results = suite.run(df)
        if not results.all_passed:
            raise ValueError(f"Quality checks failed: {results.summary}")
    """

    def __init__(self, name: str):
        self.name = name
        self._checks: list[QualityCheck] = []

    def add(self, check: QualityCheck) -> "QualitySuite":
        """Add a check. Returns self for chaining: suite.add(a).add(b)"""
        self._checks.append(check)
        return self

    def run(self, df: pd.DataFrame) -> "SuiteResult":
        """Run all checks against the DataFrame."""
        results = [check.run(df) for check in self._checks]

        for r in results:
            if r.passed:
                logger.info(f"  {r.summary}")
            elif r.severity == Severity.WARN:
                logger.warning(f"  {r.summary}")
            else:
                logger.error(f"  {r.summary}")

        return SuiteResult(suite_name=self.name, results=results)


@dataclass
class SuiteResult:
    """Aggregated results from running a QualitySuite."""
    suite_name: str
    results: list[QualityResult]

    @property
    def all_passed(self) -> bool:
        """True if every check passed."""
        return all(r.passed for r in self.results)

    @property
    def critical_passed(self) -> bool:
        """True if all CRITICAL checks passed (WARNs are OK)."""
        return all(r.passed for r in self.results if r.severity == Severity.CRITICAL)

    @property
    def failures(self) -> list[QualityResult]:
        return [r for r in self.results if not r.passed]

    @property
    def critical_failures(self) -> list[QualityResult]:
        return [r for r in self.results if not r.passed and r.severity == Severity.CRITICAL]

    @property
    def summary(self) -> str:
        total = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        failed = total - passed
        crit = len(self.critical_failures)
        status = "PASSED" if self.critical_passed else "FAILED"
        return (
            f"Quality Suite '{self.suite_name}': {status} "
            f"({passed}/{total} passed, {failed} failed, {crit} critical)"
        )


# ==============================================================================
# Built-in Check Factories
# ==============================================================================
# These are FACTORY FUNCTIONS — they create QualityCheck objects.
# Each one returns a QualityCheck configured for a specific validation.
#
# PATTERN: Factory functions let you write:
#   suite.add(not_null("close_price"))
# Instead of:
#   suite.add(QualityCheck(name="...", description="...", severity=..., check_fn=...))
# ==============================================================================


def not_null(
    column: str,
    severity: Severity = Severity.CRITICAL,
) -> QualityCheck:
    """
    Check that a column has no NULL/NaN values.

    WHY: NULL primary keys break joins. NULL prices break calculations.
    dbt equivalent: tests: [not_null]
    """
    def check(df: pd.DataFrame) -> tuple[bool, str, dict]:
        if column not in df.columns:
            return False, f"Column '{column}' not found in DataFrame", {}
        null_count = int(df[column].isna().sum())
        total = len(df)
        if null_count == 0:
            return True, f"{column}: 0 nulls in {total} rows", {}
        pct = round(null_count / total * 100, 1)
        return False, f"{column}: {null_count} nulls ({pct}%) in {total} rows", {
            "null_count": null_count, "total": total, "null_pct": pct,
        }

    return QualityCheck(
        name=f"not_null({column})",
        description=f"Column '{column}' must not contain NULL values",
        severity=severity,
        check_fn=check,
    )


def unique(
    column: str,
    severity: Severity = Severity.CRITICAL,
) -> QualityCheck:
    """
    Check that a column has no duplicate values.

    WHY: Duplicate IDs mean double-counted metrics.
    dbt equivalent: tests: [unique]
    """
    def check(df: pd.DataFrame) -> tuple[bool, str, dict]:
        if column not in df.columns:
            return False, f"Column '{column}' not found", {}
        dup_count = int(df[column].duplicated().sum())
        if dup_count == 0:
            return True, f"{column}: all {len(df)} values unique", {}
        return False, f"{column}: {dup_count} duplicate values", {
            "duplicate_count": dup_count,
        }

    return QualityCheck(
        name=f"unique({column})",
        description=f"Column '{column}' must have unique values",
        severity=severity,
        check_fn=check,
    )


def in_range(
    column: str,
    min_val: float | None = None,
    max_val: float | None = None,
    severity: Severity = Severity.CRITICAL,
) -> QualityCheck:
    """
    Check that all values in a numeric column fall within [min_val, max_val].

    WHY: Stock prices should never be negative. Volume should never be < 0.
    Great Expectations equivalent: expect_column_values_to_be_between
    """
    def check(df: pd.DataFrame) -> tuple[bool, str, dict]:
        if column not in df.columns:
            return False, f"Column '{column}' not found", {}
        series = df[column].dropna()
        violations = 0
        if min_val is not None:
            violations += int((series < min_val).sum())
        if max_val is not None:
            violations += int((series > max_val).sum())
        if violations == 0:
            bounds = f"[{min_val}, {max_val}]"
            return True, f"{column}: all {len(series)} values in range {bounds}", {}
        return False, f"{column}: {violations} values out of range [{min_val}, {max_val}]", {
            "violation_count": violations,
        }

    return QualityCheck(
        name=f"in_range({column}, {min_val}-{max_val})",
        description=f"Column '{column}' values must be between {min_val} and {max_val}",
        severity=severity,
        check_fn=check,
    )


def row_count_between(
    min_rows: int = 1,
    max_rows: int | None = None,
    severity: Severity = Severity.CRITICAL,
) -> QualityCheck:
    """
    Check that the DataFrame has a reasonable number of rows.

    WHY: An empty DataFrame means the API returned nothing — something is wrong.
         A DataFrame with 10 million rows when you expected 1000 means a bug.
    """
    def check(df: pd.DataFrame) -> tuple[bool, str, dict]:
        count = len(df)
        if count < min_rows:
            return False, f"Row count {count} < minimum {min_rows}", {"row_count": count}
        if max_rows is not None and count > max_rows:
            return False, f"Row count {count} > maximum {max_rows}", {"row_count": count}
        return True, f"Row count {count} within [{min_rows}, {max_rows or 'inf'}]", {
            "row_count": count,
        }

    return QualityCheck(
        name=f"row_count({min_rows}-{max_rows or 'inf'})",
        description=f"DataFrame must have between {min_rows} and {max_rows or 'unlimited'} rows",
        severity=severity,
        check_fn=check,
    )


def freshness(
    date_column: str,
    max_age_days: int = 3,
    severity: Severity = Severity.WARN,
) -> QualityCheck:
    """
    Check that the most recent data is not too old.

    WHY: If your pipeline runs daily but the latest data is from 5 days ago,
         something is wrong — maybe the API is down or symbols are delisted.

    dbt equivalent: sources.freshness.warn_after / error_after
    """
    def check(df: pd.DataFrame) -> tuple[bool, str, dict]:
        if date_column not in df.columns:
            return False, f"Column '{date_column}' not found", {}
        dates = pd.to_datetime(df[date_column], errors="coerce")
        if dates.isna().all():
            return False, f"No valid dates in '{date_column}'", {}
        max_date = dates.max().date() if hasattr(dates.max(), "date") else dates.max()
        age_days = (date.today() - max_date).days
        if age_days <= max_age_days:
            return True, f"Latest data: {max_date} ({age_days}d old, limit: {max_age_days}d)", {
                "max_date": str(max_date), "age_days": age_days,
            }
        return False, f"Data is stale: latest {max_date} ({age_days}d old, limit: {max_age_days}d)", {
            "max_date": str(max_date), "age_days": age_days,
        }

    return QualityCheck(
        name=f"freshness({date_column}, {max_age_days}d)",
        description=f"Latest date in '{date_column}' must be within {max_age_days} days",
        severity=severity,
        check_fn=check,
    )


def schema_match(
    expected_columns: list[str],
    severity: Severity = Severity.CRITICAL,
) -> QualityCheck:
    """
    Check that the DataFrame has all expected columns.

    WHY: If Yahoo Finance changes their API response format, columns may
         disappear. Catching this early prevents cryptic KeyError downstream.
    """
    def check(df: pd.DataFrame) -> tuple[bool, str, dict]:
        actual = set(df.columns)
        expected = set(expected_columns)
        missing = expected - actual
        if not missing:
            return True, f"All {len(expected)} expected columns present", {}
        return False, f"Missing columns: {sorted(missing)}", {
            "missing": sorted(missing), "actual": sorted(actual),
        }

    return QualityCheck(
        name=f"schema_match({len(expected_columns)} cols)",
        description=f"DataFrame must contain columns: {expected_columns}",
        severity=severity,
        check_fn=check,
    )


def custom_check(
    name: str,
    check_fn: Callable[[pd.DataFrame], tuple[bool, str, dict]],
    severity: Severity = Severity.CRITICAL,
) -> QualityCheck:
    """
    Create a custom quality check with any logic.

    Usage:
        suite.add(custom_check(
            "no_future_dates",
            lambda df: (
                df["trade_date"].max() <= date.today(),
                f"Max date: {df['trade_date'].max()}",
                {},
            ),
        ))
    """
    return QualityCheck(
        name=name,
        description=f"Custom check: {name}",
        severity=severity,
        check_fn=check_fn,
    )
