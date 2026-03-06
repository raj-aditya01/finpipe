# ==============================================================================
# utils/watermark.py — Incremental Load Tracking (High-Water Mark Pattern)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. High-water mark — the "bookmark" that tells you where you left off
#   2. Incremental vs full load — why loading EVERYTHING every time is wasteful
#   3. Idempotent state — safe to re-run, safe to crash mid-pipeline
#   4. File-based state — simple, portable, no external dependencies
#
# THE PROBLEM:
#   Your pipeline fetches stock data daily. Without a watermark:
#     Day 1: fetch 1 month of data → 500 rows
#     Day 2: fetch 1 month of data → 500 rows (490 are DUPLICATES)
#     Day 30: fetch 1 month of data → 500 rows (still mostly duplicates)
#     → You're transferring 15,000 rows/month but only 500 are new
#
#   With a watermark:
#     Day 1: fetch from 2020-01-01 (first run) → 500 rows, save watermark=2026-03-05
#     Day 2: fetch from 2026-03-05 → 10 rows (only NEW data), save watermark=2026-03-06
#     Day 30: fetch from 2026-03-06 → 10 rows each day
#     → You transfer 800 rows/month — 19x less data
#
# HOW IT WORKS:
#   1. Before fetching: read watermark file → get last loaded date
#   2. Fetch only data AFTER that date
#   3. After successful load: update watermark file with new date
#   4. If pipeline crashes: watermark is NOT updated → safe to re-run
#
# PRODUCTION ALTERNATIVES:
#   - Snowflake STREAM/TASK (built-in change tracking)
#   - Airflow XCom (key-value store between tasks)
#   - Redis/DynamoDB (distributed state for multi-worker pipelines)
#   - Delta Lake transaction log (Databricks uses this internally)
#
# We use a JSON file here — simple, zero dependencies, works everywhere.
# In production, you'd use Snowflake metadata or a database table.
#
# ==============================================================================

import json
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Default directory for watermark files
DEFAULT_WATERMARK_DIR = Path(__file__).parent.parent.parent / ".watermarks"


class WatermarkStore:
    """
    Persistent key-value store for pipeline watermarks (bookmarks).

    OOP CONCEPTS:
      - Encapsulation: file I/O details hidden behind get/set methods
      - Single Responsibility: only manages watermark state
      - Defensive programming: handles missing files, corrupt JSON

    PRODUCTION PATTERN:
      This is a simplified version of what Airflow calls "XCom" or what
      Databricks Delta Live Tables calls "flow state." The concept is
      the same: track how far each pipeline stage has progressed.

    Usage:
        store = WatermarkStore()

        # Get the last processed date (None if first run)
        last_date = store.get("stock_prices_last_date")

        # After successful load, save the new watermark
        store.set("stock_prices_last_date", "2026-03-06")
    """

    def __init__(self, directory: Path | str = DEFAULT_WATERMARK_DIR):
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._file = self._dir / "watermarks.json"
        self._data: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        """Load watermarks from disk. Returns empty dict if file missing/corrupt."""
        if not self._file.exists():
            return {}
        try:
            return json.loads(self._file.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"[watermark] Failed to read {self._file}: {e}. Starting fresh.")
            return {}

    def _save(self) -> None:
        """Persist watermarks to disk atomically."""
        # Write to temp file first, then rename — prevents corrupt file on crash
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, default=str), encoding="utf-8")
        tmp.replace(self._file)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a watermark value. Returns default if not set."""
        return self._data.get(key, default)

    def get_date(self, key: str, default: date | None = None) -> date | None:
        """Get a watermark as a date object."""
        val = self._data.get(key)
        if val is None:
            return default
        if isinstance(val, date):
            return val
        try:
            return date.fromisoformat(str(val))
        except ValueError:
            return default

    def set(self, key: str, value: Any) -> None:
        """Set a watermark and persist immediately."""
        self._data[key] = value
        self._save()
        logger.info(f"[watermark] Set {key} = {value}")

    def delete(self, key: str) -> None:
        """Remove a watermark."""
        self._data.pop(key, None)
        self._save()

    def all(self) -> dict[str, Any]:
        """Return all watermarks (read-only copy)."""
        return dict(self._data)

    def reset(self) -> None:
        """Clear ALL watermarks. Use for full re-processing."""
        self._data = {}
        self._save()
        logger.warning("[watermark] All watermarks cleared — next run will be a full load.")
