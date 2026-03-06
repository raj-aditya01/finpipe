# ==============================================================================
# pipeline/context.py — Pipeline Execution Context
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Execution context — shared state that flows through all pipeline stages
#   2. Correlation ID — trace a single pipeline run across all log lines
#   3. Metrics collection — track performance (rows/sec, duration, errors)
#   4. Immutable config + mutable state — safe separation of concerns
#
# WHY A CONTEXT OBJECT?
#   Without context, each stage must pass data via arguments:
#     df = extract(symbols, period)
#     df = validate(df, checks)
#     load(df, connector, table)
#     → What if you need to track timing? Add a dict. Track errors? Another dict.
#       Log a run ID? Pass it everywhere. It becomes a mess of arguments.
#
#   With context:
#     ctx = PipelineContext(run_id="abc123")
#     extract_stage.run(ctx)   # ctx carries everything: config, data, metrics
#     validate_stage.run(ctx)  # stages read/write data via ctx
#     load_stage.run(ctx)      # metrics tracked automatically
#
# PRODUCTION EQUIVALENTS:
#   - Airflow: TaskInstance context (ti.xcom_push / ti.xcom_pull)
#   - Spark: SparkSession (shared context for all operations)
#   - Databricks: dbutils + notebook context
#   - Prefect: FlowRunContext
#
# ==============================================================================

import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class StageMetric:
    """Performance metric for a single pipeline stage."""
    stage_name: str
    status: str = "pending"          # pending, running, success, failed, skipped
    start_time: float = 0.0
    end_time: float = 0.0
    rows_in: int = 0
    rows_out: int = 0
    error: str = ""

    @property
    def duration_seconds(self) -> float:
        if self.end_time and self.start_time:
            return round(self.end_time - self.start_time, 2)
        return 0.0

    @property
    def rows_per_second(self) -> float:
        d = self.duration_seconds
        if d > 0 and self.rows_out > 0:
            return round(self.rows_out / d, 1)
        return 0.0


class PipelineContext:
    """
    Shared execution context that flows through the entire pipeline.

    DESIGN DECISIONS:
      1. run_id: UUID for correlating all log lines from one pipeline execution.
         In production, this maps to Airflow's run_id or Databricks' job_run_id.

      2. data: Dict of DataFrames passed between stages. The extract stage writes
         ctx.data["raw"], the validate stage reads/writes ctx.data["validated"], etc.

      3. metrics: Automatically track timing and row counts per stage.

      4. params: Runtime parameters (symbols, period, etc.) — immutable config.

    THREAD SAFETY NOTE:
      This context is NOT thread-safe. Each pipeline run gets its own context.
      For parallel pipelines, create separate contexts.
    """

    def __init__(self, params: dict[str, Any] | None = None):
        # Unique identifier for this pipeline run
        self.run_id: str = uuid.uuid4().hex[:12]
        self.started_at: datetime = datetime.utcnow()

        # Runtime parameters (immutable after creation)
        self.params: dict[str, Any] = params or {}

        # Data passed between stages (mutable)
        self.data: dict[str, pd.DataFrame] = {}

        # Stage metrics (auto-tracked by PipelineRunner)
        self.metrics: dict[str, StageMetric] = {}

        # Key-value store for arbitrary state between stages
        self.state: dict[str, Any] = {}

        logger.info(f"[pipeline:{self.run_id}] Context created with params: {list(self.params.keys())}")

    # ─── Data helpers ─────────────────────────────────────────────────

    def set_dataframe(self, key: str, df: pd.DataFrame) -> None:
        """Store a DataFrame in the context (for the next stage to read)."""
        self.data[key] = df
        logger.debug(f"[pipeline:{self.run_id}] Stored DataFrame '{key}': {len(df)} rows")

    def get_dataframe(self, key: str) -> pd.DataFrame:
        """Retrieve a DataFrame. Raises KeyError if not found."""
        if key not in self.data:
            available = list(self.data.keys())
            raise KeyError(
                f"DataFrame '{key}' not found in context. Available: {available}"
            )
        return self.data[key]

    # ─── Metrics helpers ──────────────────────────────────────────────

    def start_stage(self, stage_name: str) -> StageMetric:
        """Mark a stage as started (called by PipelineRunner)."""
        metric = StageMetric(stage_name=stage_name, status="running", start_time=time.time())
        self.metrics[stage_name] = metric
        return metric

    def end_stage(self, stage_name: str, status: str, rows_in: int = 0, rows_out: int = 0, error: str = "") -> None:
        """Mark a stage as completed (called by PipelineRunner)."""
        metric = self.metrics.get(stage_name)
        if metric:
            metric.status = status
            metric.end_time = time.time()
            metric.rows_in = rows_in
            metric.rows_out = rows_out
            metric.error = error

    # ─── Report ───────────────────────────────────────────────────────

    def summary_report(self) -> str:
        """Generate a human-readable pipeline execution report."""
        lines = [
            f"Pipeline Run: {self.run_id}",
            f"Started: {self.started_at.isoformat()}",
            f"Params: {self.params}",
            "",
            f"{'Stage':<20} {'Status':<10} {'Duration':>10} {'Rows In':>10} {'Rows Out':>10} {'Rows/s':>10}",
            "-" * 75,
        ]
        total_duration = 0.0
        for name, m in self.metrics.items():
            total_duration += m.duration_seconds
            lines.append(
                f"{name:<20} {m.status:<10} {m.duration_seconds:>9.1f}s {m.rows_in:>10,} {m.rows_out:>10,} {m.rows_per_second:>10.0f}"
            )
            if m.error:
                lines.append(f"  ERROR: {m.error}")

        lines.append("-" * 75)
        lines.append(f"{'TOTAL':<20} {'':10} {total_duration:>9.1f}s")
        return "\n".join(lines)
