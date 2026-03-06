# ==============================================================================
# pipeline/stage.py — Abstract Pipeline Stage (the building block)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Pipeline as a sequence of stages — each stage is a pluggable unit
#   2. Abstract Stage pattern — all stages have the same interface
#   3. Single Responsibility — each stage does ONE thing well
#   4. Composability — stages can be rearranged, skipped, or retried independently
#
# HOW PRODUCTION PIPELINES ARE STRUCTURED:
#   Every serious data tool uses the same mental model:
#
#   Airflow:        Task1 >> Task2 >> Task3
#   Databricks DLT: @dlt.table → @dlt.table → @dlt.table
#   Spark:          read → transform → write
#   dbt:            model → model → model (DAG of SQL transforms)
#
#   The common pattern: a PIPELINE is a sequence of STAGES.
#   Each stage:
#     1. Reads input (from context, API, or database)
#     2. Does its work (fetch, validate, transform, load)
#     3. Writes output (back into context for the next stage)
#     4. Reports success/failure
#
# OOP CONCEPTS:
#   - Abstract Base Class: PipelineStage defines the contract
#   - Template Method: run() is the template, execute() is the detail
#   - Strategy Pattern: swap stages without changing the pipeline
#
# ==============================================================================

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from finpipe.pipeline.context import PipelineContext

logger = logging.getLogger(__name__)


@dataclass
class StageResult:
    """
    Outcome of running a pipeline stage.

    DESIGN: Separates "what happened" from the stage logic itself.
    The runner inspects StageResult to decide: continue, retry, or abort.
    """
    success: bool
    message: str = ""
    rows_in: int = 0
    rows_out: int = 0
    error: str = ""
    metadata: dict[str, Any] | None = None

    @property
    def summary(self) -> str:
        status = "OK" if self.success else "FAILED"
        parts = [f"[{status}] {self.message}"]
        if self.rows_out:
            parts.append(f"({self.rows_out} rows)")
        if self.error:
            parts.append(f"Error: {self.error}")
        return " ".join(parts)


class PipelineStage(ABC):
    """
    Abstract base class for all pipeline stages.

    Every stage must implement execute(ctx) and return a StageResult.
    The PipelineRunner calls stages in order, passing the shared context.

    WHY ABC (not just regular classes)?
      ABC enforces the contract — if you create a stage without execute(),
      Python raises TypeError at import time. This is "fail fast" — catch
      bugs at startup, not at 3 AM when the pipeline runs.

    OOP CONCEPT: Template Method Pattern
      PipelineStage.run() is the TEMPLATE — it handles logging + metrics.
      Subclasses implement execute() — the specific logic.
      This separates cross-cutting concerns (logging/metrics) from business logic.
    """

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description

    @abstractmethod
    def execute(self, ctx: PipelineContext) -> StageResult:
        """
        Run the stage's logic. Subclasses MUST implement this.

        Args:
            ctx: Pipeline context with shared data, params, and state.

        Returns:
            StageResult indicating success/failure with details.

        RULES FOR STAGE IMPLEMENTORS:
          1. Read input from ctx.get_dataframe("key") or ctx.params
          2. Do your work (fetch, validate, transform, load)
          3. Write output to ctx.set_dataframe("key", df)
          4. Return StageResult(success=True/False, ...)
          5. Do NOT catch exceptions you can't handle — let them propagate
             to the runner, which implements retry logic.
        """
        ...
