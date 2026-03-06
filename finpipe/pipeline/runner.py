# ==============================================================================
# pipeline/runner.py — Pipeline Orchestrator
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Orchestration — running stages in order with shared context
#   2. Retry logic   — retry failed stages with exponential backoff
#   3. Metrics       — track timing and row counts per stage
#   4. Fail-fast     — abort pipeline on critical failure
#   5. Structured logging — every log has run_id for tracing
#
# HOW THIS COMPARES TO PRODUCTION TOOLS:
#   - Airflow:         DagRun = PipelineRunner, TaskInstance = PipelineStage
#   - Databricks Jobs: Job Run = PipelineRunner, Task = PipelineStage
#   - Prefect:         Flow = PipelineRunner, Task = PipelineStage
#   - Luigi:           Worker = PipelineRunner, Task = PipelineStage
#
# PRODUCTION BEST PRACTICES IMPLEMENTED:
#   1. Correlation ID (run_id) — every log line traces back to one run
#   2. Stage isolation — one stage's failure doesn't corrupt another's state
#   3. Configurable retries — a transient API failure ≠ a permanent logic bug
#   4. Execution summary — know EXACTLY what happened, when, and how fast
#   5. Hooks (on_success, on_failure) — extensible without modifying runner
#
# ==============================================================================

import logging
import time
from typing import Callable

from finpipe.pipeline.context import PipelineContext
from finpipe.pipeline.stage import PipelineStage, StageResult
from finpipe.utils.retry import retry_with_backoff

logger = logging.getLogger(__name__)


class PipelineRunner:
    """
    Executes a pipeline: a sequence of stages with shared context.

    DESIGN PHILOSOPHY:
      The runner is DUMB about what each stage does. It only knows:
        1. Run each stage in order
        2. Pass the shared context between stages
        3. Retry failed stages (configurable)
        4. Stop if a stage fails after all retries
        5. Report what happened

    This separation means:
      - Stages are reusable (extract stage works in any pipeline)
      - Runner is reusable (any stages can be plugged in)
      - Testing is easy (mock the runner or mock the stages)
    """

    def __init__(
        self,
        stages: list[PipelineStage],
        max_retries: int = 2,
        retry_delay: float = 5.0,
        on_success: Callable[[PipelineContext], None] | None = None,
        on_failure: Callable[[PipelineContext, str], None] | None = None,
    ):
        self.stages = stages
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.on_success = on_success
        self.on_failure = on_failure

    def run(self, ctx: PipelineContext | None = None) -> PipelineContext:
        """
        Execute all stages in sequence.

        Returns the PipelineContext with metrics and final state.
        Raises RuntimeError if any stage fails after retries.

        HOW THE FLOW WORKS:
          1. Create context (or use provided one)
          2. For each stage:
             a. Log start with run_id
             b. Start timer
             c. Call stage.execute(ctx)
             d. If it fails → retry up to max_retries
             e. Record metrics (timing, rows, status)
             f. If all retries exhausted → abort pipeline
          3. Print summary report
          4. Call on_success/on_failure hook
        """
        if ctx is None:
            ctx = PipelineContext()

        run_id = ctx.run_id
        total_stages = len(self.stages)
        logger.info(f"Pipeline started | run_id={run_id} | stages={total_stages}")

        for idx, stage in enumerate(self.stages, 1):
            stage_label = f"[{idx}/{total_stages}] {stage.name}"
            logger.info(f"{stage_label} — starting | run_id={run_id}")

            result = self._execute_with_retry(stage, ctx, stage_label, run_id)

            # Record metrics regardless of success/failure
            ctx.end_stage(
                stage_name=stage.name,
                status="success" if result.success else "failed",
                rows_in=result.rows_in,
                rows_out=result.rows_out,
            )

            if result.success:
                logger.info(f"{stage_label} — {result.summary} | run_id={run_id}")
            else:
                error_msg = (
                    f"Pipeline aborted at {stage_label}: {result.error or result.message}"
                )
                logger.error(f"{error_msg} | run_id={run_id}")

                # Fire failure hook
                if self.on_failure:
                    self.on_failure(ctx, error_msg)

                print("\n" + ctx.summary_report())
                raise RuntimeError(error_msg)

        logger.info(f"Pipeline completed successfully | run_id={run_id}")

        # Fire success hook
        if self.on_success:
            self.on_success(ctx)

        print("\n" + ctx.summary_report())
        return ctx

    def _execute_with_retry(
        self,
        stage: PipelineStage,
        ctx: PipelineContext,
        stage_label: str,
        run_id: str,
    ) -> StageResult:
        """
        Run a stage with retries. Returns the last StageResult.

        RETRY STRATEGY:
          - Attempt 1: run immediately
          - Attempt 2: wait retry_delay seconds, then run
          - Attempt 3: wait retry_delay * 2 seconds, then run
          ...

        This is LINEAR backoff (not exponential) for stages. Why?
          - Stage retries involve database writes, API calls with their own retries
          - We don't want to wait 64 seconds between pipeline stage retries
          - The retry_with_backoff decorator handles exponential backoff at
            the individual API/DB call level inside each stage

        PRODUCTION NOTE:
          In Airflow, this is `retries=2, retry_delay=timedelta(seconds=5)`
          Same concept, different syntax.
        """
        last_result = StageResult(success=False, error="Never executed")

        for attempt in range(1, self.max_retries + 1):
            try:
                ctx.start_stage(stage.name)
                last_result = stage.execute(ctx)

                if last_result.success:
                    return last_result

                # Stage returned failure (not an exception)
                logger.warning(
                    f"{stage_label} — attempt {attempt}/{self.max_retries} failed: "
                    f"{last_result.error or last_result.message} | run_id={run_id}"
                )

            except Exception as exc:
                last_result = StageResult(
                    success=False,
                    message=f"Exception in {stage.name}",
                    error=str(exc),
                )
                logger.warning(
                    f"{stage_label} — attempt {attempt}/{self.max_retries} "
                    f"raised {type(exc).__name__}: {exc} | run_id={run_id}"
                )

            # Wait before retry (except after last attempt)
            if attempt < self.max_retries:
                delay = self.retry_delay * attempt
                logger.info(
                    f"{stage_label} — retrying in {delay:.1f}s | run_id={run_id}"
                )
                time.sleep(delay)

        return last_result
