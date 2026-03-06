# ==============================================================================
# tests/test_pipeline.py — Tests for Pipeline Orchestration Layer
# ==============================================================================
#
# Tests cover:
#   1. PipelineContext — run_id, data passing, metrics
#   2. PipelineStage (abstract) — contract enforcement
#   3. PipelineRunner — stage execution, retries, abort on failure
#   4. ExtractStage, ValidateStage, LoadStage — concrete stages
#
# ==============================================================================

import time
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from finpipe.pipeline.context import PipelineContext, StageMetric
from finpipe.pipeline.stage import PipelineStage, StageResult
from finpipe.pipeline.runner import PipelineRunner


# ==============================================================================
# Helpers — Concrete stages for testing
# ==============================================================================

class PassingStage(PipelineStage):
    """A stage that always succeeds."""

    def execute(self, ctx):
        ctx.data["output"] = "hello"
        return StageResult(success=True, message="All good", rows_out=10)


class FailingStage(PipelineStage):
    """A stage that always fails."""

    def execute(self, ctx):
        return StageResult(success=False, message="Bad data", error="null values")


class ExplodingStage(PipelineStage):
    """A stage that throws an exception."""

    def execute(self, ctx):
        raise ValueError("Connection refused")


class EventuallyPassesStage(PipelineStage):
    """Fails first N times, then passes. Used to test retries."""

    def __init__(self, fail_count: int = 1):
        super().__init__(name="eventually_passes")
        self._fail_count = fail_count
        self._attempts = 0

    def execute(self, ctx):
        self._attempts += 1
        if self._attempts <= self._fail_count:
            return StageResult(success=False, error=f"Attempt {self._attempts} failed")
        return StageResult(success=True, message="Finally worked", rows_out=5)


# ==============================================================================
# Tests: StageResult
# ==============================================================================

class TestStageResult:

    def test_success_summary(self):
        r = StageResult(success=True, message="Loaded data", rows_out=100)
        assert "[OK]" in r.summary
        assert "100 rows" in r.summary

    def test_failure_summary(self):
        r = StageResult(success=False, message="Oops", error="null check failed")
        assert "[FAILED]" in r.summary
        assert "null check failed" in r.summary

    def test_defaults(self):
        r = StageResult(success=True)
        assert r.rows_in == 0
        assert r.rows_out == 0
        assert r.error == ""
        assert r.metadata is None


# ==============================================================================
# Tests: PipelineContext
# ==============================================================================

class TestPipelineContext:

    def test_run_id_is_unique(self):
        ctx1 = PipelineContext()
        ctx2 = PipelineContext()
        assert ctx1.run_id != ctx2.run_id

    def test_data_store(self):
        ctx = PipelineContext()
        df = pd.DataFrame({"a": [1, 2, 3]})
        ctx.data["test"] = df
        assert len(ctx.data["test"]) == 3

    def test_params_immutable(self):
        ctx = PipelineContext(params={"env": "dev"})
        assert ctx.params["env"] == "dev"

    def test_state_key_value(self):
        ctx = PipelineContext()
        ctx.state["key1"] = "value1"
        assert ctx.state["key1"] == "value1"

    def test_record_metric(self):
        ctx = PipelineContext()
        ctx.start_stage("extract")
        time.sleep(0.01)
        ctx.end_stage("extract", status="success", rows_in=0, rows_out=100)
        assert "extract" in ctx.metrics
        assert ctx.metrics["extract"].status == "success"
        assert ctx.metrics["extract"].rows_out == 100

    def test_summary_report_runs_without_error(self):
        ctx = PipelineContext()
        ctx.start_stage("test_stage")
        ctx.end_stage("test_stage", status="success", rows_in=5, rows_out=5)
        report = ctx.summary_report()
        assert "test_stage" in report
        assert "success" in report


# ==============================================================================
# Tests: PipelineStage (abstract)
# ==============================================================================

class TestPipelineStage:

    def test_cannot_instantiate_abstract(self):
        """ABC enforces execute() implementation."""
        with pytest.raises(TypeError):
            PipelineStage(name="bad")  # type: ignore

    def test_concrete_stage_works(self):
        stage = PassingStage(name="pass_test")
        ctx = PipelineContext()
        result = stage.execute(ctx)
        assert result.success
        assert ctx.data["output"] == "hello"


# ==============================================================================
# Tests: PipelineRunner
# ==============================================================================

class TestPipelineRunner:

    def test_runs_stages_in_order(self):
        """Stages execute sequentially, data flows through context."""
        order = []

        class Stage1(PipelineStage):
            def execute(self, ctx):
                order.append(1)
                ctx.data["step1"] = True
                return StageResult(success=True, message="s1")

        class Stage2(PipelineStage):
            def execute(self, ctx):
                order.append(2)
                assert ctx.data.get("step1") is True  # Data from Stage1
                return StageResult(success=True, message="s2")

        runner = PipelineRunner(
            stages=[Stage1(name="s1"), Stage2(name="s2")],
            max_retries=1,
        )
        ctx = runner.run()
        assert order == [1, 2]

    def test_aborts_on_failure(self):
        """Pipeline stops when a stage fails after retries."""
        runner = PipelineRunner(
            stages=[
                PassingStage(name="ok"),
                FailingStage(name="bad"),
                PassingStage(name="never_reached"),
            ],
            max_retries=1,
        )
        with pytest.raises(RuntimeError, match="bad"):
            runner.run()

    def test_retries_and_recovers(self):
        """Stage fails once then succeeds on retry."""
        stage = EventuallyPassesStage(fail_count=1)
        runner = PipelineRunner(
            stages=[stage],
            max_retries=2,
            retry_delay=0.01,  # Fast for testing
        )
        ctx = runner.run()
        assert stage._attempts == 2
        assert ctx.metrics["eventually_passes"].status == "success"

    def test_retries_exhausted(self):
        """Stage fails more times than max_retries → pipeline aborts."""
        stage = EventuallyPassesStage(fail_count=5)
        runner = PipelineRunner(
            stages=[stage],
            max_retries=2,
            retry_delay=0.01,
        )
        with pytest.raises(RuntimeError):
            runner.run()

    def test_handles_exceptions_in_stage(self):
        """Stage raising exception is caught and retried."""
        runner = PipelineRunner(
            stages=[ExplodingStage(name="boom")],
            max_retries=1,
            retry_delay=0.01,
        )
        with pytest.raises(RuntimeError, match="Connection refused"):
            runner.run()

    def test_on_success_hook_called(self):
        """on_success callback fires when pipeline completes."""
        callback = MagicMock()
        runner = PipelineRunner(
            stages=[PassingStage(name="ok")],
            max_retries=1,
            on_success=callback,
        )
        runner.run()
        callback.assert_called_once()

    def test_on_failure_hook_called(self):
        """on_failure callback fires when pipeline aborts."""
        callback = MagicMock()
        runner = PipelineRunner(
            stages=[FailingStage(name="fail")],
            max_retries=1,
            on_failure=callback,
        )
        with pytest.raises(RuntimeError):
            runner.run()
        callback.assert_called_once()

    def test_metrics_recorded_per_stage(self):
        """Each stage gets a metric entry in the context."""
        runner = PipelineRunner(
            stages=[PassingStage(name="s1"), PassingStage(name="s2")],
            max_retries=1,
        )
        ctx = runner.run()
        assert "s1" in ctx.metrics
        assert "s2" in ctx.metrics
        assert ctx.metrics["s1"].status == "success"

    def test_empty_pipeline(self):
        """Pipeline with no stages runs and returns context."""
        runner = PipelineRunner(stages=[], max_retries=1)
        ctx = runner.run()
        assert ctx.run_id  # Has a valid run_id


# ==============================================================================
# Tests: ExtractStage
# ==============================================================================

class TestExtractStage:

    def test_full_extract_stores_data_in_context(self):
        """ExtractStage in full mode calls fetcher and stores result."""
        from finpipe.pipeline.stages.extract import ExtractStage

        mock_df = pd.DataFrame({
            "symbol": ["TCS", "TCS"],
            "trade_date": ["2024-01-01", "2024-01-02"],
            "close_price": [3800.0, 3850.0],
        })
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.error_message = ""

        stage = ExtractStage(
            symbols=["TCS.NS"],
            period="1mo",
            incremental=False,
        )

        # Mock the fetcher
        stage._fetcher.fetch_multiple = MagicMock(
            return_value=[(mock_df, mock_result)]
        )

        ctx = PipelineContext()
        result = stage.execute(ctx)

        assert result.success
        assert result.rows_out == 2
        assert "raw_prices" in ctx.data
        assert len(ctx.data["raw_prices"]) == 2

    def test_empty_extract_is_success(self):
        """No data found is a success (just nothing new)."""
        from finpipe.pipeline.stages.extract import ExtractStage

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.error_message = ""

        stage = ExtractStage(symbols=["FAKE.NS"], incremental=False)
        stage._fetcher.fetch_multiple = MagicMock(
            return_value=[(pd.DataFrame(), mock_result)]
        )

        ctx = PipelineContext()
        result = stage.execute(ctx)

        assert result.success
        assert result.rows_out == 0


# ==============================================================================
# Tests: ValidateStage
# ==============================================================================

class TestValidateStage:

    def _make_df(self, rows=5):
        """Create a valid-looking stock DataFrame."""
        return pd.DataFrame({
            "symbol": ["TCS"] * rows,
            "exchange_code": ["NSE"] * rows,
            "trade_date": pd.date_range("2024-01-01", periods=rows).date,
            "open_price": [3800.0] * rows,
            "high_price": [3900.0] * rows,
            "low_price": [3700.0] * rows,
            "close_price": [3850.0] * rows,
            "volume": [1000000] * rows,
        })

    def test_valid_data_passes(self):
        from finpipe.pipeline.stages.validate import ValidateStage

        stage = ValidateStage(min_rows=1, max_rows=100)
        ctx = PipelineContext()
        ctx.data["raw_prices"] = self._make_df()

        result = stage.execute(ctx)
        assert result.success
        assert "validated_prices" in ctx.data

    def test_empty_data_passes(self):
        from finpipe.pipeline.stages.validate import ValidateStage

        stage = ValidateStage()
        ctx = PipelineContext()
        ctx.data["raw_prices"] = pd.DataFrame()

        result = stage.execute(ctx)
        assert result.success  # Empty is OK

    def test_null_critical_column_fails(self):
        from finpipe.pipeline.stages.validate import ValidateStage

        df = self._make_df()
        df.loc[0, "symbol"] = None  # Null in critical column

        stage = ValidateStage(min_rows=1, max_rows=100)
        ctx = PipelineContext()
        ctx.data["raw_prices"] = df

        result = stage.execute(ctx)
        assert not result.success
        assert "Critical" in result.error

    def test_negative_price_fails(self):
        from finpipe.pipeline.stages.validate import ValidateStage

        df = self._make_df()
        df.loc[0, "close_price"] = -100.0  # Negative price

        stage = ValidateStage(min_rows=1, max_rows=100)
        ctx = PipelineContext()
        ctx.data["raw_prices"] = df

        result = stage.execute(ctx)
        assert not result.success

    def test_row_count_check(self):
        from finpipe.pipeline.stages.validate import ValidateStage

        # Too many rows (max 3, but we have 5)
        stage = ValidateStage(min_rows=1, max_rows=3)
        ctx = PipelineContext()
        ctx.data["raw_prices"] = self._make_df(5)

        result = stage.execute(ctx)
        assert not result.success
