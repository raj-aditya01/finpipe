# ==============================================================================
# databricks/ — Databricks-Ready Transforms Module
# ==============================================================================
#
# This module contains code DESIGNED to run on Databricks (not locally).
#
# Local pipeline (finpipe CLI):    Yahoo Finance → Validate → Snowflake
# Databricks pipeline (this code): Snowflake → Spark transforms → Snowflake/Delta
#
# See README.md in this directory for setup instructions.
# ==============================================================================

from finpipe.databricks.transforms import SparkTransforms

__all__ = ["SparkTransforms"]
