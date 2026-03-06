# ==============================================================================
# tests/test_config.py — Unit Tests for Configuration System
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Testing configuration loading
#   2. Mocking environment variables with monkeypatch
#   3. How Pydantic Settings validates at construction time
#
# ==============================================================================

import pytest
from finpipe.config.settings import Settings


class TestSettings:
    """Test suite for Pydantic Settings."""
    
    def test_loads_from_env(self, monkeypatch):
        """Test that Settings reads from environment variables."""
        monkeypatch.setenv("SNOWFLAKE_USER", "test_user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "test_pass123")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "xy12345.us-east-1")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "TEST_DB")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "TEST_WH")
        monkeypatch.setenv("SNOWFLAKE_ROLE", "TEST_ROLE")
        
        settings = Settings()
        assert settings.snowflake_user == "test_user"
        assert settings.snowflake_database == "TEST_DB"
    
    def test_snowflake_uri_format(self, monkeypatch):
        """Test that snowflake_uri builds correct connection string."""
        monkeypatch.setenv("SNOWFLAKE_USER", "user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pass")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "sch")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "wh")
        monkeypatch.setenv("SNOWFLAKE_ROLE", "role")
        
        settings = Settings()
        uri = settings.snowflake_uri
        assert uri.startswith("snowflake://")
        assert "user:pass@acct" in uri
        assert "/db/sch" in uri
        assert "warehouse=wh" in uri
        assert "role=role" in uri
    
    def test_safe_display_masks_password(self, monkeypatch):
        """Test that snowflake_safe_display hides the password."""
        monkeypatch.setenv("SNOWFLAKE_USER", "user")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "supersecret")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "db")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "sch")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "wh")
        monkeypatch.setenv("SNOWFLAKE_ROLE", "role")
        
        settings = Settings()
        safe = settings.snowflake_safe_display
        # snowflake_safe_display omits the password entirely
        assert "supersecret" not in safe
        assert "user@acct" in safe
    
    def test_defaults(self, monkeypatch):
        """Test default values are applied."""
        monkeypatch.setenv("SNOWFLAKE_USER", "u")
        monkeypatch.setenv("SNOWFLAKE_PASSWORD", "p")
        monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "a")
        monkeypatch.setenv("SNOWFLAKE_DATABASE", "d")
        monkeypatch.setenv("SNOWFLAKE_SCHEMA", "s")
        monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "w")
        monkeypatch.setenv("SNOWFLAKE_ROLE", "r")
        
        settings = Settings()
        assert settings.log_level == "INFO"
        assert settings.cache_ttl_seconds == 300
        assert settings.max_concurrent_downloads == 5
