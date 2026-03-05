# ==============================================================================
# tests/test_connectors.py — Unit Tests for Connector Layer
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. Unit testing with mocks — isolating code from external systems
#   2. patch() and MagicMock — faking database connections
#   3. Testing abstract base classes
#   4. Context manager testing (with statement)
#
# WHY MOCK?
#   Unit tests should NOT hit real databases. We mock Snowflake so tests
#   run instantly, offline, and without credentials. Integration tests
#   (test_integration.py) will hit real Snowflake later.
#
# ==============================================================================

from unittest.mock import patch, MagicMock
import pytest
from finpipe.connectors.base import BaseConnector
from finpipe.connectors.snowflake_connector import SnowflakeConnector


class TestBaseConnector:
    """Test the abstract base class."""
    
    def test_cannot_instantiate_abstract(self):
        """ABC cannot be instantiated directly — forces subclassing."""
        with pytest.raises(TypeError):
            BaseConnector()  # type: ignore
    
    def test_subclass_must_implement_methods(self):
        """Incomplete subclass raises TypeError."""
        class IncompleteConnector(BaseConnector):
            def connect(self): ...
            # Missing: disconnect, execute_query, execute_query_df
        
        with pytest.raises(TypeError):
            IncompleteConnector()  # type: ignore


class TestSnowflakeConnector:
    """Test SnowflakeConnector with mocked SQLAlchemy engine."""

    def _make_settings(self):
        """Create a mock Settings object for testing."""
        mock_settings = MagicMock()
        mock_settings.snowflake_uri = "snowflake://user:pass@acct/db/sch?warehouse=wh&role=role"
        mock_settings.snowflake_safe_display = "snowflake://user:****@acct/db/sch"
        mock_settings.snowflake_schema = "PUBLIC"
        return mock_settings

    @patch("finpipe.connectors.snowflake_connector.create_engine")
    def test_connect_creates_engine(self, mock_create_engine):
        """Test that connect() calls create_engine with correct URI."""
        mock_engine = MagicMock()
        # Mock the initial connection test: engine.connect() context manager
        mock_conn_ctx = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = "2026-03-05 00:00:00"
        mock_conn_ctx.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn_ctx)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = mock_engine

        conn = SnowflakeConnector(self._make_settings())
        conn.connect()

        mock_create_engine.assert_called_once()
        assert conn.is_connected is True

    @patch("finpipe.connectors.snowflake_connector.create_engine")
    def test_disconnect(self, mock_create_engine):
        """Test that disconnect() disposes the engine."""
        mock_engine = MagicMock()
        mock_conn_ctx = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = "2026-03-05 00:00:00"
        mock_conn_ctx.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn_ctx)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = mock_engine

        conn = SnowflakeConnector(self._make_settings())
        conn.connect()
        conn.disconnect()

        mock_engine.dispose.assert_called_once()
        assert conn.is_connected is False

    @patch("finpipe.connectors.snowflake_connector.create_engine")
    def test_context_manager(self, mock_create_engine):
        """Test with-statement calls connect on enter and disconnect on exit."""
        mock_engine = MagicMock()
        mock_conn_ctx = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.return_value = "2026-03-05 00:00:00"
        mock_conn_ctx.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn_ctx)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_create_engine.return_value = mock_engine

        with SnowflakeConnector(self._make_settings()) as conn:
            assert conn.is_connected is True

        # After exiting the with block, engine.dispose() should be called
        mock_engine.dispose.assert_called_once()

    @patch("finpipe.connectors.snowflake_connector.create_engine")
    def test_execute_query_returns_dicts(self, mock_create_engine):
        """Test that execute_query returns list of dicts."""
        # Build mock chain: engine.connect() -> connection -> execute() -> result
        mock_row1 = MagicMock()
        mock_row1._mapping = {"name": "Alice", "value": 1}
        mock_row2 = MagicMock()
        mock_row2._mapping = {"name": "Bob", "value": 2}

        mock_exec_result = MagicMock()
        mock_exec_result.__iter__ = MagicMock(return_value=iter([mock_row1, mock_row2]))

        # Connection for execute_query
        mock_query_conn = MagicMock()
        mock_query_conn.execute.return_value = mock_exec_result
        mock_query_conn.__enter__ = MagicMock(return_value=mock_query_conn)
        mock_query_conn.__exit__ = MagicMock(return_value=False)

        # Connection for connect() verification
        mock_init_conn = MagicMock()
        mock_init_result = MagicMock()
        mock_init_result.scalar.return_value = "2026-03-05"
        mock_init_conn.execute.return_value = mock_init_result
        mock_init_conn.__enter__ = MagicMock(return_value=mock_init_conn)
        mock_init_conn.__exit__ = MagicMock(return_value=False)

        mock_engine = MagicMock()
        # First connect() call is for init verification, second for execute_query
        mock_engine.connect.side_effect = [mock_init_conn, mock_query_conn]
        mock_create_engine.return_value = mock_engine

        conn = SnowflakeConnector(self._make_settings())
        conn.connect()
        rows = conn.execute_query("SELECT name, value FROM test")

        assert len(rows) == 2
        assert rows[0] == {"name": "Alice", "value": 1}
        assert rows[1] == {"name": "Bob", "value": 2}
