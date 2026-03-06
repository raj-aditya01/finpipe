# ==============================================================================
# config/__init__.py — Configuration Package
# ==============================================================================
# WHAT YOU LEARN HERE:
#   - Python packages (what __init__.py does)
#   - Clean imports (from finpipe.config import settings)
# ==============================================================================

from finpipe.config.settings import Settings, get_settings

__all__ = ["Settings", "get_settings"]
