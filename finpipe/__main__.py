# ==============================================================================
# __main__.py — Allows running: python -m finpipe
# ==============================================================================
# When Python sees `python -m finpipe`, it looks for __main__.py in the package.
# This file just calls the CLI entry point.
# ==============================================================================

from finpipe.cli import cli

cli()
