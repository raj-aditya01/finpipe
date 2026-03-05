# ==============================================================================
# cli.py — Command-Line Interface (run the pipeline from terminal)
# ==============================================================================
#
# WHAT YOU LEARN HERE:
#   1. CLI design — how production data tools are invoked (dbt, airflow, spark)
#   2. Click library — Python's best CLI framework (used by Flask, dbt, pip)
#   3. Wiring it all together — config → connector → fetcher → loader
#   4. Error handling at the top level — catch and report gracefully
#
# HOW TO RUN:
#   python -m finpipe.cli health           # Test Snowflake connection
#   python -m finpipe.cli tables           # List tables in Snowflake
#   python -m finpipe.cli ingest           # Fetch + load stock data
#   python -m finpipe.cli ingest --symbols TCS.NS RELIANCE.NS --period 3mo
#   python -m finpipe.cli query "SELECT COUNT(*) FROM fact_stock_prices"
#
# ==============================================================================

import logging
import sys
from datetime import date

import click

# Set up logging FIRST (before importing modules that log on import)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("finpipe")


@click.group()
@click.version_option(version="0.1.0", prog_name="finpipe")
def cli():
    """
    finpipe — Financial Data Engineering Pipeline CLI.
    
    A data engineering learning project. Each command demonstrates
    a different concept: connections, ingestion, SQL, multithreading.
    """
    pass


# ==============================================================================
# COMMAND: health — Test Snowflake Connection
# ==============================================================================

@cli.command()
def health():
    """Test Snowflake connection and show environment info."""
    from finpipe.config import get_settings
    from finpipe.connectors import SnowflakeConnector
    
    settings = get_settings()
    
    click.echo("=" * 60)
    click.echo("finpipe — Connection Health Check")
    click.echo("=" * 60)
    click.echo(f"  Snowflake: {settings.snowflake_safe_display}")
    click.echo(f"  Warehouse: {settings.snowflake_warehouse}")
    click.echo(f"  Role:      {settings.snowflake_role}")
    click.echo("-" * 60)
    
    try:
        with SnowflakeConnector(settings) as conn:
            # Test basic connectivity
            rows = conn.execute_query("SELECT CURRENT_TIMESTAMP() AS ts, CURRENT_WAREHOUSE() AS wh")
            row = rows[0]
            ts = row.get("ts") or row.get("TS", "")
            wh = row.get("wh") or row.get("WH", "")
            click.echo(f"  Status:    CONNECTED")
            click.echo(f"  Server:    {ts}")
            click.echo(f"  Warehouse: {wh}")
            
            # List tables
            tables = conn.get_tables()
            click.echo(f"  Tables:    {len(tables)}")
            for t in tables:
                click.echo(f"             - {t}")
            
        click.echo("=" * 60)
        click.secho("  HEALTHY", fg="green", bold=True)
        click.echo("=" * 60)
        
    except Exception as e:
        click.echo("=" * 60)
        click.secho(f"  UNHEALTHY: {e}", fg="red", bold=True)
        click.echo("=" * 60)
        sys.exit(1)


# ==============================================================================
# COMMAND: tables — List Snowflake Tables
# ==============================================================================

@cli.command()
@click.option("--count", is_flag=True, help="Show row counts for each table")
def tables(count):
    """List all tables and views in the current Snowflake schema."""
    from finpipe.config import get_settings
    from finpipe.connectors import SnowflakeConnector
    
    settings = get_settings()
    
    with SnowflakeConnector(settings) as conn:
        table_list = conn.get_tables()
        
        if not table_list:
            click.echo("No tables found. Run the SQL schema creation first:")
            click.echo("  See: finpipe/sql/001_create_star_schema.sql")
            return
        
        click.echo(f"\nTables in {settings.snowflake_database}.{settings.snowflake_schema}:\n")
        
        for t in table_list:
            table_name = t.split(" (")[0]
            if count:
                try:
                    rc = conn.get_row_count(table_name)
                    click.echo(f"  {t:40s} → {rc:>10,} rows")
                except Exception:
                    click.echo(f"  {t:40s} → (error reading)")
            else:
                click.echo(f"  {t}")


# ==============================================================================
# COMMAND: query — Run ad-hoc SQL from the terminal
# ==============================================================================

@cli.command()
@click.argument("sql")
@click.option("--limit", default=20, help="Max rows to display")
def query(sql, limit):
    """Run a SQL query and display results.
    
    Example: finpipe query "SELECT * FROM fact_stock_prices LIMIT 5"
    """
    from finpipe.config import get_settings
    from finpipe.connectors import SnowflakeConnector
    
    settings = get_settings()
    
    with SnowflakeConnector(settings) as conn:
        try:
            df = conn.execute_query_df(sql)
        except Exception as e:
            click.secho(f"Query failed: {e}", fg="red")
            sys.exit(1)
        
        if df.empty:
            click.echo("(no rows returned)")
            return
        
        # Truncate for display
        display_df = df.head(limit)
        click.echo(f"\n{display_df.to_string(index=False)}")
        click.echo(f"\n({len(df)} rows total, showing {len(display_df)})")


# ==============================================================================
# COMMAND: ingest — Fetch stock data and load into Snowflake
# ==============================================================================

@cli.command()
@click.option(
    "--symbols", "-s", multiple=True,
    default=["TCS.NS", "RELIANCE.NS", "INFY.NS", "HDFCBANK.NS", "ICICIBANK.NS"],
    help="Yahoo Finance symbols to fetch (e.g., TCS.NS RELIANCE.NS)"
)
@click.option("--period", "-p", default="1mo", help="Period: 1d, 5d, 1mo, 3mo, 6mo, 1y, max")
@click.option("--workers", "-w", default=5, help="Max concurrent download threads")
@click.option("--dry-run", is_flag=True, help="Fetch data but don't load into Snowflake")
def ingest(symbols, period, workers, dry_run):
    """Fetch stock data from Yahoo Finance and load into Snowflake.
    
    Examples:
    
    \b
      finpipe ingest                                    # Default 5 stocks, 1 month
      finpipe ingest -s TCS.NS -s RELIANCE.NS -p 3mo   # Specific stocks, 3 months
      finpipe ingest --workers 10 --period 1y           # Faster, 1 year of data
      finpipe ingest --dry-run                          # Fetch only, don't load
    """
    from finpipe.config import get_settings
    from finpipe.connectors import SnowflakeConnector
    from finpipe.ingestion import YFinanceFetcher, SnowflakeLoader
    
    symbols_list = list(symbols)
    
    click.echo("=" * 60)
    click.echo(f"finpipe — Data Ingestion")
    click.echo("=" * 60)
    click.echo(f"  Symbols:  {', '.join(symbols_list)}")
    click.echo(f"  Period:   {period}")
    click.echo(f"  Workers:  {workers}")
    click.echo(f"  Dry run:  {dry_run}")
    click.echo("-" * 60)
    
    # Step 1: Fetch data (multithreaded)
    click.echo("\n[1/2] Fetching data from Yahoo Finance...")
    fetcher = YFinanceFetcher(max_workers=workers)
    combined_df = fetcher.fetch_to_dataframe(symbols_list, period=period)
    
    if combined_df.empty:
        click.secho("No data fetched. Check your symbols and internet connection.", fg="red")
        sys.exit(1)
    
    click.echo(f"  Fetched: {len(combined_df)} rows for {combined_df['symbol'].nunique()} symbols")
    click.echo(f"\n  Preview (first 5 rows):")
    click.echo(f"  {combined_df.head().to_string(index=False)}")
    
    if dry_run:
        click.echo(f"\n  [DRY RUN] Skipping Snowflake load.")
        click.secho("  Done!", fg="green")
        return
    
    # Step 2: Load into Snowflake
    click.echo(f"\n[2/2] Loading into Snowflake...")
    settings = get_settings()
    
    with SnowflakeConnector(settings) as conn:
        loader = SnowflakeLoader(conn)
        result = loader.load_stock_prices(combined_df)
        
        if result.success:
            click.secho(f"\n  {result.summary}", fg="green")
        else:
            click.secho(f"\n  {result.summary}", fg="red")
            sys.exit(1)
    
    click.echo("=" * 60)
    click.secho("  Ingestion complete!", fg="green", bold=True)
    click.echo("=" * 60)


# ==============================================================================
# COMMAND: schema — Run the star schema SQL to create tables
# ==============================================================================

@cli.command()
@click.option("--file", "-f", default=None,
              help="SQL file to execute")
@click.confirmation_option(prompt="This will create/modify Snowflake tables. Continue?")
def schema(file):
    """Execute SQL schema file to create star schema tables in Snowflake."""
    import pathlib
    from finpipe.config import get_settings
    from finpipe.connectors import SnowflakeConnector
    
    # Default to the SQL file bundled with the package
    if file is None:
        file = str(pathlib.Path(__file__).parent / "sql" / "001_create_star_schema.sql")
    
    settings = get_settings()
    
    try:
        with open(file, "r", encoding="utf-8") as f:
            sql_content = f.read()
    except FileNotFoundError:
        click.secho(f"SQL file not found: {file}", fg="red")
        sys.exit(1)
    
    # Split by semicolons into individual SQL statements
    raw_parts = sql_content.split(";")
    statements = []
    for part in raw_parts:
        # Strip leading/trailing whitespace
        stripped = part.strip()
        if not stripped:
            continue
        # Check if there's any actual SQL (not just comments)
        sql_lines = [l for l in stripped.split("\n") if l.strip() and not l.strip().startswith("--")]
        if sql_lines:
            statements.append(stripped)
    
    click.echo(f"Executing {len(statements)} SQL statements from {file}...")
    
    with SnowflakeConnector(settings) as conn:
        for i, stmt in enumerate(statements, 1):
            # Skip empty or comment-only blocks
            lines = [l for l in stmt.split("\n") if l.strip() and not l.strip().startswith("--")]
            if not lines:
                continue
            
            first_line = lines[0].strip()[:60]
            click.echo(f"  [{i}/{len(statements)}] {first_line}...")
            
            try:
                conn.execute_non_query(stmt)
                click.secho(f"           OK", fg="green")
            except Exception as e:
                click.secho(f"           ERROR: {e}", fg="red")
    
    click.secho("\nSchema setup complete!", fg="green", bold=True)


# ==============================================================================
# ENTRY POINT
# ==============================================================================
# This allows running: python -m finpipe.cli
# The -m flag tells Python to run a module as a script.
# ==============================================================================

if __name__ == "__main__":
    cli()
