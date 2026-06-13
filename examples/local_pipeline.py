"""
Example: building a local data pipeline with lakekit.

This script shows how to wire up the same building blocks used in the notebooks
into a proper Python application -- syncing tables, querying with DuckDB, running
dbt transforms, and producing a result. Run it directly or import the functions
into your own application code.

Usage:
    # Sync, transform, and print a summary report
    python examples/local_pipeline.py

    # Sync only (no dbt)
    python examples/local_pipeline.py --sync-only

    # Query only (assumes cache and dbt models are already up to date)
    python examples/local_pipeline.py --query-only

    # Push dbt models to Databricks after local validation
    python examples/local_pipeline.py --target prod
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import duckdb

from lakekit.sync import CACHE_DIR, register_local, sync_table

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ADLS_ACCOUNT = os.environ.get("ADLS_ACCOUNT", "your-adls-account")
DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DB_PATH = os.environ.get(
    "LAKEHOUSE_DB_PATH",
    str(Path(__file__).parent.parent / ".cache" / "lakehouse_dev.duckdb"),
)
DBT_DIR = Path(__file__).parent.parent / "dbt"

# Tables to sync: (alias, container, path, storage_account)
TABLES = [
    ("raw_events",     "bronze", "events/raw_events/",     ADLS_ACCOUNT),
    ("event_tracking", "bronze", "events/event_tracking/", ADLS_ACCOUNT),
    ("event_metadata", "bronze", "events/event_metadata/", ADLS_ACCOUNT),
]


# ---------------------------------------------------------------------------
# Step 1: Sync -- pull Delta table snapshots to local Parquet cache
# ---------------------------------------------------------------------------

def sync_all(tables: list = TABLES) -> list[str]:
    """
    Incrementally sync all listed tables to the local cache.

    Only downloads Parquet files that are new since the last sync.
    Returns a list of table names that are now available locally.
    """
    print("=== Sync ===")
    synced = []
    for name, container, path, storage in tables:
        try:
            sync_table(name, container, path, storage)
            synced.append(name)
        except Exception as exc:
            print(f"  {name}: FAILED -- {exc}", file=sys.stderr)
    return synced


# ---------------------------------------------------------------------------
# Step 2: Query -- register cache in DuckDB and run ad-hoc SQL
# ---------------------------------------------------------------------------

def open_connection(tables: list = TABLES) -> duckdb.DuckDBPyConnection:
    """
    Open a DuckDB connection and register all locally cached tables as views.

    Tables with >500 Parquet files are registered via a lazy PyArrow dataset
    to avoid OOM on footer scans. The same connection reads from both the raw
    cache views AND any dbt-built silver/gold tables in DB_PATH (if they exist).
    """
    conn = duckdb.connect(DB_PATH)
    registered = []
    for name, *_ in tables:
        if register_local(conn, name):
            registered.append(name)
        else:
            print(f"  WARNING: {name} not in cache -- run sync first", file=sys.stderr)
    print(f"Registered bronze views: {registered}")
    return conn


def cache_status(tables: list = TABLES) -> None:
    """Print the version and age of each cached table."""
    print("\n=== Cache status ===")
    from datetime import datetime, timezone
    for name, *_ in tables:
        manifest_path = CACHE_DIR / name / ".manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            age_h = (datetime.now(timezone.utc).timestamp() - manifest_path.stat().st_mtime) / 3600
            print(f"  {name}: v{manifest['version']}  {len(manifest['files'])} files  {age_h:.1f}h old")
        else:
            print(f"  {name}: not cached")


# ---------------------------------------------------------------------------
# Step 3: dbt -- run transforms locally or push to Databricks
# ---------------------------------------------------------------------------

def dbt_run(target: str = "local", select: str | None = None) -> bool:
    """
    Run dbt models against the given target.

    target="local"  -- reads Parquet cache, writes to local DuckDB (~3s for full run)
    target="prod"   -- reads Unity Catalog bronze, writes to Unity Catalog silver

    Returns True if dbt exited cleanly, False otherwise.
    """
    print(f"\n=== dbt run --target {target} ===")

    if target == "prod":
        _ensure_databricks_token()

    cmd = ["dbt", "run", "--target", target]
    if select:
        cmd += ["--select", select]

    result = subprocess.run(cmd, cwd=DBT_DIR)
    return result.returncode == 0


def dbt_compile(target: str = "local", select: str | None = None) -> None:
    """
    Compile dbt models without running them -- useful for inspecting how
    cross_db macros resolve for each target (DuckDB vs SparkSQL).
    """
    print(f"\n=== dbt compile --target {target} ===")
    cmd = ["dbt", "compile", "--target", target]
    if select:
        cmd += ["--select", select]
    subprocess.run(cmd, cwd=DBT_DIR)


def _ensure_databricks_token() -> None:
    """Fetch a fresh Databricks token via the CLI and export it as DATABRICKS_TOKEN."""
    if not DATABRICKS_HOST:
        raise RuntimeError("DATABRICKS_HOST env var is not set")
    token = subprocess.check_output(
        ["databricks", "auth", "token", "--host", DATABRICKS_HOST],
        text=True,
    ).strip()
    os.environ["DATABRICKS_TOKEN"] = token
    print(f"  Token obtained for {DATABRICKS_HOST}")


# ---------------------------------------------------------------------------
# Step 4: Report -- query the dbt-built models and produce output
# ---------------------------------------------------------------------------

def run_report(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Example report: query the gold layer and print a summary.

    In a real application you would write this to a file, post it to Slack,
    push it to a dashboard API, etc.
    """
    print("\n=== Report ===")

    # Top-level counts from the raw bronze layer
    totals = conn.sql("""
        SELECT
            COUNT(*)                                        AS total_events,
            COUNT(*) FILTER (WHERE status = 'completed')   AS completed,
            COUNT(*) FILTER (WHERE status = 'missed')      AS missed,
            COUNT(*) FILTER (WHERE status = 'cancelled')   AS cancelled
        FROM raw_events
    """).fetchone()

    print(f"  Total events : {totals[0]:,}")
    print(f"  Completed    : {totals[1]:,}")
    print(f"  Missed       : {totals[2]:,}")
    print(f"  Cancelled    : {totals[3]:,}")

    # Daily metrics from the gold model (only available after dbt run)
    try:
        top_days = conn.sql("""
            SELECT event_date, SUM(total_events) AS events
            FROM gold.daily_metrics
            GROUP BY event_date
            ORDER BY event_date DESC
            LIMIT 5
        """).fetchall()

        print("\n  Last 5 days:")
        for row in top_days:
            print(f"    {row[0]}  {row[1]:,} events")

    except duckdb.CatalogException:
        print("  Gold models not yet built -- run dbt first (skip --query-only to include dbt)")


# ---------------------------------------------------------------------------
# Full pipeline
# ---------------------------------------------------------------------------

def run_pipeline(target: str = "local", sync: bool = True, run_dbt: bool = True) -> None:
    """
    Full pipeline: sync -> register -> dbt run -> report.

    In CI or a scheduled job you would call this directly. Locally you can
    also call each step independently during development.
    """
    if sync:
        sync_all()

    cache_status()

    conn = open_connection()

    if run_dbt:
        ok = dbt_run(target=target)
        if not ok:
            print("dbt run failed -- check output above", file=sys.stderr)
            sys.exit(1)

    run_report(conn)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Local lakehouse pipeline")
    p.add_argument("--sync-only",  action="store_true", help="Sync tables and exit")
    p.add_argument("--query-only", action="store_true", help="Query existing cache, skip sync and dbt")
    p.add_argument("--target",     default="local",     help="dbt target: local (default) or prod")
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    if args.sync_only:
        sync_all()
        cache_status()
    elif args.query_only:
        conn = open_connection()
        run_report(conn)
    else:
        run_pipeline(
            target=args.target,
            sync=True,
            run_dbt=True,
        )
