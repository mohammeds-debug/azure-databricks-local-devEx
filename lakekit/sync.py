"""
Local Parquet cache for Delta tables on ADLS Gen2.

Strategy:
  - First sync: download all Parquet files for the current Delta snapshot
  - Subsequent syncs: compare remote file list vs local manifest, download only
    new files and delete removed ones -- no full re-download

Delta table files are immutable UUIDs, so file-level diffing is safe even for
tables with compaction or partition rewrites.

Usage:
    from lakekit.sync import sync_table, register_local, CACHE_DIR

    # Sync (run once, then when you want fresh data)
    sync_table("raw_events", "bronze", "events/raw_events/", "your-adls-account")

    # Register in DuckDB -- returns True if cache exists, False if not yet synced
    if not register_local(conn, "raw_events"):
        print("Run sync_table() first")
"""

import json
import os
from pathlib import Path

import duckdb
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable

# Default cache is in the workspace dir -- survives devcontainer rebuilds because
# the workspace is a host mount. Override with LAKEHOUSE_CACHE env var if needed.
CACHE_DIR = Path(os.getenv("LAKEHOUSE_CACHE", Path(__file__).parent.parent / ".cache"))


def _adls_client(storage_account: str) -> DataLakeServiceClient:
    return DataLakeServiceClient(
        account_url=f"https://{storage_account}.dfs.core.windows.net",
        credential=AzureCliCredential(),
    )


_cred = AzureCliCredential()


def _fresh_storage_options(storage: str) -> dict:
    # Always fetch a fresh token -- avoids 401s when syncing multiple tables
    token = _cred.get_token("https://storage.azure.com/.default").token
    return {"bearer_token": token, "azure_storage_account_name": storage}


def sync_table(
    name: str,
    container: str,
    table_path: str,
    storage: str,
    cache_dir: Path = CACHE_DIR,
) -> Path:
    """
    Download or incrementally update a Delta table to a local Parquet cache.

    Only files present in the remote snapshot but missing locally are downloaded.
    Files removed from the remote snapshot are deleted locally.
    Returns the local cache directory.
    """
    local_dir = cache_dir / name
    local_dir.mkdir(parents=True, exist_ok=True)
    manifest_file = local_dir / ".manifest.json"

    storage_options = _fresh_storage_options(storage)
    uri = f"abfss://{container}@{storage}.dfs.core.windows.net/{table_path}"

    # skip_stats=True avoids reading per-file column stats from the Delta log,
    # which significantly reduces data read for tables with large transaction logs.
    remote_dt = DeltaTable(uri, storage_options=storage_options, skip_stats=True)
    remote_version = remote_dt.version()
    remote_files: list[str] = remote_dt.get_add_actions(flatten=True)["path"].to_pylist()

    manifest = (
        json.loads(manifest_file.read_text())
        if manifest_file.exists()
        else {"version": -1, "files": []}
    )

    if remote_version is not None and manifest["version"] == remote_version:
        print(f"  {name}: up to date (v{remote_version})")
        return local_dir

    cached = set(manifest["files"])
    remote = set(remote_files)
    to_add = remote - cached
    to_remove = cached - remote

    print(f"  {name}: v{manifest['version']} -> v{remote_version}  +{len(to_add)} files  -{len(to_remove)} files")

    if to_add:
        fs = _adls_client(storage).get_file_system_client(container)
        for rel_path in sorted(to_add):
            remote_adls_path = table_path.rstrip("/") + "/" + rel_path
            local_file = local_dir / rel_path
            local_file.parent.mkdir(parents=True, exist_ok=True)
            data = fs.get_file_client(remote_adls_path).download_file().readall()
            local_file.write_bytes(data)
            print(f"    + {rel_path}")

    for rel_path in sorted(to_remove):
        local_file = local_dir / rel_path
        if local_file.exists():
            local_file.unlink()
            print(f"    - {rel_path}")

    manifest_file.write_text(json.dumps({"version": remote_version, "files": list(remote)}))
    print(f"  {name}: done")
    return local_dir


def register_local(conn: duckdb.DuckDBPyConnection, name: str, cache_dir: Path = CACHE_DIR) -> bool:
    """
    Register a locally cached table in DuckDB as a view over local Parquet files.
    Returns True if the cache exists and was registered, False if not yet synced.
    """
    local_dir = cache_dir / name
    manifest_file = local_dir / ".manifest.json"

    if not manifest_file.exists():
        return False

    manifest = json.loads(manifest_file.read_text())
    parquet_files = [str(local_dir / f) for f in manifest["files"] if f.endswith(".parquet")]
    if not parquet_files:
        return False

    if len(parquet_files) > 500:
        # Too many files for DuckDB read_parquet (opens all footers upfront -> OOM).
        # Use a PyArrow dataset instead -- lazy schema inference, memory-safe.
        import pyarrow.dataset as pad
        ds = pad.dataset(parquet_files, format="parquet", schema=None)
        conn.register(name, ds)
    else:
        files_sql = ", ".join(f"'{p}'" for p in parquet_files)
        conn.execute(
            f"CREATE OR REPLACE VIEW {name} AS "
            f"SELECT * FROM read_parquet([{files_sql}], union_by_name=true)"
        )
    return True
