"""
Local query helper for Delta tables on Azure ADLS Gen2.

Auth: uses your active `az login` session via AzureCliCredential.
Run `az login && az account set --subscription $AZURE_SUBSCRIPTION_ID` before use.

Storage account:
  ADLS_ACCOUNT  (env)  -- your Azure Data Lake Storage Gen2 account

Discovery:
  list_tables("bronze")          -- scan a container for all Delta tables
  list_containers()              -- list available containers in the storage account
  open_path("bronze", "my/table/")  -- open any table by container + path
"""

import os
import time
from typing import Optional

import duckdb
import pyarrow as pa
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from deltalake import DeltaTable, write_deltalake

_TOKEN_TTL = 50 * 60  # refresh bearer token after 50 min

ADLS_ACCOUNT = os.getenv("ADLS_ACCOUNT", "your-adls-account")

# Add your own tables here: "alias": ("container", "path/in/container/", ADLS_ACCOUNT)
TABLES: dict[str, tuple[str, str, str]] = {
    "raw_events":      ("bronze", "events/raw_events/",      ADLS_ACCOUNT),
    "event_tracking":  ("bronze", "events/event_tracking/",  ADLS_ACCOUNT),
    "event_metadata":  ("bronze", "events/event_metadata/",  ADLS_ACCOUNT),
}


class LakehouseClient:
    """Stateful client that caches the bearer token across calls."""

    def __init__(self):
        self._cred = AzureCliCredential()
        self._token: Optional[str] = None
        self._token_fetched_at: float = 0.0

    def _storage_options(self, storage_account: str) -> dict:
        now = time.monotonic()
        if self._token is None or (now - self._token_fetched_at) > _TOKEN_TTL:
            self._token = self._cred.get_token("https://storage.azure.com/.default").token
            self._token_fetched_at = now
        return {
            "azure_storage_account_name": storage_account,
            "bearer_token": self._token,
        }

    def table_path(self, container: str, path: str, storage_account: str = ADLS_ACCOUNT) -> str:
        return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"

    def open(self, table_name: str, skip_stats: bool = False) -> DeltaTable:
        """Open a known table by name. See TABLES dict for available names."""
        if table_name not in TABLES:
            raise KeyError(f"Unknown table '{table_name}'. Known: {list(TABLES)}")
        container, path, storage_account = TABLES[table_name]
        return self.open_path(container, path, storage_account, skip_stats=skip_stats)

    def open_path(self, container: str, path: str, storage_account: str = ADLS_ACCOUNT, skip_stats: bool = False) -> DeltaTable:
        """Open any Delta table by container + path."""
        uri = self.table_path(container, path, storage_account)
        return DeltaTable(uri, storage_options=self._storage_options(storage_account), skip_stats=skip_stats)

    def to_dataset(self, table_name: str, skip_stats: bool = False):
        """Return a lazy PyArrow Dataset -- scanned on demand, nothing downloaded upfront."""
        return self.open(table_name, skip_stats=skip_stats).to_pyarrow_dataset()

    def to_arrow(
        self,
        table_name: str,
        columns: Optional[list[str]] = None,
        filters: Optional[list] = None,
    ) -> pa.Table:
        """Read a known table into a PyArrow Table (eager -- downloads everything)."""
        dt = self.open(table_name)
        return dt.to_pyarrow_table(columns=columns, filters=filters)

    def query(self, sql: str, **tables: pa.Table) -> duckdb.DuckDBPyRelation:
        """
        Run SQL with DuckDB. Pass keyword args to register Arrow tables by name.

        Example:
            lh = LakehouseClient()
            calls = lh.to_arrow("calls")
            lh.query("SELECT status, COUNT(*) FROM calls GROUP BY 1", calls=calls).df()
        """
        con = duckdb.connect()
        for name, tbl in tables.items():
            con.register(name, tbl)
        return con.sql(sql)

    def _adls_client(self, storage_account: str) -> DataLakeServiceClient:
        return DataLakeServiceClient(
            account_url=f"https://{storage_account}.dfs.core.windows.net",
            credential=self._cred,
        )

    def list_containers(self, storage_account: str = ADLS_ACCOUNT) -> list[str]:
        """List all containers in the storage account."""
        client = self._adls_client(storage_account)
        return [fs.name for fs in client.list_file_systems()]

    def list_tables(
        self,
        container: str,
        storage_account: str = ADLS_ACCOUNT,
        max_depth: int = 3,
    ) -> list[str]:
        """
        Scan a container for Delta tables and return their paths.

        Looks for directories containing _delta_log up to max_depth levels deep.

        Example:
            lh.list_tables("bronze")
            lh.list_tables("silver")
        """
        client = self._adls_client(storage_account)
        fs_client = client.get_file_system_client(container)

        delta_log_marker = "_delta_log"
        delta_paths: set[str] = set()

        for item in fs_client.get_paths(recursive=True):
            if not item.is_directory:
                continue
            parts = item.name.rstrip("/").split("/")
            if parts[-1] == delta_log_marker and len(parts) <= max_depth + 1:
                table_path = "/".join(parts[:-1]) + "/"
                delta_paths.add(table_path)

        return sorted(delta_paths)

    def schema(self, table_name: str) -> pa.Schema:
        return self.open(table_name).schema().to_arrow()

    def history(self, table_name: str, limit: int = 10) -> list[dict]:
        return self.open(table_name).history(limit=limit)

    def sample(
        self,
        table_name: str,
        n: int = 100,
        columns: Optional[list[str]] = None,
    ) -> pa.Table:
        """Pull a small sample without a full table scan."""
        dt = self.open(table_name)
        return dt.to_pyarrow_dataset().head(n, columns=columns)


# ---------------------------------------------------------------------------
# LakehouseDuckDB -- persistent DuckDB connection
# ---------------------------------------------------------------------------
# Opens the same .duckdb file that dbt-duckdb writes to, so dbt-built silver/gold
# models are visible alongside bronze source views in the same query session.
#
# Workflow:
#   1. lhdb = db()
#   2. lhdb.init_sources()   -- creates DuckDB views for bronze tables
#   3. lhdb.query("SELECT ...").df()
#
# After `dbt run --target local`:
#   lhdb.query("SELECT * FROM silver.your_model LIMIT 10").df()

_DB_PATH = os.getenv("LAKEHOUSE_DB_PATH", "/workspaces/your-project/local-devex/.cache/lakehouse_dev.duckdb")


class LakehouseDuckDB:
    """
    Persistent DuckDB connection backed by the local lakehouse file.

    Shares the same database file as dbt-duckdb. dbt-built models (silver/gold)
    and bronze source views are all queryable in the same session.
    """

    def __init__(self, path: str = _DB_PATH):
        self._path = path
        self._con: Optional[duckdb.DuckDBPyConnection] = None

    def _get_con(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(self._path)
            self._con.execute("INSTALL delta; LOAD delta;")
            self._con.execute("INSTALL azure;  LOAD azure;")
            # picks up `az login` token automatically
            self._con.execute("""
                CREATE OR REPLACE SECRET azure_cred (
                    TYPE     azure,
                    PROVIDER credential_chain
                )
            """)
        return self._con

    def init_sources(self) -> None:
        """
        Register all known bronze tables as DuckDB views via delta_scan.

        Safe to call multiple times (CREATE OR REPLACE). Not needed if you have
        already run `dbt run --target local` -- dbt-duckdb creates the same views
        automatically.
        """
        con = self._get_con()
        for name, (container, path, storage_account) in TABLES.items():
            uri = f"abfss://{container}@{storage_account}.dfs.core.windows.net/{path}"
            schema = container
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            con.execute(
                f"CREATE OR REPLACE VIEW {schema}.{name} AS "
                f"SELECT * FROM delta_scan('{uri}')"
            )
        print(f"Registered {len(TABLES)} source views in {self._path}")

    def query(self, sql: str) -> duckdb.DuckDBPyRelation:
        """
        Run any SQL against the persistent DuckDB file.

        Bronze source views (bronze.<table>) and dbt-built models (silver.<model>,
        gold.<model>) are all visible. Returns a DuckDB relation -- call .df()
        for a pandas DataFrame or .arrow() for PyArrow.
        """
        return self._get_con().sql(sql)

    def tables(self) -> duckdb.DuckDBPyRelation:
        """List every table and view visible in the database."""
        return self._get_con().sql("SHOW ALL TABLES")

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            self._con = None


_client: Optional[LakehouseClient] = None
_db_singleton: Optional[LakehouseDuckDB] = None


def _get_client() -> LakehouseClient:
    global _client
    if _client is None:
        _client = LakehouseClient()
    return _client


def db() -> LakehouseDuckDB:
    """Return the module-level LakehouseDuckDB singleton."""
    global _db_singleton
    if _db_singleton is None:
        _db_singleton = LakehouseDuckDB()
    return _db_singleton


def list_containers(storage_account: str = ADLS_ACCOUNT) -> list[str]:
    return _get_client().list_containers(storage_account)


def list_tables(
    container: str,
    storage_account: str = ADLS_ACCOUNT,
    max_depth: int = 3,
) -> list[str]:
    return _get_client().list_tables(container, storage_account, max_depth)


def open_table(table_name: str) -> DeltaTable:
    return _get_client().open(table_name)


def open_path(container: str, path: str, storage_account: str = ADLS_ACCOUNT) -> DeltaTable:
    return _get_client().open_path(container, path, storage_account)


def to_dataset(table_name: str, skip_stats: bool = False):
    """Lazy PyArrow Dataset -- use this for DuckDB; data is fetched on demand per query."""
    return _get_client().to_dataset(table_name, skip_stats=skip_stats)


def to_arrow(
    table_name: str,
    columns: Optional[list[str]] = None,
    filters: Optional[list] = None,
) -> pa.Table:
    return _get_client().to_arrow(table_name, columns=columns, filters=filters)


def query(sql: str, **tables: pa.Table) -> duckdb.DuckDBPyRelation:
    return _get_client().query(sql, **tables)


def table_path(container: str, path: str, storage_account: str = ADLS_ACCOUNT) -> str:
    return _get_client().table_path(container, path, storage_account)


def schema(table_name: str) -> pa.Schema:
    return _get_client().schema(table_name)


def sample(table_name: str, n: int = 100, columns: Optional[list[str]] = None) -> pa.Table:
    return _get_client().sample(table_name, n=n, columns=columns)
