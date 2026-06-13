# Local Lakehouse DevEx: Claude Context

## Before doing anything else

Run this cache freshness check:

```bash
python3 -c "
import json, os
from pathlib import Path
from datetime import datetime, timezone

cache = Path('/workspaces/your-project/local-devex/.cache')
if not cache.exists():
    print('CACHE MISSING - run the Sync cell in explore_sql.ipynb before querying')
else:
    stale = []
    for manifest in cache.glob('*/.manifest.json'):
        age_hours = (datetime.now(timezone.utc).timestamp() - manifest.stat().st_mtime) / 3600
        if age_hours > 24:
            stale.append(f'{manifest.parent.name} ({age_hours:.0f}h old)')
    if stale:
        print('STALE CACHE - run the Sync cell in explore_sql.ipynb to refresh:')
        for t in stale: print(f'  {t}')
    else:
        print('Cache is fresh.')
"
```

If any table cache is missing or older than 24 hours, tell the dev before proceeding.

---

## Purpose

Local devcontainer for querying Delta tables on Azure ADLS Gen2 and iterating on dbt models before running on Databricks. No cluster needed.

## Architecture

```
Azure ADLS Gen2
      |  delta-rs (Rust TLS)
      v
.cache/  (local Parquet files, workspace-mounted, survives container rebuilds)
      |  DuckDB read_parquet()
      v
DuckDB  -->  conn.sql() in explore_sql.ipynb (ad-hoc SQL)
        -->  dbt-duckdb  -->  silver/gold tables in .cache/lakehouse_dev.duckdb
                                    |  queried in explore_dbt.ipynb
                                    v
                             dbt run --target prod  -->  Databricks Unity Catalog
```

## Key files

| File | Purpose |
|---|---|
| `lakekit/lakehouse.py` | `LakehouseClient`, `LakehouseDuckDB`, `TABLES` registry, module-level helpers |
| `lakekit/sync.py` | Incremental Parquet cache sync from ADLS |
| `lakekit/dbutils_stub.py` | Databricks `dbutils` stub, resolves secrets from env vars |
| `dbt/macros/cross_db.sql` | DuckDB vs SparkSQL syntax adapters (`json_str`, `count_if`, `avg_if`) |
| `dbt/models/staging/` | Source abstraction, the only layer that knows local vs prod |
| `dbt/models/silver/` | Transformation models (joins, JSON parsing, cleaning) |
| `dbt/models/gold/` | Aggregation models (metrics, rollups) |

## dbt local vs prod: the only difference

Staging models switch between targets using `{% if target.name == 'local' %}`:
- **local**: `read_parquet('/workspaces/your-project/local-devex/.cache/<table>/*.parquet')`
- **prod**: `{{ source('bronze_source', '<table>') }}`, resolves to Unity Catalog

All silver/gold SQL is identical across both targets. Syntax differences (JSON extraction, conditional aggregation) are handled by macros in `cross_db.sql`.

## Materialization

- `staging`: `view` (bronze schema)
- `silver`: `table`
- `gold`: `table`

## Adding a model

1. Create `dbt/models/silver/your_model.sql` using `{{ ref('stg_...') }}`
2. For DuckDB/Spark syntax differences, use macros from `cross_db.sql`
3. Run `dbt run --target local --select your_model`
4. Query: `conn.sql("SELECT * FROM silver.your_model LIMIT 10")`
5. Push: `dbt run --target prod --select your_model`

## dbt DuckDB file location

dbt writes to `.cache/lakehouse_dev.duckdb` (workspace-mounted, survives rebuilds). Do not use `/tmp/`: it is wiped on container rebuild.
