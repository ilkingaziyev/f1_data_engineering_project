# F1 Data Engineering Pipeline

End-to-end data engineering platform for Formula 1 data — combining a batch (historical season) branch and a streaming (live race) branch through a shared medallion architecture (Bronze → Silver → Gold).

## Architecture

```
Jolpica API ─────▶ Bronze (Postgres) ─▶ Silver (Spark, local mode) ─▶ Gold (SQL)
                                                                          │
OpenF1 API  ─────▶ Bronze (MinIO)   ─▶ Silver (Pandas)     ─▶ Gold (MinIO+PG)
                                                                          │
                                                                          ▼
                                                              driver_360 (Consolidation)
```

## DAGs

| DAG | Purpose |
|---|---|
| `f1_api_static_pipeline_main` | Batch: Jolpica API → Bronze → Spark Silver → SQL Gold |
| `f1_api_streaming_pipeline_main` | Streaming: OpenF1 API → Bronze → Pandas Silver → Gold |
| `f1_api_gold_consolidation` | Joins static + streaming Gold outputs into `driver_360` |

## Package Structure

```
dags/
├── f1_api_static_pipeline_main.py
├── f1_api_streaming_pipeline_main.py
├── f1_api_gold_consolidation.py
└── f1/
    ├── config.py     — central configuration (schemas, MinIO paths, API URLs)
    ├── common.py     — hooks, Spark session helper, audit log
    ├── bronze.py     — Jolpica API extraction + MinIO/Postgres loading
    ├── cleaning.py   — PySpark cleaning functions per dataset
    ├── silver.py     — generic Spark JDBC Bronze→Silver dispatcher
    └── gold.py       — SQL business marts (driver_standings, etc.)
```

## Tech Stack

Apache Airflow · Apache Spark (local mode) · Pandas · MinIO (S3) · PostgreSQL

## Data Sources

- **[Jolpica API](https://github.com/jolpica/jolpica-f1)** — Ergast-compatible historical F1 data
- **[OpenF1 API](https://openf1.org/)** — live and historical session telemetry

## Setup

1. Configure Airflow Connections: `minio_conn` (S3), `postgres_conn` (Postgres)
2. Place the Postgres JDBC driver at the path set in `f1/config.py` (`POSTGRES_JDBC_JAR`)
3. Ensure `pyspark` and Java are available in the Airflow worker environment
4. Drop the `dags/` contents into your Airflow `dags/` folder

## Notes

- Spark runs in `local[*]` mode directly inside the PythonOperator process — no `spark-submit` or external cluster required.
- The streaming branch intentionally uses Pandas rather than Spark: at a 5-minute cadence, Spark's JVM startup cost outweighs the benefit for small (80-150 row) batches.
