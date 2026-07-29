"""
F1 Project | config.py
Central configuration (connections, storage, schemas, tables).
"""

# ---------------- CONNECTIONS ----------------
MINIO_CONN_ID = "minio_conn"
POSTGRES_CONN_ID = "postgres_conn"

# ---------------- STORAGE (MinIO) - TEST NAMESPACE ----------------
BUCKET_NAME = "ilkin"
BRONZE_PREFIX = "F1/F1_bronze/"

# Multi-dataset streaming layout (laps, pit, stints, drivers)
STREAM_BRONZE_PREFIX = "F1/F1_streaming_bronze"
STREAM_SILVER_PREFIX = "F1/F1_streaming_silver"
STREAM_GOLD_PREFIX = "F1/F1_streaming_gold"
STREAM_STATE_KEY = f"{STREAM_BRONZE_PREFIX}/_state/state.json"
REPLAY_YEAR = 2025
ROWS_PER_BATCH_MIN = 80
ROWS_PER_BATCH_MAX = 150

# Optional MinIO Parquet mirror of the static Gold marts, used by the
# consolidation DAG to write driver_360 alongside its Postgres copy.
GOLD_PARQUET_PREFIX = "F1/F1_gold"

# ---------------- POSTGRES SCHEMAS (medallion) - TEST NAMESPACE ----------------
BRONZE_SCHEMA = "f1_bronze_main"
SILVER_SCHEMA = "f1_silver_main"
GOLD_SCHEMA = "f1_schema_main"
TARGET_SCHEMAS = [BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA]

# ---------------- SPARK ----------------
POSTGRES_JDBC_JAR = "/opt/spark/jars/postgresql-42.7.3.jar"

# ---------------- API SOURCES ----------------
# Real Jolpica F1 API base (Ergast-compatible). NOTE: api.jolpica.com does
# NOT exist - the actual domain is api.jolpi.ca.
JOLPICA_BASE = "https://api.jolpi.ca/ergast/f1"
JOLPICA_SEASON = "2025"

OPENF1_BASE = "https://api.openf1.org/v1"

# ---------------- TABLES (bronze / silver per dataset) ----------------
# Each dataset flows: MinIO raw -> bronze -> silver (all Postgres).
# Gold tables are cross-dataset business marts, built separately in gold.py.
TABLES = {
    "drivers":      {"bronze": "drivers",      "silver": "drivers"},
    "constructors": {"bronze": "constructors", "silver": "constructors"},
    "races":        {"bronze": "races",        "silver": "races"},
    "results":      {"bronze": "results",      "silver": "results"},
}