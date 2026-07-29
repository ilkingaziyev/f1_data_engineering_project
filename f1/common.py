"""
F1 Project | common.py
Shared hooks, connection checks, schema prep, Spark session helper,
and the audit log used by all pipeline stages.
"""
import logging

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from f1.config import (
    MINIO_CONN_ID, POSTGRES_CONN_ID, TARGET_SCHEMAS,
    GOLD_SCHEMA, POSTGRES_JDBC_JAR,
)

log = logging.getLogger(__name__)


# ---------------- hooks ----------------
def get_pg():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


def get_s3():
    return S3Hook(aws_conn_id=MINIO_CONN_ID)


# ---------------- connection checks ----------------
def check_minio_connection(**_):
    get_s3().get_conn().list_buckets()
    log.info("MinIO connection OK")


def check_postgres_connection(**_):
    if get_pg().get_first("SELECT 1;")[0] != 1:
        raise RuntimeError("PostgreSQL connection test failed")
    log.info("PostgreSQL connection OK")


# ---------------- schema prep ----------------
def prepare_postgres_schema(**_):
    pg = get_pg()
    for schema in TARGET_SCHEMAS:
        pg.run(f"CREATE SCHEMA IF NOT EXISTS {schema};")
    pg.run(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA}.etl_audit_log (
            id SERIAL PRIMARY KEY, dataset TEXT, layer TEXT, table_name TEXT,
            total_rows INTEGER, status TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
    """)
    log.info("Schemas ready: %s", TARGET_SCHEMAS)


def audit(pg, dataset, layer, table, rows, status="OK"):
    pg.run(f"""INSERT INTO {GOLD_SCHEMA}.etl_audit_log (dataset, layer, table_name, total_rows, status)
               VALUES (%s,%s,%s,%s,%s)""",
           parameters=(dataset, layer, table, int(rows), status))


# ---------------- Spark (local mode + Postgres JDBC driver) ----------------
def get_spark(app):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder
             .appName(app)
             .master("local[*]")
             .config("spark.jars", POSTGRES_JDBC_JAR)
             .config("spark.driver.extraClassPath", POSTGRES_JDBC_JAR)
             .config("spark.executor.extraClassPath", POSTGRES_JDBC_JAR)
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    return spark


def jdbc_params():
    """Build (url, properties) for Spark <-> Postgres over JDBC."""
    c = get_pg().get_connection(POSTGRES_CONN_ID)
    port = c.port or 5432
    db = c.schema
    if not db:
        raise ValueError("Database name missing in the Airflow Postgres connection")
    url = f"jdbc:postgresql://{c.host}:{port}/{db}"
    props = {"user": c.login, "password": c.password, "driver": "org.postgresql.Driver"}
    return url, props