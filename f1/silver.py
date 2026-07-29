"""
F1 Project | silver.py
Generic Bronze -> Silver using PySpark over JDBC (Postgres -> Postgres).
Cleaning is dispatched per dataset via f1.cleaning.CLEANERS.
"""
import logging

from f1.common import get_spark, jdbc_params, get_pg, audit
from f1.cleaning import CLEANERS

log = logging.getLogger(__name__)


def load_to_silver(dataset, bronze_schema, bronze_table, silver_schema, silver_table, **_):
    spark = None
    try:
        spark = get_spark(f"F1_Silver_{dataset}")
        url, props = jdbc_params()
        bronze_fqn = f"{bronze_schema}.{bronze_table}"
        silver_fqn = f"{silver_schema}.{silver_table}"

        log.info("Reading bronze via Spark JDBC: %s", bronze_fqn)
        bronze_df = spark.read.jdbc(url=url, table=bronze_fqn, properties=props)
        if bronze_df.count() == 0:
            log.warning("Bronze %s empty; skipping silver", bronze_fqn)
            return

        silver_df = CLEANERS[dataset](bronze_df)
        n = silver_df.count()
        log.info("Writing %d cleaned rows to %s", n, silver_fqn)
        # overwrite WITHOUT truncate -> Spark drops & recreates the table with
        # column types that match the cleaned DataFrame (avoids type mismatch
        # against a table left over from an earlier run).
        (silver_df.write
            .mode("overwrite")
            .jdbc(url=url, table=silver_fqn, properties=props))

        audit(get_pg(), dataset, "silver", silver_table, n)
        log.info("Silver done: %s (%d rows)", silver_fqn, n)
    except Exception:
        log.exception("Silver failed for %s", dataset)
        raise
    finally:
        if spark is not None:
            spark.stop()
            log.info("SparkSession stopped")