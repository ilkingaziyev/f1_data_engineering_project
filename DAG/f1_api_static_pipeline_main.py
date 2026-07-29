from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import cross_downstream
from airflow.utils.trigger_rule import TriggerRule

from f1.config import TABLES, BRONZE_SCHEMA, SILVER_SCHEMA
from f1.common import check_minio_connection, check_postgres_connection, prepare_postgres_schema
from f1.bronze import upload_bronze_to_minio, load_bronze_to_postgres
from f1.silver import load_to_silver
from f1.gold import build_driver_standings, build_constructor_standings, build_driver_performance

default_args = {"owner": "ilkin", "retries": 2, "retry_delay": timedelta(minutes=2)}

with DAG(
    dag_id="f1_api_static_pipeline_main",
    default_args=default_args,
    description="F1 medallion pipeline (MinIO + PySpark + Postgres), modular, batch only",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["f1", "minio", "pyspark", "postgres", "medallion"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    check_minio = PythonOperator(task_id="check_minio", python_callable=check_minio_connection)
    check_postgres = PythonOperator(task_id="check_postgres", python_callable=check_postgres_connection)
    prepare_schema = PythonOperator(task_id="prepare_schema", python_callable=prepare_postgres_schema)

    upload_bronze = PythonOperator(task_id="upload_bronze_to_minio", python_callable=upload_bronze_to_minio)

    silver_tasks = []

    def make_bronze_silver(ds):
        t = TABLES[ds]
        bronze = PythonOperator(
            task_id=f"load_bronze_{ds}", python_callable=load_bronze_to_postgres,
            op_kwargs={"dataset": ds})
        silver = PythonOperator(
            task_id=f"silver_{ds}", python_callable=load_to_silver,
            op_kwargs={"dataset": ds, "bronze_schema": BRONZE_SCHEMA, "bronze_table": t["bronze"],
                       "silver_schema": SILVER_SCHEMA, "silver_table": t["silver"]})
        bronze >> silver
        silver_tasks.append(silver)
        return bronze

    b_drivers = make_bronze_silver("drivers")
    b_constructors = make_bronze_silver("constructors")
    b_races = make_bronze_silver("races")
    b_results = make_bronze_silver("results")

    gold_standings = PythonOperator(task_id="build_driver_standings", python_callable=build_driver_standings)
    gold_constructors = PythonOperator(task_id="build_constructor_standings",
                                        python_callable=build_constructor_standings)
    gold_performance = PythonOperator(task_id="build_driver_performance", python_callable=build_driver_performance)

    # ---- wiring ----
    start >> [check_minio, check_postgres]
    [check_minio, check_postgres] >> prepare_schema >> upload_bronze
    upload_bronze >> [b_drivers, b_constructors, b_races, b_results]

    # NOTE: list >> list is not supported by Airflow's >> operator.
    # cross_downstream connects every item in the first list to every
    # item in the second list.
    cross_downstream(silver_tasks, [gold_standings, gold_constructors, gold_performance])
    [gold_standings, gold_constructors, gold_performance] >> end