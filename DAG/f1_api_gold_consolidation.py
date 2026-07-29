import math
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

from f1.common import get_s3, get_pg
from f1.config import GOLD_SCHEMA, GOLD_PARQUET_PREFIX, STREAM_GOLD_PREFIX, BUCKET_NAME

default_args = {"owner": "ilkin", "retries": 2, "retry_delay": timedelta(minutes=3)}


# ============================ helpers ============================
def read_parquet_all(hook, layer_prefix, name):
    import io
    prefix = f"{layer_prefix}/{name}/"
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix) or []
    frames = []
    for k in keys:
        if k.endswith(".parquet"):
            obj = hook.get_key(key=k, bucket_name=BUCKET_NAME)
            frames.append(pd.read_parquet(io.BytesIO(obj.get()["Body"].read())))
    if not frames:
        raise FileNotFoundError(f"no parquet under {prefix}")
    return pd.concat(frames, ignore_index=True)


def write_parquet(hook, pdf, layer_prefix, name):
    import io
    buf = io.BytesIO()
    pdf.to_parquet(buf, index=False)
    key = f"{layer_prefix}/{name}/{name}.parquet"
    hook.load_bytes(buf.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
    print(f"written s3://{BUCKET_NAME}/{key} ({len(pdf)} rows)")


def _py(v):
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(v, "item"):
        try:
            v = v.item()
        except Exception:
            pass
    if isinstance(v, float):
        if math.isnan(v):
            return None
        if v.is_integer():
            return int(v)
    return v


def load_df_to_pg(pg, df, table, ddl, columns):
    pg.run(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    pg.run(f"DROP TABLE IF EXISTS {table}")
    pg.run(ddl)
    rows = [tuple(_py(v) for v in row) for row in df[columns].itertuples(index=False, name=None)]
    pg.insert_rows(table=table, rows=rows, target_fields=columns)
    print(f"loaded {len(rows)} rows into {table}")


def _safe_parquet(hook, prefix, name, cols):
    """Read a parquet dataset; return an empty frame with cols if missing."""
    try:
        return read_parquet_all(hook, prefix, name)
    except Exception as e:
        print(f"[warn] {name} not available yet ({e}); using empty frame")
        return pd.DataFrame(columns=cols)


# ============================ task ============================
def build_driver_360(**_):
    """Join static standings/performance (Postgres) with streaming lap
    stats (MinIO Parquet) per driver, keyed on lowercase last name."""
    hook = get_s3()
    pg = get_pg()

    standings = pd.read_sql(f"SELECT * FROM {GOLD_SCHEMA}.driver_standings",
                             pg.get_sqlalchemy_engine())
    perf = pd.read_sql(f"SELECT driver_id, avg_positions_gained, best_gain "
                       f"FROM {GOLD_SCHEMA}.driver_performance",
                       pg.get_sqlalchemy_engine())
    stream = _safe_parquet(hook, STREAM_GOLD_PREFIX, "lap_stats",
                           ["full_name", "team_name", "laps_completed",
                            "best_lap", "avg_lap", "top_speed"])

    if len(stream):
        stream = stream.dropna(subset=["full_name"]).copy()
        stream["name_key"] = stream["full_name"].str.strip().str.split().str[-1].str.lower()
        stream_agg = stream.groupby("name_key", as_index=False).agg(
            stream_laps=("laps_completed", "sum"),
            stream_best_lap=("best_lap", "min"),
            stream_avg_lap=("avg_lap", "mean"),
            stream_top_speed=("top_speed", "max"),
        )
        stream_agg["stream_avg_lap"] = stream_agg["stream_avg_lap"].round(3)
    else:
        stream_agg = pd.DataFrame(columns=["name_key", "stream_laps", "stream_best_lap",
                                            "stream_avg_lap", "stream_top_speed"])

    df = standings.copy()
    df["name_key"] = df["driver_name"].str.strip().str.split().str[-1].str.lower()
    df = df.merge(perf, on="driver_id", how="left")
    df = df.merge(stream_agg, on="name_key", how="left")

    keep = ["driver_id", "driver_name", "points", "wins", "podiums", "dnf_count",
            "avg_finish_position", "rank", "avg_positions_gained", "best_gain",
            "stream_laps", "stream_best_lap", "stream_avg_lap", "stream_top_speed"]
    driver_360 = df[keep].sort_values("rank")

    write_parquet(hook, driver_360, GOLD_PARQUET_PREFIX, "driver_360")

    load_df_to_pg(pg, driver_360, f"{GOLD_SCHEMA}.driver_360", f"""
        CREATE TABLE {GOLD_SCHEMA}.driver_360 (
            driver_id TEXT PRIMARY KEY, driver_name TEXT,
            points DOUBLE PRECISION, wins INTEGER, podiums INTEGER, dnf_count INTEGER,
            avg_finish_position DOUBLE PRECISION, rank INTEGER,
            avg_positions_gained DOUBLE PRECISION, best_gain DOUBLE PRECISION,
            stream_laps INTEGER, stream_best_lap DOUBLE PRECISION,
            stream_avg_lap DOUBLE PRECISION, stream_top_speed DOUBLE PRECISION)""", keep)


with DAG(
    dag_id="f1_api_gold_consolidation",
    description="Join static + streaming Gold -> driver_360 (Parquet + Postgres)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="30 6 * * *",   # manual trigger during testing; switch to "30 6 * * *" once verified
    catchup=False,
    max_active_runs=1,
    tags=["f1", "gold", "marts"],
) as dag:
    PythonOperator(task_id="build_driver_360", python_callable=build_driver_360)