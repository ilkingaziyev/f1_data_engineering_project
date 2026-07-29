import io
import json
import math
import random
import re
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from f1.common import get_s3, get_pg
from f1.config import (
    BUCKET_NAME, GOLD_SCHEMA,
    STREAM_BRONZE_PREFIX, STREAM_SILVER_PREFIX, STREAM_GOLD_PREFIX,
    STREAM_STATE_KEY, REPLAY_YEAR, ROWS_PER_BATCH_MIN, ROWS_PER_BATCH_MAX,
    OPENF1_BASE,
)

default_args = {"owner": "ilkin", "retries": 2, "retry_delay": timedelta(minutes=2)}


# ============================ helpers ============================
def upload_json(hook, key, data):
    hook.load_string(json.dumps(data, ensure_ascii=False), key=key,
                     bucket_name=BUCKET_NAME, replace=True)


def write_parquet_by_session(hook, pdf, layer_prefix, name):
    """One Parquet per session under <layer>/<name>/session=<key>/."""
    for sk, part in pdf.groupby("session_key"):
        buf = io.BytesIO()
        part.to_parquet(buf, index=False)
        key = f"{layer_prefix}/{name}/session={sk}/{name}.parquet"
        hook.load_bytes(buf.getvalue(), key=key, bucket_name=BUCKET_NAME, replace=True)
    print(f"written {name}: {len(pdf)} rows")


def read_parquet_all(hook, layer_prefix, name):
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


def read_stream_bronze(hook, dataset):
    """Merge every streaming-bronze JSON; attach session_key from the path."""
    prefix = f"{STREAM_BRONZE_PREFIX}/{dataset}/"
    keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=prefix) or []
    rows = []
    for k in keys:
        if not k.endswith(".json"):
            continue
        m = re.search(r"session=(\d+)", k)
        for row in json.loads(hook.read_key(k, bucket_name=BUCKET_NAME)):
            row["_session_key"] = m.group(1) if m else None
            rows.append(row)
    return rows


def _py(v):
    """Coerce numpy/NaN/NaT values to plain Python for Postgres inserts."""
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


def _to_num(series, kind="float"):
    s = pd.to_numeric(series, errors="coerce")
    return s.astype("Int64") if kind == "int" else s


# ============================ OpenF1 + state ============================
def openf1_get(path, params):
    query = "&".join(f"{k}={v}" for k, v in params.items())
    resp = requests.get(f"{OPENF1_BASE}/{path}?{query}", timeout=30)
    resp.raise_for_status()
    time.sleep(0.5)
    data = resp.json()
    return data if isinstance(data, list) else []


def load_state(hook):
    if hook.check_for_key(key=STREAM_STATE_KEY, bucket_name=BUCKET_NAME):
        return json.loads(hook.read_key(STREAM_STATE_KEY, bucket_name=BUCKET_NAME))
    return {}


def save_state(hook, state):
    hook.load_string(json.dumps(state), key=STREAM_STATE_KEY,
                     bucket_name=BUCKET_NAME, replace=True)


def replay_sessions(hook, state):
    if "sessions" not in state:
        sessions = openf1_get("sessions", {"year": REPLAY_YEAR, "session_name": "Race"})
        state["sessions"] = [s["session_key"] for s in sessions]
        print(f"replay pool: {len(state['sessions'])} historic race sessions")
    return state["sessions"]


def live_session_key():
    """
    OpenF1 gates real-time 'latest' session lookups behind a paid tier.
    A 401 here just means we don't have paid access to check for a live
    session - fall back to replay instead of failing the task.
    """
    try:
        sessions = openf1_get("sessions", {"session_key": "latest"})
    except requests.exceptions.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 401:
            print("live-session check returned 401 (no paid OpenF1 access) - replay mode")
            return None
        raise
    if not sessions:
        return None
    s = sessions[0]
    try:
        start = datetime.fromisoformat(s["date_start"].replace("Z", "+00:00"))
        end = datetime.fromisoformat(s["date_end"].replace("Z", "+00:00"))
    except (KeyError, ValueError):
        return None
    now = datetime.now(start.tzinfo)
    return s["session_key"] if start <= now <= end + timedelta(hours=1) else None


def replay_pool(hook, dataset, session_key):
    """Fetch a historic session's full dataset once, cache it in MinIO."""
    key = f"{STREAM_BRONZE_PREFIX}/_replay/{dataset}/session={session_key}/pool.json"
    if hook.check_for_key(key=key, bucket_name=BUCKET_NAME):
        return json.loads(hook.read_key(key, bucket_name=BUCKET_NAME))
    rows = openf1_get(dataset, {"session_key": session_key})
    upload_json(hook, key, rows)
    print(f"[{dataset}] pool fetched for session {session_key}: {len(rows)} rows")
    return rows


# ============================ bronze tasks ============================
def _extract_stream(dataset):
    """Emit one micro-batch of a dataset (laps/pit/stints); replay or live mode."""
    hook = get_s3()
    state = load_state(hook)
    dstate = state.setdefault(dataset, {"session_idx": 0, "offset": 0})
    today = datetime.utcnow().strftime("%Y-%m-%d")

    live_key = live_session_key()
    if live_key is not None and dataset == "stints":
        # Stints have no per-row timestamp - just snapshot each call.
        rows = openf1_get(dataset, {"session_key": live_key})
        now_iso = datetime.utcnow().isoformat()
        for r in rows:
            r["event_time"] = now_iso
            r["ingest_mode"] = "live"
        if rows:
            stamp = datetime.utcnow().strftime("%H%M%S")
            upload_json(hook, f"{STREAM_BRONZE_PREFIX}/{dataset}/ingest_date={today}"
                              f"/session={live_key}/live_{stamp}.json", rows)
            print(f"[{dataset}] LIVE: snapshot with {len(rows)} rows")
        else:
            print(f"[{dataset}] LIVE: no rows")
        save_state(hook, state)
        return

    if live_key is not None:
        last_ts = dstate.get("last_live_ts", "")
        ts_field = "date_start" if dataset == "laps" else "date"
        fresh = [r for r in openf1_get(dataset, {"session_key": live_key})
                 if (r.get(ts_field) or "") > last_ts]
        if fresh:
            for r in fresh:
                r["event_time"] = r.get(ts_field)
                r["ingest_mode"] = "live"
            stamp = datetime.utcnow().strftime("%H%M%S")
            upload_json(hook, f"{STREAM_BRONZE_PREFIX}/{dataset}/ingest_date={today}"
                              f"/session={live_key}/live_{stamp}.json", fresh)
            dstate["last_live_ts"] = max(r.get(ts_field) or "" for r in fresh)
            print(f"[{dataset}] LIVE: {len(fresh)} new rows")
        else:
            print(f"[{dataset}] LIVE: no new rows")
    else:
        sessions = replay_sessions(hook, state)
        if not sessions:
            save_state(hook, state)
            return
        idx = dstate["session_idx"] % len(sessions)
        sk = sessions[idx]
        pool = replay_pool(hook, dataset, sk)
        offset = dstate["offset"]
        while offset >= len(pool) and idx < len(sessions) - 1:
            idx += 1
            sk = sessions[idx]
            pool = replay_pool(hook, dataset, sk)
            offset = 0
        if offset >= len(pool):
            print(f"[{dataset}] REPLAY: all sessions consumed")
            dstate["session_idx"], dstate["offset"] = idx, offset
            save_state(hook, state)
            return
        n = random.randint(ROWS_PER_BATCH_MIN, ROWS_PER_BATCH_MAX)
        batch = pool[offset:offset + n]
        for i, r in enumerate(batch):
            r["event_time"] = (datetime.utcnow() + timedelta(seconds=i)).isoformat()
            r["ingest_mode"] = "replay"
        upload_json(hook, f"{STREAM_BRONZE_PREFIX}/{dataset}/ingest_date={today}"
                          f"/session={sk}/offset={offset:06d}.json", batch)
        dstate["session_idx"], dstate["offset"] = idx, offset + len(batch)
        print(f"[{dataset}] REPLAY: rows {offset}-{offset + len(batch) - 1} of session {sk}")

    save_state(hook, state)


def stream_extract_laps(**_):
    _extract_stream("laps")


def stream_extract_pit(**_):
    _extract_stream("pit")


def stream_extract_stints(**_):
    _extract_stream("stints")


def stream_extract_drivers(**_):
    """Driver list per session; fetched exactly once (skip-if-exists)."""
    hook = get_s3()
    lap_keys = hook.list_keys(bucket_name=BUCKET_NAME, prefix=f"{STREAM_BRONZE_PREFIX}/laps/") or []
    session_keys = sorted({m.group(1) for k in lap_keys if (m := re.search(r"session=(\d+)", k))})
    fetched = skipped = 0
    for sk in session_keys:
        key = f"{STREAM_BRONZE_PREFIX}/drivers/session={sk}/drivers.json"
        if hook.check_for_key(key=key, bucket_name=BUCKET_NAME):
            skipped += 1
            continue
        rows = openf1_get("drivers", {"session_key": sk})
        if rows:
            upload_json(hook, key, rows)
            fetched += 1
    print(f"[drivers] {fetched} fetched, {skipped} skipped")


# ============================ silver tasks (Pandas) ============================
def stream_silver_laps(**_):
    hook = get_s3()
    raw = read_stream_bronze(hook, "laps")
    if not raw:
        print("no stream laps yet")
        return
    df = pd.DataFrame([{
        "session_key": r["_session_key"],
        "driver_number": r.get("driver_number"),
        "lap_number": r.get("lap_number"),
        "lap_duration": r.get("lap_duration"),
        "sector1": r.get("duration_sector_1"),
        "sector2": r.get("duration_sector_2"),
        "sector3": r.get("duration_sector_3"),
        "speed_trap": r.get("st_speed"),
        "event_time": r.get("event_time"),
        "ingest_mode": r.get("ingest_mode"),
    } for r in raw])
    df["driver_number"] = _to_num(df["driver_number"], "int")
    df["lap_number"] = _to_num(df["lap_number"], "int")
    for c in ["lap_duration", "sector1", "sector2", "sector3", "speed_trap"]:
        df[c] = _to_num(df[c], "float")
    df = df.dropna(subset=["driver_number", "lap_number"])
    df = df.drop_duplicates(subset=["session_key", "driver_number", "lap_number"])
    write_parquet_by_session(hook, df, STREAM_SILVER_PREFIX, "laps")


def stream_silver_pit(**_):
    hook = get_s3()
    raw = read_stream_bronze(hook, "pit")
    if not raw:
        print("no stream pit yet")
        return
    df = pd.DataFrame([{
        "session_key": r["_session_key"],
        "driver_number": r.get("driver_number"),
        "lap_number": r.get("lap_number"),
        "pit_duration": r.get("pit_duration"),
        "event_time": r.get("event_time"),
        "ingest_mode": r.get("ingest_mode"),
    } for r in raw])
    df["driver_number"] = _to_num(df["driver_number"], "int")
    df["lap_number"] = _to_num(df["lap_number"], "int")
    df["pit_duration"] = _to_num(df["pit_duration"], "float")
    df = df.dropna(subset=["driver_number"])
    df = df.drop_duplicates(subset=["session_key", "driver_number", "lap_number"])
    write_parquet_by_session(hook, df, STREAM_SILVER_PREFIX, "pit")


def stream_silver_stints(**_):
    hook = get_s3()
    raw = read_stream_bronze(hook, "stints")
    if not raw:
        print("no stream stints yet")
        return
    df = pd.DataFrame([{
        "session_key": r["_session_key"],
        "driver_number": r.get("driver_number"),
        "stint_number": r.get("stint_number"),
        "compound": r.get("compound"),
        "lap_start": r.get("lap_start"),
        "lap_end": r.get("lap_end"),
        "tyre_age_at_start": r.get("tyre_age_at_start"),
        "event_time": r.get("event_time"),
        "ingest_mode": r.get("ingest_mode"),
    } for r in raw])
    for c in ["driver_number", "stint_number", "lap_start", "lap_end", "tyre_age_at_start"]:
        df[c] = _to_num(df[c], "int")
    df = df.dropna(subset=["driver_number", "stint_number"])
    df = df.drop_duplicates(subset=["session_key", "driver_number", "stint_number"])
    write_parquet_by_session(hook, df, STREAM_SILVER_PREFIX, "stints")


def stream_silver_drivers(**_):
    hook = get_s3()
    raw = read_stream_bronze(hook, "drivers")
    if not raw:
        print("no stream drivers yet")
        return
    df = pd.DataFrame([{
        "session_key": r["_session_key"],
        "driver_number": r.get("driver_number"),
        "full_name": r.get("full_name"),
        "name_acronym": r.get("name_acronym"),
        "team_name": r.get("team_name"),
    } for r in raw])
    df["driver_number"] = _to_num(df["driver_number"], "int")
    df = df.dropna(subset=["driver_number"])
    df = df.drop_duplicates(subset=["session_key", "driver_number"])
    write_parquet_by_session(hook, df, STREAM_SILVER_PREFIX, "drivers")


# ============================ gold + serving ============================
def stream_gold_lap_stats(**_):
    """Per driver per session: lap stats plus current tyre strategy."""
    hook = get_s3()
    laps = read_parquet_all(hook, STREAM_SILVER_PREFIX, "laps")
    drivers = read_parquet_all(hook, STREAM_SILVER_PREFIX, "drivers")
    stats = laps.dropna(subset=["lap_duration"]).groupby(
        ["session_key", "driver_number"], as_index=False).agg(
        laps_completed=("lap_number", "nunique"),
        best_lap=("lap_duration", "min"),
        avg_lap=("lap_duration", "mean"),
        top_speed=("speed_trap", "max"))
    stats["avg_lap"] = stats["avg_lap"].round(3)
    stats = stats.merge(
        drivers[["session_key", "driver_number", "full_name", "team_name"]],
        on=["session_key", "driver_number"], how="left")

    try:
        stints = read_parquet_all(hook, STREAM_SILVER_PREFIX, "stints")
        total_stints = stints.groupby(["session_key", "driver_number"], as_index=False).agg(
            total_stints=("stint_number", "nunique"))
        latest_idx = stints.groupby(["session_key", "driver_number"])["stint_number"].idxmax()
        current = stints.loc[latest_idx, ["session_key", "driver_number", "compound"]]
        current = current.rename(columns={"compound": "current_compound"})
        stats = stats.merge(total_stints, on=["session_key", "driver_number"], how="left")
        stats = stats.merge(current, on=["session_key", "driver_number"], how="left")
    except FileNotFoundError:
        print("[lap_stats] no stints data yet, skipping tyre columns")
        stats["total_stints"] = pd.NA
        stats["current_compound"] = pd.NA

    write_parquet_by_session(hook, stats, STREAM_GOLD_PREFIX, "lap_stats")


def load_postgres_stream_live(**_):
    """UPSERT row-level laps: the near-real-time table."""
    hook, pg = get_s3(), get_pg()
    laps = read_parquet_all(hook, STREAM_SILVER_PREFIX, "laps")
    table = f"{GOLD_SCHEMA}.stream_laps_live"
    pg.run(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    pg.run(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            session_key TEXT, driver_number INTEGER, lap_number INTEGER,
            lap_duration DOUBLE PRECISION, sector1 DOUBLE PRECISION,
            sector2 DOUBLE PRECISION, sector3 DOUBLE PRECISION,
            speed_trap DOUBLE PRECISION, event_time TEXT, ingest_mode TEXT,
            PRIMARY KEY (session_key, driver_number, lap_number))""")
    columns = ["session_key", "driver_number", "lap_number", "lap_duration",
               "sector1", "sector2", "sector3", "speed_trap", "event_time", "ingest_mode"]
    rows = [tuple(_py(v) for v in row) for row in laps[columns].itertuples(index=False, name=None)]
    pg.insert_rows(table=table, rows=rows, target_fields=columns,
                   replace=True, replace_index=["session_key", "driver_number", "lap_number"])
    print(f"upserted {len(rows)} rows into {table}")


def load_postgres_stream_stats(**_):
    """Full refresh of the aggregated per-driver session stats."""
    hook, pg = get_s3(), get_pg()
    stats = read_parquet_all(hook, STREAM_GOLD_PREFIX, "lap_stats")
    table = f"{GOLD_SCHEMA}.stream_driver_lap_stats"
    pg.run(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")
    pg.run(f"DROP TABLE IF EXISTS {table}")
    pg.run(f"""
        CREATE TABLE {table} (
            session_key TEXT, driver_number INTEGER, full_name TEXT,
            team_name TEXT, laps_completed INTEGER,
            best_lap DOUBLE PRECISION, avg_lap DOUBLE PRECISION,
            top_speed DOUBLE PRECISION, total_stints INTEGER,
            current_compound TEXT,
            PRIMARY KEY (session_key, driver_number))""")
    columns = ["session_key", "driver_number", "full_name", "team_name",
               "laps_completed", "best_lap", "avg_lap", "top_speed",
               "total_stints", "current_compound"]
    for c in columns:
        if c not in stats.columns:
            stats[c] = None
    rows = [tuple(_py(v) for v in row) for row in stats[columns].itertuples(index=False, name=None)]
    pg.insert_rows(table=table, rows=rows, target_fields=columns)
    print(f"loaded {len(rows)} rows into {table}")


# ============================ DAG ============================
with DAG(
    dag_id="f1_api_streaming_pipeline_main",
    description="OpenF1 (replay/live) -> Bronze -> Silver(Pandas) -> Gold -> Postgres, every 5 min",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["f1", "streaming"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    with TaskGroup(group_id="bronze_ingestion") as bronze_ingestion:
        e_laps = PythonOperator(task_id="stream_extract_laps", python_callable=stream_extract_laps)
        e_pit = PythonOperator(task_id="stream_extract_pit", python_callable=stream_extract_pit)
        e_stints = PythonOperator(task_id="stream_extract_stints", python_callable=stream_extract_stints)
        e_drivers = PythonOperator(task_id="stream_extract_drivers", python_callable=stream_extract_drivers)
        e_laps >> e_drivers

    with TaskGroup(group_id="silver_processing") as silver_processing:
        s_laps = PythonOperator(task_id="stream_silver_laps", python_callable=stream_silver_laps)
        s_pit = PythonOperator(task_id="stream_silver_pit", python_callable=stream_silver_pit)
        s_stints = PythonOperator(task_id="stream_silver_stints", python_callable=stream_silver_stints)
        s_drivers = PythonOperator(task_id="stream_silver_drivers", python_callable=stream_silver_drivers)

    with TaskGroup(group_id="gold_processing") as gold_processing:
        g_stats = PythonOperator(task_id="stream_gold_lap_stats", python_callable=stream_gold_lap_stats)

    with TaskGroup(group_id="postgres_loading") as postgres_loading:
        p_live = PythonOperator(task_id="load_postgres_stream_live", python_callable=load_postgres_stream_live)
        p_stats = PythonOperator(task_id="load_postgres_stream_stats", python_callable=load_postgres_stream_stats)

    start >> bronze_ingestion

    e_laps >> s_laps
    e_pit >> s_pit
    e_stints >> s_stints
    e_drivers >> s_drivers

    [s_laps, s_drivers, s_stints] >> g_stats
    s_laps >> p_live
    g_stats >> p_stats

    postgres_loading >> end