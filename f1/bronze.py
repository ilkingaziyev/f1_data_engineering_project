import json
import logging

import pandas as pd
import requests

from f1.common import get_s3, get_pg, audit
from f1.config import BUCKET_NAME, BRONZE_PREFIX, BRONZE_SCHEMA, JOLPICA_BASE, JOLPICA_SEASON, TABLES

log = logging.getLogger(__name__)


def _fetch_paginated(url, table_key, list_key, limit=100):
    offset = 0
    total = None
    items = []
    while total is None or offset < total:
        resp = requests.get(url, params={"limit": limit, "offset": offset}, timeout=30)
        resp.raise_for_status()
        mrdata = resp.json().get("MRData", {})
        total = int(mrdata.get("total", 0))
        page_items = mrdata.get(table_key, {}).get(list_key, [])
        if not page_items:
            break
        items.extend(page_items)
        offset += limit
    return items


def _fetch_results_flat(season, limit=100):
    offset = 0
    total = None
    rows = []
    while total is None or offset < total:
        resp = requests.get(
            f"{JOLPICA_BASE}/{season}/results.json",
            params={"limit": limit, "offset": offset}, timeout=30,
        )
        resp.raise_for_status()
        mrdata = resp.json().get("MRData", {})
        total = int(mrdata.get("total", 0))
        races = mrdata.get("RaceTable", {}).get("Races", [])
        if not races:
            break
        for r in races:
            race_id = f"{r.get('season')}_{r.get('round')}"
            for res in r.get("Results", []):
                rows.append({
                    "race_id": race_id,
                    "driver_id": (res.get("Driver") or {}).get("driverId"),
                    "constructor_id": (res.get("Constructor") or {}).get("constructorId"),
                    "grid": res.get("grid"),
                    "position": res.get("position"),
                    "points": res.get("points"),
                    "status": res.get("status"),
                })
        offset += limit
    return rows


def upload_bronze_to_minio(**_):
    s3 = get_s3()
    season = JOLPICA_SEASON

    drivers_raw = _fetch_paginated(f"{JOLPICA_BASE}/{season}/drivers.json", "DriverTable", "Drivers")
    drivers = [{
        "driver_id": d.get("driverId"),
        "given_name": d.get("givenName"),
        "family_name": d.get("familyName"),
        "code": d.get("code"),
        "number": d.get("permanentNumber"),
        "date_of_birth": d.get("dateOfBirth"),
        "nationality": d.get("nationality"),
    } for d in drivers_raw]

    constructors_raw = _fetch_paginated(f"{JOLPICA_BASE}/{season}/constructors.json",
                                         "ConstructorTable", "Constructors")
    constructors = [{
        "constructor_id": c.get("constructorId"),
        "name": c.get("name"),
        "nationality": c.get("nationality"),
    } for c in constructors_raw]

    races_raw = _fetch_paginated(f"{JOLPICA_BASE}/{season}.json", "RaceTable", "Races")
    races = [{
        "race_id": f"{r.get('season')}_{r.get('round')}",
        "season": r.get("season"),
        "round": r.get("round"),
        "race_name": r.get("raceName"),
        "circuit_name": (r.get("Circuit") or {}).get("circuitName"),
        "date": r.get("date"),
    } for r in races_raw]

    results = _fetch_results_flat(season)

    datasets = {"drivers": drivers, "constructors": constructors, "races": races, "results": results}
    for name, rows in datasets.items():
        s3.load_string(
            json.dumps(rows, default=str),
            key=f"{BRONZE_PREFIX}{name}.json",
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        log.info("Bronze written: %s%s.json (%d rows)", BRONZE_PREFIX, name, len(rows))


def load_bronze_to_postgres(dataset: str, **_):
    pg = get_pg()
    s3 = get_s3()
    key = f"{BRONZE_PREFIX}{dataset}.json"
    body = s3.read_key(key, bucket_name=BUCKET_NAME)
    df = pd.DataFrame(json.loads(body)).astype(object).where(lambda x: x.notna(), None)

    tbl = TABLES[dataset]["bronze"]
    df.to_sql(tbl, pg.get_sqlalchemy_engine(), schema=BRONZE_SCHEMA,
              if_exists="replace", index=False, method="multi", chunksize=1000)
    audit(pg, dataset, "bronze", tbl, len(df))
    log.info("bronze %s.%s: %d rows", BRONZE_SCHEMA, tbl, len(df))