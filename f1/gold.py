"""
F1 Project | gold.py
Silver -> Gold business marts, built with SQL directly inside Postgres.
Unlike bronze/silver (one table per dataset), gold tables here join
multiple silver tables into cross-dataset business marts.
"""
import logging

from f1.common import get_pg, audit
from f1.config import SILVER_SCHEMA, GOLD_SCHEMA

log = logging.getLogger(__name__)


def build_driver_standings(**_):
    pg = get_pg()
    s, g = SILVER_SCHEMA, GOLD_SCHEMA
    pg.run(f"""
        DROP TABLE IF EXISTS {g}.driver_standings;
        CREATE TABLE {g}.driver_standings AS
        SELECT r.driver_id, d.driver_name,
               SUM(r.points) AS points,
               COUNT(*) FILTER (WHERE r.position = 1) AS wins,
               COUNT(*) FILTER (WHERE r.position <= 3) AS podiums,
               COUNT(*) FILTER (WHERE r.status <> 'Finished') AS dnf_count,
               ROUND(AVG(r.position)::numeric, 2) AS avg_finish_position,
               ROW_NUMBER() OVER (ORDER BY SUM(r.points) DESC) AS rank
        FROM {s}.results r
        JOIN {s}.drivers d ON d.driver_id = r.driver_id
        GROUP BY r.driver_id, d.driver_name
        ORDER BY points DESC;
    """)
    n = pg.get_first(f"SELECT COUNT(*) FROM {g}.driver_standings")[0]
    audit(pg, "drivers", "gold", "driver_standings", n)
    log.info("Gold %s.driver_standings: %d rows", g, n)


def build_constructor_standings(**_):
    pg = get_pg()
    s, g = SILVER_SCHEMA, GOLD_SCHEMA
    pg.run(f"""
        DROP TABLE IF EXISTS {g}.constructor_standings;
        CREATE TABLE {g}.constructor_standings AS
        SELECT r.constructor_id, c.constructor_name,
               SUM(r.points) AS points,
               COUNT(*) FILTER (WHERE r.position = 1) AS wins
        FROM {s}.results r
        JOIN {s}.constructors c ON c.constructor_id = r.constructor_id
        GROUP BY r.constructor_id, c.constructor_name
        ORDER BY points DESC;
    """)
    n = pg.get_first(f"SELECT COUNT(*) FROM {g}.constructor_standings")[0]
    audit(pg, "constructors", "gold", "constructor_standings", n)
    log.info("Gold %s.constructor_standings: %d rows", g, n)


def build_driver_performance(**_):
    pg = get_pg()
    s, g = SILVER_SCHEMA, GOLD_SCHEMA
    pg.run(f"""
        DROP TABLE IF EXISTS {g}.driver_performance;
        CREATE TABLE {g}.driver_performance AS
        SELECT driver_id,
               ROUND(AVG(position)::numeric, 2) AS avg_finish_position,
               COUNT(*) FILTER (WHERE status <> 'Finished') AS dnf_count,
               COUNT(DISTINCT race_id) AS races_participated,
               ROUND(AVG(grid - position)::numeric, 2) AS avg_positions_gained,
               MAX(grid - position) AS best_gain
        FROM {s}.results
        WHERE position IS NOT NULL AND grid IS NOT NULL
        GROUP BY driver_id;
    """)
    n = pg.get_first(f"SELECT COUNT(*) FROM {g}.driver_performance")[0]
    audit(pg, "drivers", "gold", "driver_performance", n)
    log.info("Gold %s.driver_performance: %d rows", g, n)