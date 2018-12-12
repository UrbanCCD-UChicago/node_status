#!/usr/bin/env python3

import psycopg2
from logging2 import Logger, LogLevel

from app import app
from config import Config


@app.task
def expire_node_last_hour():
    _cfg = Config()

    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    logger = Logger("expire_node_last_hour", level=_cfg.LL)

    logger.info("gathering latest observation timestamps")
    cursor.execute("SELECT node_id, timestamp FROM latest_observation_per_node")
    rows = cursor.fetchall()

    logger.info("pruning old observations from node_last_hour")
    for (node_id, timestamp) in rows:
        logger.debug(f"node_id={node_id}  timestamp={timestamp}")
        cursor.execute(
            """
            DELETE FROM node_last_hour
            WHERE node_id = %s
            AND timestamp < (%s - interval '1 hour')
            """, 
            (node_id, timestamp))

    conn.commit()
    cursor.close()
    conn.close()
