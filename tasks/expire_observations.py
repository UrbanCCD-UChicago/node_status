#!/usr/bin/env python3

import psycopg2
from logging2 import Logger, LogLevel

from app import app
from config import Config


@app.task
def expire_observations():
    _cfg = Config()

    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    logger = Logger("expire_observations", level=_cfg.LL)

    logger.info("pruning observations older than 1 hour")
    cursor.execute(
        """
        DELETE FROM observations
        WHERE timestamp < (now() - interval '1 hour')
        """)

    conn.commit()
    cursor.close()
    conn.close()