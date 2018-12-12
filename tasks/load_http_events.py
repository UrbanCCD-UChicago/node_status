#!/usr/bin/env python3

import codecs
import csv
import os
import os.path
import shutil

import psycopg2
import requests
from logging2 import Logger, LogLevel

from app import app
from config import Config


@app.task
def load_http_events():
    _cfg = Config()

    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    logger = Logger("load_http_events", level=_cfg.LL)

    basedir = os.path.join(os.path.dirname(__file__), "downloads")
    if not os.path.exists(basedir):
        os.mkdir(basedir)

    filename = os.path.join(basedir, "http-events.csv")
    if os.path.exists(filename):
        os.remove(filename)

    logger.info("downloading recent http events")
    logger.debug(f"downloading to {filename}")

    res = requests.get("https://www.mcs.anl.gov/research/projects/waggle/downloads/status/http-events.csv", stream=True)
    with open(filename, mode="wb") as fh:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                fh.write(chunk)

    logger.info(f"ripping {filename}")
    with codecs.open(filename, mode="r", encoding="utf8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            logger.debug(f"{row}")
            try:
                cursor.execute(
                    """
                    INSERT INTO node_http_events (node_id, timestamp, boot_id, boot_media)
                    VALUES (%(node_id)s, %(timestamp)s, %(boot_id)s, %(boot_media)s)
                    ON CONFLICT (node_id)
                    DO UPDATE SET
                        timestamp   = EXCLUDED.timestamp,
                        boot_id     = EXCLUDED.boot_id,
                        boot_media  = EXCLUDED.boot_media
                    """, row)
            except Exception as e:
                logger.error(f"tried upserting data, got error: {e}")
        conn.commit()

    logger.info("cleaning up")
    cursor.close()
    conn.close()
    shutil.rmtree(basedir)
