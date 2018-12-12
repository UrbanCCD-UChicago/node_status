#!/usr/bin/env python3

import codecs
import csv
import gzip
import os
import os.path
import shutil
import tarfile

import psycopg2
import requests
from logging2 import Logger, LogLevel

from app import app
from config import Config


@app.task
def load_data():
    _cfg = Config()

    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    logger = Logger("load_data", level=_cfg.LL)

    basedir = os.path.join(os.path.dirname(__file__), "downloads")
    if not os.path.exists(basedir):
        os.mkdir(basedir)

    filename = os.path.join(basedir, "AoT_Chicago.complete.recent.tar")
    if os.path.exists(filename):
        os.remove(filename)

    logger.info("downloading recent tarball")
    logger.debug(f"downloading to {filename}")
    res = requests.get("https://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/AoT_Chicago.complete.recent.tar", stream=True)
    with open(filename, mode="wb") as fh:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                fh.write(chunk)

    logger.info("decompressing tarball")
    tarball = tarfile.open(filename)
    datadirname = tarball.getnames()[0]
    logger.debug(f"datadirname: {datadirname}")
    tarball.extractall(path=basedir)
    tarball.close()
    os.remove(filename)

    dirname = os.path.join(basedir, datadirname)
    logger.debug(f"recent data dirname: {dirname}")

    nodes_filename = os.path.join(dirname, "nodes.csv")
    logger.info(f"ripping {nodes_filename}")
    with codecs.open(nodes_filename, mode="r", encoding="utf8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row["end_timestamp"] == "": row["end_timestamp"] = None
            logger.debug(f"{row}")
            cursor.execute(
                """
                INSERT INTO nodes (node_id, project_id, vsn, address, lat, lon, description, start_timestamp, end_timestamp)
                VALUES (%(node_id)s, %(project_id)s, %(vsn)s, %(address)s, %(lat)s, %(lon)s, %(description)s, %(start_timestamp)s, %(end_timestamp)s)
                ON CONFLICT (node_id)
                DO UPDATE SET
                    project_id =      EXCLUDED.project_id,
                    vsn =             EXCLUDED.vsn,
                    address =         EXCLUDED.address,
                    lat =             EXCLUDED.lat,
                    lon =             EXCLUDED.lon,
                    description =     EXCLUDED.description,
                    start_timestamp = EXCLUDED.start_timestamp,
                    end_timestamp =   EXCLUDED.end_timestamp
                """, row)
        conn.commit()

    datagz_filename = os.path.join(dirname, "data.csv.gz")
    data_filename = os.path.join(dirname, "data.csv")
    logger.debug(f"gunzipping {datagz_filename}")
    with gzip.open(datagz_filename) as ifh:
        with open(data_filename, mode="wb") as ofh:
            shutil.copyfileobj(ifh, ofh)

    logger.info(f"ripping {data_filename}")
    with codecs.open(data_filename, mode="r", encoding="utf8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            logger.debug(f"{row}")
            try:
                cursor.execute(
                    """
                    INSERT INTO observations (node_id, timestamp, subsystem, sensor, parameter, value_raw, value_hrf)
                    VALUES (%(node_id)s, %(timestamp)s, %(subsystem)s, %(sensor)s, %(parameter)s, %(value_raw)s, %(value_hrf)s)
                    """, row)
            except:
                break
        conn.commit()

    logger.info("updating node latest observations")
    cursor.execute(
        """
        SELECT node_id, MAX(timestamp) AS timestamp
        FROM observations
        GROUP BY node_id
        """)
    rows = cursor.fetchall()

    for row in rows:
        row = dict(zip(["node_id", "timestamp"], row))
        logger.debug(f"({row})")
        cursor.execute(
            """
            INSERT INTO latest_observation_per_node (node_id, timestamp)
            VALUES (%(node_id)s, %(timestamp)s)
            ON CONFLICT (node_id)
            DO UPDATE SET
                timestamp = EXCLUDED.timestamp
            """, row)
    conn.commit()

    logger.info("cleaning up")
    cursor.close()
    conn.close()
    shutil.rmtree(dirname)
