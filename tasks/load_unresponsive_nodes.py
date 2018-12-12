#!/usr/bin/env python3

import codecs
import csv
import os
import os.path
import re
import shutil
from datetime import datetime

import psycopg2
import requests
from logging2 import Logger, LogLevel

from app import app
from config import Config


vsn_regex = re.compile("[0-9A-Z]{3,4}")

@app.task
def load_unresponsive_nodes():
    _cfg = Config()

    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    logger = Logger("load_unresponsive_nodes", level=LogLevel.debug)

    basedir = os.path.join(os.path.dirname(__file__), "downloads")
    if not os.path.exists(basedir):
        os.mkdir(basedir)

    filename = os.path.join(basedir, "aotnodes_report.txt")
    if os.path.exists(filename):
        os.remove(filename)

    logger.info("downloading raj's nodes report")
    logger.debug(f"downloading to {filename}")

    res = requests.get("https://www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/aotnodes_report.txt", stream=True)
    with open(filename, mode="wb") as fh:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                fh.write(chunk)

    logger.info("ripping nodes report")
    vsns = []
    grepping = False
    with codecs.open(filename, mode="r", encoding="utf8") as fh:
        for line in fh:
            logger.debug(f"grepping?: {grepping}. line: {line}")
            if grepping:
                vsn = line.split(",")[0].strip()
                if re.match(vsn_regex, line):
                    vsns.append(vsn)
                else:
                    grepping = False
                    continue
            
            elif line.lower().startswith("commissioned and unresponsive nodes"):
                grepping = True
            
            else:
                continue

    logger.debug(f"vsns: {vsns}")

    logger.info("getting node ids for vsns")
    cursor.execute(
        """
        SELECT node_id
        FROM nodes
        WHERE vsn = ANY(%s)
        """, (vsns,))
    nodes = [node for (node,) in cursor.fetchall()]
    logger.debug(f"nodes={nodes}")
    conn.commit()

    logger.info("getting timestamps for existing unresponsive nodes")
    cursor.execute(
        """
        SELECT node_id, timestamp
        FROM unresponsive_nodes
        """)
    unresp = dict(row for row in cursor.fetchall())
    logger.debug(f"unresp={unresp}")
    conn.commit()

    logger.info("updating unresponsive nodes table")
    cursor.execute(
        """
        TRUNCATE TABLE unresponsive_nodes
        """)
    now = datetime.now()
    for node_id in nodes:
        existing = unresp.get(node_id)
        if existing is None: 
            timestamp = now
        else: 
            timestamp = existing
        try:
            cursor.execute(
                """
                INSERT INTO unresponsive_nodes (node_id, timestamp)
                VALUES (%s, %s)
                """, (node_id, timestamp))
        except Exception as e:
            logger.error(f"tried to insert data into unresponsive nodes, got error: {e}")
    conn.commit()

    logger.info("cleaning up")
    cursor.close()
    conn.close()
    shutil.rmtree(basedir)
