#!/usr/bin/env python3

import codecs
import csv
import gzip
import logging
import os
import tarfile

import arrow
import psycopg2
import requests

import utils
from app import app
from config import Config


DOWNLOAD_SOURCE = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/status/http-events.csv'

UPSERT_BOOT_EVENT = """
INSERT INTO boot_events
    (node_id, timestamp, boot_id, boot_media)
VALUES
    (%(node_id)s, %(timestamp)s, %(boot_id)s, %(boot_media)s)
ON CONFLICT ( node_id )
    DO UPDATE SET
        timestamp   = EXCLUDED.timestamp,
        boot_id     = EXCLUDED.boot_id,
        boot_media  = EXCLUDED.boot_media
"""


@app.task
def run():
    # init
    _cfg = Config()
    logger = logging.getLogger('load_boot_events')
    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    # download the csv
    logger.info(f'downloading source csv')

    download_filename = utils.download(DOWNLOAD_SOURCE, save_to_file=True)

    # get the existing node ids
    cursor.execute("SELECT node_id FROM nodes")
    node_ids = set(node for (node,) in cursor.fetchall())

    # rip the csv
    logger.info('ripping source csv')
    logger.debug(f'download_filename={download_filename}')
    logger.debug(f'upsert sql template is:{UPSERT_BOOT_EVENT}\n')

    boots = {}
    for row in utils.iter_csv(download_filename):
        row['timestamp'] = row['timestamp']

        node_id = row.pop('node_id')
        if node_id not in node_ids:
            logger.warning(f'{node_id} not present in nodes table')
            continue

        if row['timestamp'] > boots.get(node_id, {'timestamp': ''}).get('timestamp'):
            boots[node_id] = row

    for node_id, row in boots.items():
        row['node_id'] = node_id
        row['timestamp'] = arrow.get(row['timestamp']).datetime
        cursor.execute(UPSERT_BOOT_EVENT, row)

    conn.commit()

    # clean up
    logger.info('cleaning up')
    os.remove(download_filename)


if __name__ == '__main__':
    run()
