#!/usr/bin/env python3

import codecs
import csv
import gzip
import os
import os.path
import shutil
import tarfile

import arrow
import psycopg2
import requests
from logging2 import Logger, LogLevel

from app import app
from config import Config


DOWNLOAD_DIRNAME    = 'downloads'
DOWNLOAD_FILENAME   = 'http-events.csv'
DOWNLOAD_SOURCE     = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/status/http-events.csv'

UPSERT_BOOT_EVENT = """
INSERT INTO boot_events (node_id, timestamp, boot_id, boot_media)
VALUES (%(node_id)s, %(timestamp)s, %(boot_id)s, %(boot_media)s)
ON CONFLICT (node_id)
DO UPDATE SET
    timestamp   = EXCLUDED.timestamp,
    boot_id     = EXCLUDED.boot_id,
    boot_media  = EXCLUDED.boot_media
"""


@app.task
def run():
    # init
    _cfg = Config()
    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()
    logger = Logger("load_boot_events", level=_cfg.LL)

    # ensure clean download path
    basedir = os.path.join(os.path.dirname(__file__), DOWNLOAD_DIRNAME)

    if os.path.exists(basedir):
        logger.warning(f'download basedir still exists. did something error last run?')
        logger.debug(f'removing download basedir')
        shutil.rmtree(basedir)

    os.mkdir(basedir)

    # download the csv
    logger.info(f'downloading source csv')
    
    download_filename = os.path.join(basedir, DOWNLOAD_FILENAME)
    logger.debug(f'source={DOWNLOAD_SOURCE}')
    logger.debug(f'download_filename={download_filename}')

    res = requests.get(DOWNLOAD_SOURCE, stream=True)
    with open(download_filename, 'wb') as fh:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                fh.write(chunk)

    # get the existing node ids
    cursor.execute("SELECT node_id FROM nodes")
    node_ids = set(node for (node,) in cursor.fetchall())

    # rip the csv
    logger.info('ripping source csv')
    logger.debug(f'download_filename={download_filename}')
    logger.debug(f'upsert sql template is:{UPSERT_BOOT_EVENT}\n')

    boots = {}
    with codecs.open(download_filename, 'r', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
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
        logger.debug(f'{row}')
        try:
            cursor.execute(UPSERT_BOOT_EVENT, row)
        except Exception as e:
            logger.error(f'{e}')

    conn.commit()

    # clean up
    logger.info('cleaning up')
    shutil.rmtree(basedir)
    conn.close()


if __name__ == '__main__':
    run()
