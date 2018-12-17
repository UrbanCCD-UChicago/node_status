#!/usr/bin/env python3

import codecs
import csv
import gzip
import os
import os.path
import re
import shutil
import tarfile
from datetime import date, datetime

import arrow
import psycopg2
import requests
from logging2 import Logger, LogLevel

from app import app
from config import Config


DOWNLOAD_DIRNAME    = 'downloads'
DOWNLOAD_FILENAME   = 'live-nodes.txt'
DOWNLOAD_SOURCE     = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/live-nodes.txt'
TIMESTAMP_LINE_RE   = re.compile('updated on .+ (?P<time>\d\d\:\d\d).*', re.IGNORECASE)
NODE_ID_AND_PORT_RE = re.compile('.{6}(?P<port>\d{5})\s+0*(?P<node_id>001e06\w+).*', re.IGNORECASE)

UPSERT_RSSH_PORT = """
INSERT INTO rssh_ports (node_id, timestamp, port)
VALUES (%s, %s, %s)
ON CONFLICT (node_id)
DO UPDATE SET
    timestamp   = EXCLUDED.timestamp,
    port        = EXCLUDED.port
"""


@app.task
def run():
    # init
    _cfg = Config()
    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()
    logger = Logger("load_rssh_ports", level=_cfg.LL)

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
    logger.debug(f'upsert sql template is:{UPSERT_RSSH_PORT}\n')

    timestamp = None
    with codecs.open(download_filename, 'r', encoding='utf8') as fh:
        for line in fh:
            logger.debug(f'{line}')

            if not timestamp:
                # check for the timestamp -- it should be the first line
                m = TIMESTAMP_LINE_RE.match(line)
                if m:
                    time = m.groupdict().get('time')
                    if time:
                        timestamp = arrow.get(
                            f'{date.today().isoformat()} {time} America/Chicago',
                            'YYYY-MM-DD HH:mm ZZZ').datetime
                        logger.debug(f'timestamp set to {timestamp}')
                        continue
                    else:
                        logger.warning('timestamp line regex hit but no parse result')

                else:
                    logger.warning('timestamp not set -- skipping')
                    continue

            # check if the line is a data line
            m = NODE_ID_AND_PORT_RE.match(line)
            if m:
                node_id = m.groupdict().get('node_id').lower()
                port = m.groupdict().get('port')

                if node_id not in node_ids:
                    logger.warning(f'{node_id} not present in nodes table')
                elif node_id and port:
                    cursor.execute(UPSERT_RSSH_PORT, (node_id, timestamp, port))
                else:
                    logger.warning('data line regex hit but no parse result')
            
            else:
                logger.debug('skipping line')

    conn.commit()

    # clean up
    logger.info('cleaning up')
    shutil.rmtree(basedir)
    conn.close()


if __name__ == '__main__':
    run()
