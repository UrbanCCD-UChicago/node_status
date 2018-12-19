#!/usr/bin/env python3

import codecs
import csv
import gzip
import logging
import os
import re
import shutil
import tarfile
from datetime import date, datetime

import arrow
import psycopg2
import requests

import utils
from app import app
from config import Config


DOWNLOAD_SOURCE     = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/beehive1/live-nodes.txt'
TIMESTAMP_LINE_RE   = re.compile('updated on .+ (?P<time>\d\d\:\d\d).*', re.IGNORECASE)
NODE_ID_AND_PORT_RE = re.compile('.{6}(?P<port>\d{5})\s+0*(?P<node_id>001e06\w+).*', re.IGNORECASE)

UPSERT_RSSH_PORT = """
INSERT INTO rssh_ports
    (node_id, timestamp, port)
VALUES
    (%(node_id)s, %(timestamp)s, %(port)s)
ON CONFLICT ( node_id )
    DO UPDATE SET
        timestamp   = EXCLUDED.timestamp,
        port        = EXCLUDED.port
"""


@app.task
def run():
    # init
    _cfg = Config()
    logger = logging.getLogger('load_rssh_ports')
    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    download_filename = utils.download(DOWNLOAD_SOURCE, save_to_file=True)

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
                        logger.warning(f'timestamp line regex hit but no parse result: {line}')

                else:
                    continue

            # check if the line is a data line
            m = NODE_ID_AND_PORT_RE.match(line)
            if m:
                node_id = m.groupdict().get('node_id').lower()
                port = m.groupdict().get('port')

                if node_id not in node_ids:
                    logger.warning(f'{node_id} not present in nodes table')
                elif node_id and port:
                    cursor.execute(UPSERT_RSSH_PORT, {'node_id': node_id, 'timestamp': timestamp, 'port': port})
                else:
                    logger.warning(f'data line regex hit but no parse result: {line}')

    conn.commit()

    # clean up
    logger.info('cleaning up')
    os.remove(download_filename)


if __name__ == '__main__':
    run()
