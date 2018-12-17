#!/usr/bin/env python3

# why aren't the timestamps set to UTC in AoT data?
import os
import time
os.environ['TZ'] = 'America/Chicago'
time.tzset()

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


DOWNLOAD_DIRNAME    = 'downloads'
DOWNLOAD_FILENAME   = 'AoT_Chicago.complete.tar'
DOWNLOAD_SOURCE     = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/AoT_Chicago.complete.recent.tar'
NODES_FILENAME      = 'nodes.csv'
DATA_ZIPNAME        = 'data.csv.gz'
DATA_FILENAME       = 'data.csv'

UPSERT_NODE = """
INSERT INTO nodes (node_id, project_id, vsn, address, lat, lon, description, start_timestamp, end_timestamp)
VALUES (%(node_id)s, %(project_id)s, %(vsn)s, %(address)s, %(lat)s, %(lon)s, %(description)s, %(start_timestamp)s, %(end_timestamp)s)
ON CONFLICT (node_id)
DO UPDATE SET
    project_id      = EXCLUDED.project_id,
    vsn             = EXCLUDED.vsn,
    address         = EXCLUDED.address,
    lat             = EXCLUDED.lat,
    lon             = EXCLUDED.lon,
    description     = EXCLUDED.description,
    start_timestamp = EXCLUDED.start_timestamp,
    end_timestamp   = EXCLUDED.end_timestamp
"""

INSERT_OBSERVATION = """
INSERT INTO observations (timestamp, node_id, subsystem, sensor, parameter, value_raw, value_hrf)
VALUES (%(timestamp)s, %(node_id)s, %(subsystem)s, %(sensor)s, %(parameter)s, %(value_raw)s, %(value_hrf)s)
"""


@app.task
def run():
    # init
    _cfg = Config()
    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()
    logger = Logger("load_nodes_and_observations", level=_cfg.LL)

    # ensure clean download path
    basedir = os.path.join(os.path.dirname(__file__), DOWNLOAD_DIRNAME)

    if os.path.exists(basedir):
        logger.warning(f'download basedir still exists. did something error last run?')
        logger.debug(f'removing download basedir')
        shutil.rmtree(basedir)

    os.mkdir(basedir)

    # download tarball
    logger.info(f'downloading source tarball')
    
    download_filename = os.path.join(basedir, DOWNLOAD_FILENAME)
    logger.debug(f'source={DOWNLOAD_SOURCE}')
    logger.debug(f'download_filename={download_filename}')

    res = requests.get(DOWNLOAD_SOURCE, stream=True)
    with open(download_filename, 'wb') as fh:
        for chunk in res.iter_content(chunk_size=1024):
            if chunk:
                fh.write(chunk)

    # extract tarball
    logger.info('decompressing tarball')
    tarball = tarfile.open(download_filename)
    data_dirname = tarball.getnames()[0]
    tarball.extractall(path=basedir)
    tarball.close()

    dirname = os.path.join(basedir, data_dirname)
    logger.debug(f'dirname={dirname}')

    # rip nodes.csv
    nodes_filename = os.path.join(dirname, NODES_FILENAME)
    logger.info('ripping nodes file')
    logger.debug(f'nodes_filename={nodes_filename}')
    logger.debug(f'upsert sql template is:{UPSERT_NODE}\n')

    with codecs.open(nodes_filename, 'r', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            if row['end_timestamp'] == '':
                row['end_timestamp'] = None
            logger.debug(f'{row}')
            cursor.execute(UPSERT_NODE, row)

    conn.commit()

    # extract and rip data.csv.gz
    datagz_filename = os.path.join(dirname, DATA_ZIPNAME)
    data_filename = os.path.join(dirname, DATA_FILENAME)
    logger.debug(f'gunzipping {datagz_filename}')

    with gzip.open(datagz_filename) as ifh:
        with open(data_filename, 'wb') as ofh:
            shutil.copyfileobj(ifh, ofh)

    logger.info('ripping data file')
    logger.debug(f'data_filename={data_filename}')
    logger.debug(f'insert sql template is:{INSERT_OBSERVATION}\n')

    with codecs.open(data_filename, 'rb', encoding='utf8') as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            logger.debug(f'{row}')
            # the unique constraint on the row will prevent us
            # from trying to overwrite a ton on entries. the data
            # is produced in 5 minute intervals with at least
            # 30 minutes of back data to fill in potential gaps.
            # ergo, most of the data in the file is totally
            # useless most of the time.
            try:
                cursor.execute(INSERT_OBSERVATION, row)
            except:
                break

    conn.commit()

    # clean up
    logger.info('cleaning up')
    shutil.rmtree(basedir)
    conn.close()


if __name__ == '__main__':
    run()