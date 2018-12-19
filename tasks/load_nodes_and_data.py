#!/usr/bin/env python3

import codecs
import csv
import gzip
import logging
import os
import os.path
import re
import shutil
import tarfile

import arrow
import psycopg2
import requests

import utils
from app import app
from config import Config


TARBALL_LIST_PAGE   = 'https://www.mcs.anl.gov/research/projects/waggle/downloads/datasets/index.php'
NODES_FILENAME      = 'nodes.csv'
DATA_ZIPNAME        = 'data.csv.gz'
DATA_FILENAME       = 'data.csv'

UPSERT_NODE = """
INSERT INTO nodes
    (node_id, vsn, address, lat, lon, description)
VALUES
    (%(node_id)s, %(vsn)s, %(address)s, %(lat)s, %(lon)s, %(description)s)
ON CONFLICT ( node_id )
    DO UPDATE SET
        vsn             = EXCLUDED.vsn,
        address         = EXCLUDED.address,
        lat             = EXCLUDED.lat,
        lon             = EXCLUDED.lon,
        description     = EXCLUDED.description
"""

UPSERT_PROJECT_NODE = """
INSERT INTO projects_nodes
    (node_id, project_id, start_timestamp, end_timestamp)
VALUES
    (%(node_id)s, %(project_id)s, %(start_timestamp)s, %(end_timestamp)s)
ON CONFLICT ( node_id, project_id )
    DO UPDATE SET
        start_timestamp = EXCLUDED.start_timestamp,
        end_timestamp   = EXCLUDED.end_timestamp
"""

INSERT_OBSERVATION = """
INSERT INTO observations
    (timestamp, node_id, subsystem, sensor, parameter, value_raw, value_hrf)
VALUES
    (%(timestamp)s, %(node_id)s, %(subsystem)s, %(sensor)s, %(parameter)s, %(value_raw)s, %(value_hrf)s)
ON CONFLICT ( node_id, timestamp, subsystem, sensor, parameter )
    DO NOTHING
"""


_cfg = Config()

logger = logging.getLogger('load_nodes_and_data')

conn = psycopg2.connect(_cfg.get_pg_dsn())
cursor = conn.cursor()

ANL_LONLAT = (-87.981930, 41.718427)

DEFAULT_LONLAT = {
    'AoT_Chicago': (-87.623177, 41.881832),
    'AoT_Denver': (-104.991531, 39.742043),
    'AoT_Portland': (-122.676483, 45.523064),
    'AoT_Syracuse': (-76.154480, 43.088947),
    'AoT_Stanford': (-122.166077, 37.424107),
    'AoT_Detroit': (-83.045753, 42.331429),
    'AoT_Seattle': (-122.335167, 47.608013),
    'AoT_NIU': (-88.767890, 41.933563),
    'AoT_UNC': (-79.047782, 35.905043),
    'AoT_UW': (-89.412659, 43.076388),
    'Waggle_Tokyo': (139.839478, 35.652832),
    'LinkNYC': (-73.935242, 40.730610),
    'Waggle_Dronebears': ANL_LONLAT,
    'Waggle_Others': ANL_LONLAT,
    'NUCWR-MUGS': ANL_LONLAT,
    'GASP': ANL_LONLAT,
}

def scrape_list_page():
    logger.info('scraping tarball list page for download links')
    content = utils.download(TARBALL_LIST_PAGE)

    urls = set([
        url for url in re.findall('<a\s*href=[\'|"](.*?)[\'"].*?>', content)
        if url.endswith('.complete.recent.tar')
    ])

    logger.debug(f'{len(urls)} urls={urls}')
    return urls


def __check_lonlat(value):
    return value is None or value == '' or value == 0 or value == 0.0 or value == '0' or value == '0.0'


def process_tarball(url):
    # download tarball
    logger.info(f'downloading source tarball')

    # download the tarball
    download_filename = utils.download(url, save_to_file=True)

    # extract tarball
    basedir = os.path.join(
        os.path.dirname(__file__),
        'downloads')
    logger.info(f'decompressing tarball {download_filename}')
    tarball = tarfile.open(download_filename)
    extraction_dirname = tarball.getnames()[0]
    tarball.extractall(path=basedir)
    tarball.close()

    # delete the tarball
    os.remove(download_filename)

    dirname = os.path.join(basedir, extraction_dirname)
    logger.debug(f'dirname={dirname}')

    # rip nodes.csv
    nodes_filename = os.path.join(dirname, NODES_FILENAME)
    logger.info('ripping nodes file')
    logger.debug(f'nodes_filename={nodes_filename}')
    logger.debug(f'upsert sql template is:{UPSERT_NODE}\n')

    for row in utils.iter_csv(nodes_filename):
        # fix the insane timestamps in this file
        row['start_timestamp'] = arrow.get(f'{row["start_timestamp"]} America/Chicago', 'YYYY/MM/DD HH:mm:ss ZZZ').to('UTC').datetime

        if row['end_timestamp'] == '':
            row['end_timestamp'] = None
        else:
            row['end_timestamp'] = arrow.get(f'{row["end_timestamp"]} America/Chicago', 'YYYY/MM/DD HH:mm:ss ZZZ').to('UTC').datetime

        # fix missing locations
        if __check_lonlat(row['lon']) or __check_lonlat(row['lat']):
            (lon, lat) = DEFAULT_LONLAT.get(row['project_id'])
            row['lon'] = lon
            row['lat'] = lat

        # upsert node and project/node data
        cursor.execute(UPSERT_NODE, row)
        cursor.execute(UPSERT_PROJECT_NODE, row)

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

    for row in utils.iter_csv(data_filename):
        row['timestamp'] = arrow.get(f'{row["timestamp"]} America/Chicago', 'YYYY/MM/DD HH:mm:ss ZZZ').to('UTC').datetime
        cursor.execute(INSERT_OBSERVATION, row)

    conn.commit()

    # clean up
    logger.info('cleaning up')
    shutil.rmtree(dirname)


@app.task
def run():
    urls = scrape_list_page()
    for url in urls:
        try:
            process_tarball(url)
        except Exception as e:
            logger.error(f'{e}')


if __name__ == '__main__':
    run()