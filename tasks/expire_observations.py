#!/usr/bin/env python3

import psycopg2

from app import app
from config import Config


DELETE_OBSERVATIONS = """
DELETE FROM observations
WHERE node_id = %(node_id)s
    AND timestamp < (%(timestamp)s - interval '1 hour')
"""


@app.task
def run():
    # init
    _cfg = Config()
    logger = logging.getLogger('expire_observations')
    conn = psycopg2.connect(_cfg.get_pg_dsn())
    cursor = conn.cursor()

    # get node id, max(timestamp)
    logger.info('getting max timestamps per node')
    cursor.execute('SELECT node_id, max(timestamp) FROM observations GROUP BY node_id')
    nodes_times = dict((n, t) for (n, t) in cursor.fetchall())
    conn.commit()

    # remove all observations older than an hour per node
    logger.info('deleting all observations older than hour per node')
    logger.debug(f'delete sql statement:{DELETE_OBSERVATIONS}\n')
    for node_id, timestamp in nodes_times.items():
        cursor.execute(DELETE_OBSERVATIONS, {'node_id': node_id, 'timestamp': timestamp})

    conn.commit()

    # clean up
    logger.info('cleaning up')
    cursor.close()
    conn.close()


if __name__ == '__main__':
    run()
