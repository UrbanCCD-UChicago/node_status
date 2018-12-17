#!/usr/bin/env python3

# why aren't the timestamps set to UTC in AoT data?
import os
import time
os.environ['TZ'] = 'America/Chicago'
time.tzset()

import csv
from datetime import datetime, timedelta
from io import StringIO

import psycopg2
from flask import Flask, jsonify, make_response, send_file
from flask_cors import CORS
from logging2 import Logger

from config import Config


BASE_QUERY = """
WITH q0 AS (
    SELECT node_id, max(timestamp) AS latest_observation_timestamp
    FROM observations
    GROUP BY node_id
),
q1 AS (
    SELECT
        n.node_id,
        vsn,
        project_id,
        lon,
        lat,
        address,
        description,
        start_timestamp,
        end_timestamp,
        b.timestamp AS latest_boot_timestamp,
        boot_id,
        boot_media,
        r.timestamp AS latest_rssh_timestamp,
        port
    FROM
        nodes n
        LEFT JOIN boot_events b ON b.node_id = n.node_id
        LEFT JOIN rssh_ports r ON r.node_id = n.node_id
),
q2 AS (
    SELECT q1.*, q0.latest_observation_timestamp
    FROM q1 LEFT JOIN q0 ON q0.node_id = q1.node_id
)
SELECT
    *,
    CASE
        WHEN latest_observation_timestamp >= (NOW() - interval '24 hours') 
            THEN 'green'
        WHEN latest_boot_timestamp >= (NOW() - interval '24 hours')
            AND latest_rssh_timestamp >= (NOW() - interval '24 hours')
            THEN 'blue'
        WHEN latest_rssh_timestamp >= (NOW() - interval '24 hours')
            THEN 'yellow'
        WHEN latest_boot_timestamp >= (NOW() - interval '24 hours')
            THEN 'orange'
        ELSE 'red'
    END AS status
FROM q2
ORDER BY vsn ASC
"""

EXPORT_QUERY = """
SELECT o.*
FROM observations o
    LEFT JOIN nodes n ON n.node_id = o.node_id
WHERE
    n.vsn = %s
ORDER BY
    timestamp DESC,
    subsystem ASC,
    sensor ASC,
    parameter ASC
"""


_cfg = Config()

conn = psycopg2.connect(_cfg.get_pg_dsn())
cursor = conn.cursor()

logger = Logger("web", level=_cfg.LL)

app = Flask(__name__)
app.config.from_object(_cfg)
CORS(app)


def _get_status():
    headers = "node_id vsn project_id lon lat address description start_timestamp " \
        "end_timestamp latest_boot_timestamp boot_id boot_media latest_rssh_timestamp " \
        "port latest_observation_timestamp status".split(" ")

    cursor.execute(BASE_QUERY)
    rows = cursor.fetchall()
    conn.commit()

    return [dict(zip(headers, row)) for row in rows]


@app.route("/status.csv")
def status_csv():
    rows = _get_status()
    si = StringIO()
    writer = csv.DictWriter(si, rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    resp = make_response(si.getvalue())
    now = datetime.now().strftime("%Y-%m-%d.%H-%M-%S")
    resp.headers["Content-Disposition"] = f"attachment; filename=node-status-{now}.csv"
    resp.headers["Content-type"] = "text/csv"
    return resp


@app.route("/status.json")
def status_json():
    return jsonify(_get_status())


@app.route("/status.geojson")
def status_geojson():
    data = []
    for obj in _get_status():
        if obj["end_timestamp"]:
            continue
        
        lon = obj.pop("lon")
        lat = obj.pop("lat")
        data.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat]
            },
            "properties": obj
        })
    
    return jsonify(data)


@app.route("/")
def index():
    return send_file("index.html")


@app.route("/last-hour/<vsn>.csv")
def last_hour(vsn):
    cursor.execute(EXPORT_QUERY, (vsn,))
    headers = ["node_id", "timestamp", "subsystem", "sensor", "parameter", "value_raw", "value_hrf"]
    rows = [dict(zip(headers, row)) for row in cursor.fetchall()]

    conn.commit()

    si = StringIO()
    writer = csv.DictWriter(si, rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    resp = make_response(si.getvalue())
    now = datetime.now().strftime("%Y-%m-%d.%H-%M-%S")
    resp.headers["Content-Disposition"] = f"attachment; filename={vsn}-last-hour-{now}.csv"
    resp.headers["Content-type"] = "text/csv"
    return resp


if __name__ == "__main__":
    app.run(debug=_cfg.DEBUG, host="0.0.0.0")
