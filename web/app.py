#!/usr/bin/env python3

import csv
from datetime import datetime, timedelta
from io import StringIO

import psycopg2
from flask import Flask, jsonify, make_response, send_file
from flask_cors import CORS
from logging2 import Logger

from config import Config


_cfg = Config()

conn = psycopg2.connect(_cfg.get_pg_dsn())
cursor = conn.cursor()

logger = Logger("webapp", level=_cfg.LL)

app = Flask(__name__)
app.config.from_object(_cfg)
CORS(app)


def _get_status():
    headers = "node_id vsn project_id lon lat address description start_timestamp end_timestamp latest_observation_timestamp " \
        "latest_http_event_timestamp boot_id boot_media unresponsive_since".split(" ")
    
    cursor.execute(
        """
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
          lo.timestamp as latest_observation_timestamp,
          nhe.timestamp as latest_http_event_timestamp,
          boot_id,
          boot_media,
          un.timestamp as unresponsive_since
        FROM
          nodes n
          LEFT JOIN latest_observation_per_node lo ON n.node_id = lo.node_id
          LEFT JOIN node_http_events nhe ON n.node_id = nhe.node_id
          LEFT JOIN unresponsive_nodes un ON n.node_id = un.node_id
        ORDER BY
          vsn ASC
        """)
    rows = cursor.fetchall()
    conn.commit()
    
    now = datetime.now()
    tolerance = timedelta(hours=6)

    nodes = [dict(zip(headers, row)) for row in rows]
    for node in nodes:
        if node["end_timestamp"] is not None:
            node["status"] = "Decommissioned"
        
        elif node["unresponsive_since"] is not None:
            node["status"] = "Unresponsive"
        
        elif node["latest_observation_timestamp"] is not None and now - node["latest_observation_timestamp"] <= tolerance:
            node["status"] = "Reporting live data"
        
        elif node["latest_http_event_timestamp"] is not None and now - node["latest_http_event_timestamp"] <= tolerance:
            node["status"] = "Alive but not reporting"

        else:
            node["status"] = "Unknown"
    
    return nodes


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


if __name__ == "__main__":
    app.run(debug=_cfg.DEBUG, host="0.0.0.0")