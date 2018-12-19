#!/usr/bin/env python3

import csv
from datetime import datetime, timedelta
from io import StringIO

import arrow
import psycopg2
from flask import Flask, jsonify, make_response, render_template
from flask_cors import CORS

import queries
from config import Config


_cfg = Config()

conn = psycopg2.connect(_cfg.get_pg_dsn())
cursor = conn.cursor()

app = Flask(__name__)
app.config.from_object(_cfg)
CORS(app)


ALL_PROJECTS = 'all'


def _get_status(project_id):
    headers = "node_id vsn project_id lon lat address description start_timestamp " \
        "end_timestamp latest_boot_timestamp boot_id boot_media latest_rssh_timestamp " \
        "port latest_observation_timestamp status".split(" ")

    if project_id == ALL_PROJECTS:
        cursor.execute(queries.STATUSES_FOR_ALL)
    else:
        cursor.execute(queries.STATUSES_FOR_PROJECT, {'project_id': project_id})

    rows = cursor.fetchall()
    conn.commit()

    rows = [dict(zip(headers, row)) for row in rows]
    for row in rows:
        for key, value in row.items():
            if isinstance(value, datetime):
                row[key] = arrow.get(value).to('America/Chicago').format('YYYY-MM-DD HH:mm:ss Z')

    return rows


@app.route('/status.csv')
@app.route('/status/<project_id>.csv')
def status_csv(project_id=ALL_PROJECTS):
    rows = _get_status(project_id)
    si = StringIO()
    writer = csv.DictWriter(si, rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    resp = make_response(si.getvalue())
    now = datetime.now().strftime("%Y-%m-%d.%H-%M-%S")
    resp.headers["Content-Disposition"] = f"attachment; filename=node-status-{now}.csv"
    resp.headers["Content-type"] = "text/csv"
    return resp


@app.route('/status.json')
@app.route('/status/<project_id>.json')
def status_json(project_id=ALL_PROJECTS):
    return jsonify(_get_status(project_id))


@app.route('/status.geojson')
@app.route('/status/<project_id>.geojson')
def status_geojson(project_id=ALL_PROJECTS):
    data = []
    for obj in _get_status(project_id):
        lon = obj.pop('lon')
        lat = obj.pop('lat')
        data.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [lon, lat]
            },
            'properties': obj
        })

    return jsonify(data)


@app.route('/')
@app.route('/<project_id>')
def index(project_id=ALL_PROJECTS):
    cursor.execute(queries.PROJECT_IDS)
    projects = [proj for (proj,) in cursor.fetchall()]
    conn.commit()
    return render_template('index.html', project_id=project_id, hostname=_cfg.HOSTNAME, projects=projects)


@app.route('/export/<node_id>.csv')
def export(node_id):
    cursor.execute(queries.EXPORT_FOR_NODE, {'node_id': node_id})
    headers = ['node_id', 'timestamp', 'subsystem', 'sensor', 'parameter', "value_raw", "value_hrf"]
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
