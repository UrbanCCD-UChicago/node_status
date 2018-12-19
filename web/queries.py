
STATUSES_FOR_ALL = """
WITH q0 AS (
    SELECT node_id, max(timestamp) AS latest_observation_timestamp
    FROM observations
    GROUP BY node_id
),
q1 AS (
    SELECT
        n.node_id,
        vsn,
        p.project_id,
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
        LEFT JOIN projects_nodes p ON p.node_id = n.node_id
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
        WHEN end_timestamp IS NOT NULL
            THEN 'black'
        WHEN start_timestamp IS NULL AND latest_observation_timestamp IS NOT NULL
            THEN 'gray'
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


STATUSES_FOR_PROJECT = """
WITH q0 AS (
    SELECT node_id, max(timestamp) AS latest_observation_timestamp
    FROM observations
    GROUP BY node_id
),
q1 AS (
    SELECT
        n.node_id,
        vsn,
        p.project_id,
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
        LEFT JOIN projects_nodes p ON p.node_id = n.node_id
        LEFT JOIN boot_events b ON b.node_id = n.node_id
        LEFT JOIN rssh_ports r ON r.node_id = n.node_id
    WHERE
        p.project_id = %(project_id)s
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


EXPORT_FOR_NODE = """
SELECT *
FROM observations
WHERE
    node_id = %(node_id)s
ORDER BY
    timestamp DESC,
    subsystem ASC,
    sensor ASC,
    parameter ASC
"""
