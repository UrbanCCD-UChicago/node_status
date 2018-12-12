\c node_status

-- creates a table that tracks the latest observation timestamp for each node

CREATE TABLE latest_observation_per_node (
  node_id     TEXT REFERENCES nodes(node_id),
  timestamp   TIMESTAMP NOT NULL
) ;

-- index important columns

CREATE UNIQUE INDEX idx_lopn_node_id
ON latest_observation_per_node (node_id) ;

CREATE INDEX idx_lopn_timestamp
ON latest_observation_per_node (timestamp) ;

CREATE INDEX idx_lopn_node_id_timestamp
ON latest_observation_per_node (node_id, timestamp) ;