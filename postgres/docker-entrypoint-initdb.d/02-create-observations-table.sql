\c node_status

-- creates the observations table

CREATE TABLE observations (
  node_id     TEXT REFERENCES nodes(node_id),
  timestamp   TIMESTAMP NOT NULL,
  subsystem   TEXT,
  sensor      TEXT,
  parameter   TEXT,
  value_raw   TEXT,
  value_hrf   TEXT
) ;

-- index important columns

CREATE UNIQUE INDEX idx_observations_uniq
ON observations (node_id, timestamp, subsystem, sensor, parameter) ;

CREATE INDEX idx_observations_timestamp
ON observations (timestamp) ;

CREATE INDEX idx_observations_node_id
ON observations (node_id) ;

CREATE INDEX idx_observations_node_id_timestamp
ON observations (node_id, timestamp) ;