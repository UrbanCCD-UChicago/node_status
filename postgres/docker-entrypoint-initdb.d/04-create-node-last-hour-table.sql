\c node_status

-- creates a table that is a cache of the last hour of data for a node. the
-- rows of the observations table have an arbitrary ttl, but this table will
-- retain the last hour's worth of data for a node.

CREATE TABLE node_last_hour (
  node_id     TEXT REFERENCES nodes(node_id),
  timestamp   TIMESTAMP NOT NULL,
  subsystem   TEXT,
  sensor      TEXT,
  parameter   TEXT,
  value_raw   TEXT,
  value_hrf   TEXT
) ;

-- automatically fill the table with new values from observations

CREATE FUNCTION fun_load_node_last_hour() 
RETURNS TRIGGER AS $$
  BEGIN
    INSERT INTO node_last_hour (node_id, timestamp, subsystem, sensor, parameter, value_raw, value_hrf)
      VALUES (NEW.node_id, NEW.timestamp, NEW.subsystem, NEW.sensor, NEW.parameter, NEW.value_raw, NEW.value_hrf);
    
    RETURN NEW;
  END
$$
LANGUAGE plpgsql ;

CREATE TRIGGER tgr_nodes_location
AFTER INSERT ON observations
FOR EACH ROW
EXECUTE PROCEDURE fun_load_node_last_hour() ;

-- index important columns

CREATE INDEX idx_node_last_hour_timestamp
ON node_last_hour (timestamp) ;

CREATE INDEX idx_node_last_hour_node_id
ON node_last_hour (node_id) ;

CREATE INDEX idx_node_last_hour_node_id_timestamp
ON node_last_hour (node_id, timestamp) ;