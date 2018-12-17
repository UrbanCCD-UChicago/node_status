\c node_status

SET timezone TO 'America/Chicago';


CREATE TABLE nodes (
  node_id         TEXT NOT NULL PRIMARY KEY ,
  vsn             TEXT NOT NULL UNIQUE ,
  project_id      TEXT NULL ,
  lon             FLOAT NULL ,
  lat             FLOAT NULL ,
  address         TEXT NULL ,
  description     TEXT NULL ,
  start_timestamp TIMESTAMPTZ NOT NULL ,
  end_timestamp   TIMESTAMPTZ NULL
) ;


CREATE TABLE observations (
  node_id         TEXT REFERENCES nodes ,
  timestamp       TIMESTAMPTZ NOT NULL ,
  subsystem       TEXT NOT NULL ,
  sensor          TEXT NOT NULL ,
  parameter       TEXT NOT NULL ,
  value_raw       TEXT NULL ,
  value_hrf       TEXT NULL ,

  UNIQUE ( node_id, timestamp, subsystem, sensor, parameter )
) ;

CREATE INDEX idx_obs_node_id
ON observations (node_id) ;

CREATE INDEX idx_obs_timestamp
ON observations (timestamp) ;

CREATE INDEX idx_obs_subsystem
ON observations (subsystem) ;

CREATE INDEX idx_obs_sensor
ON observations (sensor) ;

CREATE INDEX idx_obs_parameter
ON observations (parameter) ;


CREATE TABLE boot_events (
  node_id         TEXT REFERENCES nodes UNIQUE ,
  timestamp       TIMESTAMPTZ NOT NULL ,
  boot_id         TEXT NOT NULL ,
  boot_media      TEXT NOT NULL
) ;

CREATE INDEX idx_be_timestamp
ON boot_events (timestamp) ;


CREATE TABLE rssh_ports (
  node_id         TEXT REFERENCES nodes UNIQUE ,
  timestamp       TIMESTAMPTZ NOT NULL ,
  port            TEXT NOT NULL
) ;

CREATE INDEX idx_rp_timestamp
ON rssh_ports (timestamp) ;
