\c node_status


CREATE TABLE nodes (
  node_id         TEXT PRIMARY KEY ,
  vsn             TEXT NOT NULL ,
  lon             FLOAT NULL ,
  lat             FLOAT NULL ,
  address         TEXT NULL ,
  description     TEXT NULL
) ;


CREATE TABLE projects_nodes (
  node_id         TEXT NOT NULL ,
  project_id      TEXT NOT NULL ,
  start_timestamp TIMESTAMP NOT NULL ,
  end_timestamp   TIMESTAMP NULL ,

  UNIQUE ( node_id, project_id )
) ;

CREATE INDEX idx_projects_nodes_node_id
ON projects_nodes ( node_id ) ;

CREATE INDEX idx_projects_nodes_project_id
ON projects_nodes (project_id) ;


CREATE TABLE observations (
  node_id         TEXT NOT NULL ,
  timestamp       TIMESTAMP NOT NULL ,
  subsystem       TEXT NOT NULL ,
  sensor          TEXT NOT NULL ,
  parameter       TEXT NOT NULL ,
  value_raw       TEXT NULL ,
  value_hrf       TEXT NULL ,

  UNIQUE ( node_id, timestamp, subsystem, sensor, parameter )
) ;

CREATE INDEX idx_observations_node_id
ON observations ( node_id ) ;

CREATE INDEX idx_observations_timestamp
ON observations ( timestamp ) ;

CREATE INDEX idx_observations_subsystem
ON observations ( subsystem ) ;

CREATE INDEX idx_observations_sensor
ON observations ( sensor ) ;

CREATE INDEX idx_observations_parameter
ON observations ( parameter ) ;


CREATE TABLE boot_events (
  node_id         TEXT NOT NULL UNIQUE ,
  timestamp       TIMESTAMP NOT NULL ,
  boot_id         TEXT NOT NULL ,
  boot_media      TEXT NOT NULL
) ;

CREATE INDEX idx_boot_events_node_id
ON boot_events ( node_id ) ;


CREATE TABLE rssh_ports (
  node_id         TEXT NOT NULL UNIQUE ,
  timestamp       TIMESTAMP NOT NULL ,
  port            TEXT NOT NULL
) ;

CREATE INDEX idx_rssh_ports_node_id
ON rssh_ports ( node_id ) ;
