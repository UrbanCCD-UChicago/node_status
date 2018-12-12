\c node_status

-- creates the main nodes table

CREATE TABLE nodes (
  node_id           TEXT NOT NULL PRIMARY KEY,
  vsn               TEXT NOT NULL,
  project_id        TEXT NOT NULL,
  lon               FLOAT,
  lat               FLOAT,
  location          GEOMETRY(POINT, 4326),  -- dynamically populated
  address           TEXT,
  description       TEXT,
  start_timestamp   TIMESTAMP NOT NULL,
  end_timestamp     TIMESTAMP
) ;

-- automatically fill the `location` attribute from lon/lat

CREATE FUNCTION fun_make_nodes_location() 
RETURNS TRIGGER AS $$
  BEGIN
    IF NEW.lon != 0.0 AND NEW.lat != 0.0 THEN
      NEW.location = ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326);
    ELSE
      NEW.lon = NULL;
      NEW.lat = NULL;
      NEW.location = NULL;
    END IF ;
    RETURN NEW;
  END
$$
LANGUAGE plpgsql ;

CREATE TRIGGER tgr_nodes_location
BEFORE INSERT OR UPDATE ON nodes
FOR EACH ROW
EXECUTE PROCEDURE fun_make_nodes_location() ;

-- index important columns

CREATE UNIQUE INDEX idx_nodes_vsn
ON nodes (vsn) ;

CREATE INDEX idx_nodes_project_id
ON nodes (project_id) ;

CREATE INDEX idx_nodes_start_timestamp
ON nodes (start_timestamp) ;

CREATE INDEX idx_nodes_end_timestamp
ON nodes (end_timestamp) ;

CREATE INDEX idx_nodes_location
ON nodes USING gist (location) ;