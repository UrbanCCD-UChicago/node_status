\c node_status

CREATE TABLE node_http_events (
  node_id     TEXT REFERENCES nodes(node_id),
  timestamp   TIMESTAMP NOT NULL,
  boot_id     TEXT NOT NULL,
  boot_media  TEXT NOT NULL
) ;

CREATE UNIQUE INDEX idx_node_http_events_uniq
ON node_http_events (node_id) ;