\c node_status ;

CREATE TABLE unresponsive_nodes (
  node_id     TEXT REFERENCES nodes(node_id),
  timestamp   TIMESTAMP NOT NULL
) ;

CREATE UNIQUE INDEX idx_unresp_nodes_uniq
ON unresponsive_nodes (node_id) ;