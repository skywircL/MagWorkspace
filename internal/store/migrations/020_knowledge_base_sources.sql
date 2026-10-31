CREATE TABLE IF NOT EXISTS run_knowledge_base_sources (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 knowledge_base_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 metadata_sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 PRIMARY KEY(run_id,knowledge_base_id),
 CONSTRAINT run_knowledge_base_sources_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
