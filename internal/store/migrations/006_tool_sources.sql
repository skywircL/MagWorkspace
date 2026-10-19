CREATE TABLE IF NOT EXISTS run_document_sources (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 document_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 version_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 index_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 generation varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 PRIMARY KEY(run_id,document_id,version_id,index_id),
 CONSTRAINT run_document_sources_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS run_conversation_sources (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 PRIMARY KEY(run_id,source_run_id),
 KEY run_conversation_sources_source(source_run_id),
 CONSTRAINT run_conversation_sources_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_conversation_sources_distinct CHECK(run_id<>source_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
