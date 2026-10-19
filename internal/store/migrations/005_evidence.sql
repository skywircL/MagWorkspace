CREATE TABLE IF NOT EXISTS run_retrieval_scopes (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 document_ids json NOT NULL,
 CONSTRAINT run_retrieval_scopes_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_retrieval_scopes_array CHECK(JSON_TYPE(document_ids)='ARRAY' AND JSON_LENGTH(document_ids)<=100)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS run_history_sources (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 PRIMARY KEY(run_id,source_run_id),
 CONSTRAINT run_history_sources_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_history_sources_source_fk FOREIGN KEY(source_run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_history_sources_distinct CHECK(run_id<>source_run_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS run_evidence (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 document_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 version_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 index_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 generation varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 chunk_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 title varchar(255) NOT NULL,
 start_byte int NOT NULL,
 end_byte int NOT NULL,
 cited boolean NOT NULL DEFAULT FALSE,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY run_evidence_passage(run_id,index_id,chunk_id),
 KEY run_evidence_cited(run_id,cited,created_at,id),
 CONSTRAINT run_evidence_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_evidence_offsets CHECK(start_byte>=0 AND end_byte>start_byte AND end_byte<=1048576),
 CONSTRAINT run_evidence_hashes CHECK(OCTET_LENGTH(source_sha256)=64 AND OCTET_LENGTH(sha256)=64),
 CONSTRAINT run_evidence_title CHECK(OCTET_LENGTH(title) BETWEEN 1 AND 255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
