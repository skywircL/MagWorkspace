CREATE TABLE IF NOT EXISTS run_knowledge_base_scopes (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 knowledge_base_ids json NOT NULL,
 CONSTRAINT run_knowledge_base_scopes_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_knowledge_base_scopes_array CHECK(JSON_TYPE(knowledge_base_ids)='ARRAY' AND JSON_LENGTH(knowledge_base_ids)<=20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
