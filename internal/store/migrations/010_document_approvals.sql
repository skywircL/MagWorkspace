CREATE TABLE IF NOT EXISTS run_document_approvals (
 prompt_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 tool_call_id varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
 metadata json NOT NULL,
 result json NULL,
 executed_at datetime(6) NULL,
 UNIQUE KEY run_document_approval_call(run_id,tool_call_id),
 CONSTRAINT document_approval_prompt_fk FOREIGN KEY(prompt_id) REFERENCES run_prompts(id) ON DELETE CASCADE,
 CONSTRAINT document_approval_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
