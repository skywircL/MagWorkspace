SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='runs' AND constraint_name='runs_status')>0, 'ALTER TABLE runs DROP CHECK runs_status', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='runs' AND constraint_name='runs_status_v2')=0, 'ALTER TABLE runs ADD CONSTRAINT runs_status_v2 CHECK(status IN (''running'',''suspended'',''completed'',''failed''))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
ALTER TABLE runs MODIFY COLUMN active_marker tinyint GENERATED ALWAYS AS (CASE WHEN status IN ('running','suspended') THEN 1 ELSE NULL END) STORED;
CREATE TABLE IF NOT EXISTS run_prompts (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 tool_call_id varchar(256) NOT NULL,
 question text NOT NULL,
 options json NOT NULL,
 status varchar(16) NOT NULL DEFAULT 'pending',
 answer text NOT NULL,
 snapshot_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 snapshot_sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 snapshot_size int NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 expires_at datetime(6) NOT NULL,
 resolved_at datetime(6),
 KEY run_prompts_run(run_id,created_at,id),
 KEY run_prompts_object(snapshot_key),
 CONSTRAINT run_prompts_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_prompts_status CHECK(status IN ('pending','resuming','done','failed','canceled','expired')),
 CONSTRAINT run_prompts_size CHECK(snapshot_size BETWEEN 1 AND 524288),
 CONSTRAINT run_prompts_question CHECK(OCTET_LENGTH(question) BETWEEN 1 AND 4096),
 CONSTRAINT run_prompts_options CHECK(JSON_TYPE(options)='ARRAY' AND JSON_LENGTH(options)<=10),
 CONSTRAINT run_prompts_answer CHECK(OCTET_LENGTH(answer)<=4096)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
