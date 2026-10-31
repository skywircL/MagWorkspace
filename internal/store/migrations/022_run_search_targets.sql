CREATE TABLE IF NOT EXISTS run_search_targets (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 scope json NOT NULL,
 CONSTRAINT run_search_targets_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_search_targets_object CHECK(JSON_TYPE(scope)='OBJECT')
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SET @mw_scope_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='runs' AND column_name='resource_scope_version')=0, 'ALTER TABLE runs ADD COLUMN resource_scope_version tinyint unsigned NOT NULL DEFAULT 1', 'DO 0');
PREPARE mw_scope_statement FROM @mw_scope_ddl;
EXECUTE mw_scope_statement;
DEALLOCATE PREPARE mw_scope_statement;
