CREATE TABLE IF NOT EXISTS user_memory_capture_state (
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 epoch bigint unsigned NOT NULL DEFAULT 0,
 PRIMARY KEY(workspace_id,user_id),
 CONSTRAINT capture_state_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT capture_state_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB;
CREATE TABLE IF NOT EXISTS memory_capture_jobs (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 session_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 source_message_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 epoch bigint unsigned NOT NULL,
 status varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'waiting',
 mode varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'automatic',
 attempt int NOT NULL DEFAULT 0,
 generation bigint unsigned NOT NULL DEFAULT 0,
 lease_expires_at datetime(6) NULL,
 next_attempt_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 saved_count int NOT NULL DEFAULT 0,
 error_code varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 KEY capture_pending(status,next_attempt_at,lease_expires_at),
 KEY capture_owner(workspace_id,user_id,status),
 CONSTRAINT capture_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT capture_message_fk FOREIGN KEY(source_message_id) REFERENCES messages(id) ON DELETE CASCADE,
 CONSTRAINT capture_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT capture_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
 CONSTRAINT capture_status CHECK(status IN ('waiting','queued','running','done','failed','cancelled')),
 CONSTRAINT capture_mode CHECK(mode IN ('automatic','explicit'))
) ENGINE=InnoDB;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='user_memories' AND column_name='origin')=0, 'ALTER TABLE user_memories ADD COLUMN origin varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''manual''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='user_memories' AND column_name='source_run_id')=0, 'ALTER TABLE user_memories ADD COLUMN source_run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='user_memories' AND column_name='source_created_at')=0, 'ALTER TABLE user_memories ADD COLUMN source_created_at datetime(6) NULL', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='user_memories' AND constraint_name='memories_origin')=0, 'ALTER TABLE user_memories ADD CONSTRAINT memories_origin CHECK(origin IN (''manual'',''explicit'',''automatic''))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
