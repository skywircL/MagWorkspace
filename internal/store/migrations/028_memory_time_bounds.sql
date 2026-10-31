SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='user_memories' AND constraint_name='memories_time_range')=0, 'ALTER TABLE user_memories ADD CONSTRAINT memories_time_range CHECK(created_at>=0 AND created_at<253402300800 AND (expires_at IS NULL OR (expires_at>=0 AND expires_at<253402300800)) AND (last_used_at IS NULL OR (last_used_at>=0 AND last_used_at<253402300800)))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='memory_tombstones' AND constraint_name='memory_tombstones_time_range')=0, 'ALTER TABLE memory_tombstones ADD CONSTRAINT memory_tombstones_time_range CHECK(forgotten_at>=0 AND forgotten_at<253402300800)', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
