CREATE TABLE IF NOT EXISTS object_cleanup (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 object_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 state varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'pending',
 available_at datetime(6) NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY object_cleanup_key(object_key),
 KEY object_cleanup_due(available_at,id),
 CONSTRAINT object_cleanup_state CHECK(state IN ('pending','deleting'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_versions' AND column_name='object_key')=0, 'ALTER TABLE document_versions ADD COLUMN object_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '''', ADD KEY document_versions_object(object_key)', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
ALTER TABLE document_versions MODIFY COLUMN content mediumblob NULL;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_versions' AND constraint_name='document_versions_content_size')>0, 'ALTER TABLE document_versions DROP CHECK document_versions_content_size', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_versions' AND constraint_name='document_versions_storage')=0, 'ALTER TABLE document_versions ADD CONSTRAINT document_versions_storage CHECK(size_bytes BETWEEN 0 AND 8388608 AND ((object_key='''' AND content IS NOT NULL AND size_bytes=OCTET_LENGTH(content)) OR (object_key<>'''' AND content IS NULL)))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
