-- Add the replacement first: a partially applied migration never drops all guards.
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_versions' AND constraint_name='document_versions_storage_v2')=0, 'ALTER TABLE document_versions ADD CONSTRAINT document_versions_storage_v2 CHECK((object_key='''' AND content IS NOT NULL AND size_bytes BETWEEN 0 AND 8388608 AND size_bytes=OCTET_LENGTH(content)) OR (object_key<>'''' AND content IS NULL AND size_bytes BETWEEN 0 AND 536870912))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_versions' AND constraint_name='document_versions_storage')>0, 'ALTER TABLE document_versions DROP CHECK document_versions_storage', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
