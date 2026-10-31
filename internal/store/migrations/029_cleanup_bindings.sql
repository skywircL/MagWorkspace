-- Bind resources at creation. Empty IDs retain unresolved historical rows safely;
-- workers must never infer their target from the current deployment.
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_versions' AND column_name='storage_id')=0, 'ALTER TABLE document_versions ADD COLUMN storage_id varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_versions' AND constraint_name='document_versions_binding')=0, 'ALTER TABLE document_versions ADD CONSTRAINT document_versions_binding CHECK(OCTET_LENGTH(storage_id)=0 OR REGEXP_LIKE(storage_id,''^[0-9a-f]{64}$'',''c''))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name='storage_id')=0, 'ALTER TABLE document_indexes ADD COLUMN storage_id varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_indexes' AND constraint_name='document_indexes_binding')=0, 'ALTER TABLE document_indexes ADD CONSTRAINT document_indexes_binding CHECK(OCTET_LENGTH(storage_id)=0 OR REGEXP_LIKE(storage_id,''^[0-9a-f]{64}$'',''c''))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='object_cleanup' AND column_name='storage_id')=0, 'ALTER TABLE object_cleanup ADD COLUMN storage_id varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='object_cleanup' AND constraint_name='object_cleanup_binding')=0, 'ALTER TABLE object_cleanup ADD CONSTRAINT object_cleanup_binding CHECK(OCTET_LENGTH(storage_id)=0 OR REGEXP_LIKE(storage_id,''^[0-9a-f]{64}$'',''c''))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND column_name='storage_id')=0, 'ALTER TABLE index_cleanup ADD COLUMN storage_id varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='index_cleanup' AND constraint_name='index_cleanup_binding')=0, 'ALTER TABLE index_cleanup ADD CONSTRAINT index_cleanup_binding CHECK(OCTET_LENGTH(storage_id)=0 OR REGEXP_LIKE(storage_id,''^[0-9a-f]{64}$'',''c''))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='object_cleanup' AND column_name='attempts')=0, 'ALTER TABLE object_cleanup ADD COLUMN attempts int unsigned NOT NULL DEFAULT 0', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='object_cleanup' AND column_name='last_error_code')=0, 'ALTER TABLE object_cleanup ADD COLUMN last_error_code varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='object_cleanup' AND column_name='last_attempt_at')=0, 'ALTER TABLE object_cleanup ADD COLUMN last_attempt_at datetime(6) NULL', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND column_name='attempts')=0, 'ALTER TABLE index_cleanup ADD COLUMN attempts int unsigned NOT NULL DEFAULT 0', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND column_name='last_error_code')=0, 'ALTER TABLE index_cleanup ADD COLUMN last_error_code varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND column_name='last_attempt_at')=0, 'ALTER TABLE index_cleanup ADD COLUMN last_attempt_at datetime(6) NULL', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND column_name='claim_token')=0, 'ALTER TABLE index_cleanup ADD COLUMN claim_token varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT ''''', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND column_name='lease_expires_at')=0, 'ALTER TABLE index_cleanup ADD COLUMN lease_expires_at datetime(6) NULL', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='index_cleanup' AND constraint_name='index_cleanup_lease')=0, 'ALTER TABLE index_cleanup ADD CONSTRAINT index_cleanup_lease CHECK((OCTET_LENGTH(claim_token)=0 AND lease_expires_at IS NULL) OR (REGEXP_LIKE(claim_token,''^[0-9a-f]{32}$'',''c'') AND lease_expires_at IS NOT NULL))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='object_cleanup' AND index_name='object_cleanup_binding_key')=0, 'ALTER TABLE object_cleanup ADD UNIQUE KEY object_cleanup_binding_key(storage_id,object_key)', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='object_cleanup' AND index_name='object_cleanup_key')>0, 'ALTER TABLE object_cleanup DROP INDEX object_cleanup_key', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND index_name='index_cleanup_binding_generation')=0, 'ALTER TABLE index_cleanup ADD UNIQUE KEY index_cleanup_binding_generation(storage_id,profile_id,generation)', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='index_cleanup' AND index_name='index_cleanup_generation')>0, 'ALTER TABLE index_cleanup DROP INDEX index_cleanup_generation', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
CREATE TABLE IF NOT EXISTS cleanup_retirements (
 kind varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 storage_id varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 profile_id varchar(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 resource_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 next_check_at datetime(6) NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(kind,storage_id,profile_id,resource_key),
 KEY cleanup_retirements_due(kind,next_check_at),
 CONSTRAINT cleanup_retirements_binding CHECK(OCTET_LENGTH(storage_id)=0 OR REGEXP_LIKE(storage_id,'^[0-9a-f]{64}$','c')),
 CONSTRAINT cleanup_retirements_shape CHECK((kind='object' AND OCTET_LENGTH(kind)=6 AND OCTET_LENGTH(profile_id)=0 AND OCTET_LENGTH(resource_key)>0) OR (kind='index' AND OCTET_LENGTH(kind)=5 AND REGEXP_LIKE(profile_id,'^[0-9a-f]{64}$','c') AND REGEXP_LIKE(resource_key,'^[0-9a-f]{32}$','c')))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
INSERT INTO cleanup_retirements(kind,storage_id,profile_id,resource_key,next_check_at)
 SELECT 'index',storage_id,profile_id,generation,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 DAY) FROM index_cleanup
 ON DUPLICATE KEY UPDATE resource_key=cleanup_retirements.resource_key;
INSERT INTO cleanup_retirements(kind,storage_id,profile_id,resource_key,next_check_at)
 SELECT 'object',storage_id,'',object_key,DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 1 DAY) FROM object_cleanup WHERE state='deleting'
 ON DUPLICATE KEY UPDATE resource_key=cleanup_retirements.resource_key;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_versions' AND constraint_name='document_versions_inline_binding')=0, 'ALTER TABLE document_versions ADD CONSTRAINT document_versions_inline_binding CHECK(OCTET_LENGTH(object_key)>0 OR OCTET_LENGTH(storage_id)=0)', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
