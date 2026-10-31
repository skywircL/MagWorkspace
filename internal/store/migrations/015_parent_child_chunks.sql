CREATE TABLE IF NOT EXISTS chunks (
 index_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 generation varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 kind varchar(8) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 parent_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 ordinal int NOT NULL,
 start_byte int NOT NULL,
 end_byte int NOT NULL,
 content text NOT NULL,
 sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 context_header text NOT NULL,
 PRIMARY KEY(index_id,generation,id),
 UNIQUE KEY chunks_ordinal(index_id,generation,kind,ordinal),
 KEY chunks_parent(index_id,generation,parent_id),
 KEY chunks_generation(generation,index_id),
 CONSTRAINT chunks_index_fk FOREIGN KEY(index_id) REFERENCES document_indexes(id) ON DELETE CASCADE,
 CONSTRAINT chunks_kind CHECK((kind='parent' AND parent_id='') OR (kind='child' AND OCTET_LENGTH(parent_id)=32)),
 CONSTRAINT chunks_ordinal_valid CHECK(ordinal>=0 AND ordinal<8192),
 CONSTRAINT chunks_offsets CHECK(start_byte>=0 AND end_byte>start_byte AND end_byte<=1048576 AND end_byte-start_byte=OCTET_LENGTH(content)),
 CONSTRAINT chunks_hash CHECK(OCTET_LENGTH(sha256)=64),
 CONSTRAINT chunks_content_size CHECK(OCTET_LENGTH(content)<=16384 AND OCTET_LENGTH(context_header)<=8192)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name='parent_count')=0, 'ALTER TABLE document_indexes ADD COLUMN parent_count int NOT NULL DEFAULT 0', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_indexes' AND constraint_name='document_indexes_manifest')>0, 'ALTER TABLE document_indexes DROP CHECK document_indexes_manifest', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name='artifact_key')>0, 'ALTER TABLE document_indexes DROP COLUMN artifact_key', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='document_indexes' AND column_name='artifact_sha256')>0, 'ALTER TABLE document_indexes DROP COLUMN artifact_sha256', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='document_indexes' AND constraint_name='document_indexes_chunks')=0, 'ALTER TABLE document_indexes ADD CONSTRAINT document_indexes_chunks CHECK(chunk_count>=0 AND parent_count>=0 AND chunk_count+parent_count<=8192 AND (status<>''ready'' OR (chunk_count>0 AND parent_count>0)))', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
