CREATE TABLE IF NOT EXISTS knowledge_bases (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 name varchar(255) NOT NULL,
 description text NOT NULL,
 default_slot tinyint NULL,
 created_by varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY knowledge_bases_workspace_id(workspace_id,id),
 UNIQUE KEY knowledge_bases_default(workspace_id,default_slot),
 KEY knowledge_bases_workspace_created(workspace_id,created_at,id),
 CONSTRAINT knowledge_bases_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT knowledge_bases_creator_fk FOREIGN KEY(created_by) REFERENCES users(id),
 CONSTRAINT knowledge_bases_name CHECK(OCTET_LENGTH(name) BETWEEN 1 AND 255),
 CONSTRAINT knowledge_bases_description CHECK(OCTET_LENGTH(description)<=2000),
 CONSTRAINT knowledge_bases_default_slot CHECK(default_slot IS NULL OR default_slot=1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SET @mw_kb_initial = (SELECT COUNT(*)=0 FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='documents' AND column_name='knowledge_base_id');
INSERT INTO knowledge_bases(id,workspace_id,name,description,default_slot,created_by)
SELECT LOWER(HEX(RANDOM_BYTES(16))),w.id,'默认知识库','',1,m.user_id FROM workspaces w JOIN (SELECT workspace_id,MIN(user_id) AS user_id FROM workspace_members WHERE role='owner' GROUP BY workspace_id) m ON m.workspace_id=w.id
WHERE @mw_kb_initial AND NOT EXISTS(SELECT 1 FROM knowledge_bases k WHERE k.workspace_id=w.id);
SET @mw_ddl = IF(@mw_kb_initial, 'ALTER TABLE documents ADD COLUMN knowledge_base_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NULL', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
UPDATE documents d JOIN knowledge_bases k ON k.workspace_id=d.workspace_id AND k.default_slot=1 SET d.knowledge_base_id=k.id WHERE d.knowledge_base_id IS NULL;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=DATABASE() AND table_name='documents' AND column_name='knowledge_base_id' AND is_nullable='YES')>0, 'ALTER TABLE documents MODIFY COLUMN knowledge_base_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema=DATABASE() AND table_name='documents' AND index_name='documents_knowledge_base_live')=0, 'ALTER TABLE documents ADD KEY documents_knowledge_base_live(workspace_id,knowledge_base_id,deleted_at,updated_at,id)', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
SET @mw_ddl = IF((SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema=DATABASE() AND table_name='documents' AND constraint_name='documents_knowledge_base_fk')=0, 'ALTER TABLE documents ADD CONSTRAINT documents_knowledge_base_fk FOREIGN KEY(workspace_id,knowledge_base_id) REFERENCES knowledge_bases(workspace_id,id) ON DELETE CASCADE', 'DO 0');
PREPARE mw_statement FROM @mw_ddl;
EXECUTE mw_statement;
DEALLOCATE PREPARE mw_statement;
