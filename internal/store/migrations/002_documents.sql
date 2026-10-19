CREATE TABLE IF NOT EXISTS documents (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 title varchar(255) NOT NULL,
 current_version_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 created_by varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 deleted_at datetime(6) NULL,
 KEY documents_workspace_live(workspace_id,deleted_at,updated_at,id),
 CONSTRAINT documents_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT documents_creator_fk FOREIGN KEY(created_by) REFERENCES users(id),
 CONSTRAINT documents_title_size CHECK(OCTET_LENGTH(title) BETWEEN 1 AND 255)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS document_versions (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 document_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 version_number int NOT NULL,
 filename varchar(255) NOT NULL,
 media_type varchar(255) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 size_bytes int NOT NULL,
 sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 content mediumblob NOT NULL,
 created_by varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY document_versions_number(document_id,version_number),
 CONSTRAINT document_versions_document_fk FOREIGN KEY(document_id) REFERENCES documents(id) ON DELETE CASCADE,
 CONSTRAINT document_versions_creator_fk FOREIGN KEY(created_by) REFERENCES users(id),
 CONSTRAINT document_versions_positive CHECK(version_number > 0),
 CONSTRAINT document_versions_filename_size CHECK(OCTET_LENGTH(filename) BETWEEN 1 AND 255),
 CONSTRAINT document_versions_content_size CHECK(size_bytes BETWEEN 0 AND 8388608 AND size_bytes=OCTET_LENGTH(content))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
