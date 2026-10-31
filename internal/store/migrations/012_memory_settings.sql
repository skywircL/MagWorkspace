CREATE TABLE IF NOT EXISTS user_memory_settings (
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 enabled boolean NOT NULL DEFAULT TRUE,
 PRIMARY KEY(workspace_id,user_id),
 CONSTRAINT memory_settings_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT memory_settings_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB;
