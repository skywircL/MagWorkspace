CREATE TABLE IF NOT EXISTS user_memories (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 kind varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 content text NOT NULL,
 topic text NOT NULL,
 normalized_key text NOT NULL,
 fingerprint char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 importance tinyint NOT NULL,
 status varchar(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'active',
 created_at double NOT NULL,
 expires_at double NULL,
 superseded_by varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 source_session_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 use_count bigint unsigned NOT NULL DEFAULT 0,
 last_used_at double NULL,
 KEY memories_scope(workspace_id,user_id,status,created_at),
 KEY memories_fingerprint(workspace_id,user_id,fingerprint),
 CONSTRAINT memories_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT memories_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
 CONSTRAINT memories_kind CHECK(kind IN ('profile','preference','fact','task')),
 CONSTRAINT memories_status CHECK(status IN ('active','superseded','archived')),
 CONSTRAINT memories_importance CHECK(importance BETWEEN 1 AND 5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS memory_tombstones (
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 fingerprint char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 forgotten_at double NOT NULL,
 PRIMARY KEY(workspace_id,user_id,fingerprint),
 CONSTRAINT memory_tombstones_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT memory_tombstones_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
