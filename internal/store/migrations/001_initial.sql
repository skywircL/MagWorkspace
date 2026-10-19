CREATE TABLE IF NOT EXISTS users (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 email varchar(254) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL UNIQUE,
 name varchar(255) NOT NULL,
 password_hash varchar(255) NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 CONSTRAINT users_email_lower CHECK (email = lower(email))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS auth_sessions (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 access_hash varchar(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
 refresh_hash varchar(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL UNIQUE,
 access_expires_at datetime(6) NOT NULL,
 refresh_expires_at datetime(6) NOT NULL,
 CONSTRAINT auth_sessions_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS workspaces (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 name varchar(255) NOT NULL,
 description text NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS workspace_members (
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 role varchar(16) NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 PRIMARY KEY(workspace_id,user_id),
 CONSTRAINT workspace_members_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT workspace_members_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
 CONSTRAINT workspace_members_role CHECK(role IN ('owner','admin','contributor','viewer'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS sessions (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 workspace_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 user_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 title varchar(200) NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 updated_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 KEY sessions_user_workspace(user_id,workspace_id,updated_at),
 CONSTRAINT sessions_workspace_fk FOREIGN KEY(workspace_id) REFERENCES workspaces(id) ON DELETE CASCADE,
 CONSTRAINT sessions_user_fk FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS runs (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 session_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 status varchar(16) NOT NULL,
 error_code varchar(100) NOT NULL DEFAULT '',
 mode varchar(100) NOT NULL DEFAULT '',
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 finished_at datetime(6),
 lease_expires_at datetime(6) NOT NULL,
 active_marker tinyint GENERATED ALWAYS AS (CASE WHEN status='running' THEN 1 ELSE NULL END) STORED,
 UNIQUE KEY runs_one_active_session(session_id,active_marker),
 KEY runs_session_created(session_id,created_at,id),
 CONSTRAINT runs_session_fk FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
 CONSTRAINT runs_status CHECK(status IN ('running','completed','failed'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS messages (
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 session_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 role varchar(16) NOT NULL,
 content mediumtext NOT NULL,
 created_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY messages_run_role(run_id,role),
 KEY messages_session_created(session_id,created_at,id),
 CONSTRAINT messages_session_fk FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
 CONSTRAINT messages_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT messages_role CHECK(role IN ('user','assistant'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
