CREATE TABLE IF NOT EXISTS run_journal_heads (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 execution_generation bigint unsigned NOT NULL DEFAULT 1,
 seq bigint unsigned NOT NULL DEFAULT 0,
 digest char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '',
 total_bytes int unsigned NOT NULL DEFAULT 0,
 CONSTRAINT run_journal_heads_run_fk FOREIGN KEY(run_id) REFERENCES runs(id) ON DELETE CASCADE,
 CONSTRAINT run_journal_heads_generation CHECK(execution_generation>0),
 CONSTRAINT run_journal_heads_size CHECK(total_bytes<=16777216)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
CREATE TABLE IF NOT EXISTS run_journal_records (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 seq bigint unsigned NOT NULL,
 digest char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 object_key varchar(512) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 object_sha256 char(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 size_bytes int unsigned NOT NULL,
 PRIMARY KEY(run_id,seq),
 UNIQUE KEY run_journal_records_object(object_key),
 CONSTRAINT run_journal_records_head_fk FOREIGN KEY(run_id) REFERENCES run_journal_heads(run_id) ON DELETE CASCADE,
 CONSTRAINT run_journal_records_seq CHECK(seq>0),
 CONSTRAINT run_journal_records_size CHECK(size_bytes BETWEEN 1 AND 2097153)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
