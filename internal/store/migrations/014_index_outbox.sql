CREATE TABLE IF NOT EXISTS index_outbox (
 job_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 failures int NOT NULL DEFAULT 0,
 lease_recoveries int NOT NULL DEFAULT 0,
 next_attempt_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 next_dispatch_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY index_outbox_delivery(id),
 KEY index_outbox_dispatch(next_dispatch_at,next_attempt_at,job_id),
 CONSTRAINT index_outbox_job_fk FOREIGN KEY(job_id) REFERENCES document_indexes(id) ON DELETE CASCADE,
 CONSTRAINT index_outbox_failures CHECK(failures >= 0),
 CONSTRAINT index_outbox_recoveries CHECK(lease_recoveries >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
INSERT INTO index_outbox(job_id,id)
SELECT id,LOWER(HEX(RANDOM_BYTES(16))) FROM document_indexes
WHERE status IN ('queued','running')
ON DUPLICATE KEY UPDATE job_id=index_outbox.job_id;
