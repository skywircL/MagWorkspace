CREATE TABLE IF NOT EXISTS memory_capture_outbox (
 run_id varchar(32) CHARACTER SET ascii COLLATE ascii_bin PRIMARY KEY,
 id varchar(32) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
 next_dispatch_at datetime(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
 UNIQUE KEY memory_capture_outbox_delivery(id),
 KEY memory_capture_outbox_dispatch(next_dispatch_at,run_id),
 CONSTRAINT memory_capture_outbox_job_fk FOREIGN KEY(run_id) REFERENCES memory_capture_jobs(run_id) ON DELETE CASCADE
) ENGINE=InnoDB;
INSERT INTO memory_capture_outbox(run_id,id)
SELECT run_id,LOWER(HEX(RANDOM_BYTES(16))) FROM memory_capture_jobs WHERE status IN ('queued','running')
ON DUPLICATE KEY UPDATE run_id=memory_capture_outbox.run_id;
