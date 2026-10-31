INSERT INTO index_cleanup(id,profile_id,generation,not_before)
SELECT LOWER(HEX(RANDOM_BYTES(16))),i.profile_id,i.generation,GREATEST(DATE_ADD(UTC_TIMESTAMP(6),INTERVAL 5 MINUTE),COALESCE(MAX(DATE_ADD(i.lease_expires_at,INTERVAL 30 SECOND)),UTC_TIMESTAMP(6)))
FROM document_indexes i JOIN documents d ON d.id=i.document_id AND d.workspace_id=i.workspace_id WHERE d.deleted_at IS NOT NULL AND i.generation<>'' GROUP BY i.profile_id,i.generation
ON DUPLICATE KEY UPDATE id=VALUES(id),not_before=GREATEST(index_cleanup.not_before,VALUES(not_before)),created_at=UTC_TIMESTAMP(6);
INSERT INTO object_cleanup(id,object_key,state,available_at)
SELECT LOWER(HEX(RANDOM_BYTES(16))),v.object_key,'deleting',UTC_TIMESTAMP(6) FROM document_versions v JOIN documents d ON d.id=v.document_id WHERE d.deleted_at IS NOT NULL AND v.object_key<>''
ON DUPLICATE KEY UPDATE id=VALUES(id),state='deleting',available_at=UTC_TIMESTAMP(6);
DELETE FROM documents WHERE deleted_at IS NOT NULL;
