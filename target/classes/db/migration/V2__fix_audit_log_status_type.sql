ALTER TABLE audit_log
    ALTER COLUMN status TYPE request_status USING status::request_status;
