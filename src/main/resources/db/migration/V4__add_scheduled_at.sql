ALTER TABLE queue_requests ADD COLUMN scheduled_at TIMESTAMPTZ;

-- index so worker query (scheduled_at <= NOW()) is fast
CREATE INDEX idx_queue_requests_scheduled ON queue_requests (model_name, status, scheduled_at, priority_weight, created_at);
