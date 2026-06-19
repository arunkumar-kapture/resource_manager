-- Rename max_concurrent -> max_concurrent_requests
ALTER TABLE model_configs RENAME COLUMN max_concurrent TO max_concurrent_requests;

-- Add metrics_urls as a text array (one model can run on multiple vLLM instances)
ALTER TABLE model_configs ADD COLUMN metrics_urls TEXT[] NOT NULL DEFAULT '{}';

-- Add p95_threshold_seconds — TTFT P95 danger threshold; requests hard-stopped above this
ALTER TABLE model_configs ADD COLUMN p95_threshold_seconds FLOAT NOT NULL DEFAULT 2.0;
