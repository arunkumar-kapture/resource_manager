-- Enums
CREATE TYPE request_mode AS ENUM ('priority', 'flex', 'batch');
CREATE TYPE request_status AS ENUM ('queued', 'processing', 'completed', 'failed');
CREATE TYPE audit_source AS ENUM ('immediate', 'queue');

-- model_configs
CREATE TABLE model_configs (
    model_name          VARCHAR(100) PRIMARY KEY,
    rpm_limit           INT NOT NULL DEFAULT 60,
    batch_threshold_pct FLOAT NOT NULL DEFAULT 0.30,
    metrics_url         VARCHAR(500),
    max_concurrent      INT NOT NULL DEFAULT 10,
    is_active           BOOLEAN NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO model_configs (model_name, rpm_limit, batch_threshold_pct, max_concurrent, is_active)
VALUES
    ('aether-nova',  60, 0.30, 10, true),
    ('aether-pulse', 60, 0.30, 10, true),
    ('bolt-halo',    60, 0.30, 10, true),
    ('bolt-surge',   60, 0.30, 10, true);

-- request_log (sliding window RPM tracker, pruned every 2 minutes)
CREATE TABLE request_log (
    id          BIGSERIAL PRIMARY KEY,
    model_name  VARCHAR(100) NOT NULL,
    mode        request_mode NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_request_log_model_created ON request_log (model_name, created_at);

-- queue_requests
CREATE TABLE queue_requests (
    id               UUID PRIMARY KEY,
    model_name       VARCHAR(100) NOT NULL,
    mode             request_mode NOT NULL,
    priority_weight  SMALLINT NOT NULL DEFAULT 3,
    payload          JSONB NOT NULL,
    status           request_status NOT NULL DEFAULT 'queued',
    result           JSONB,
    error_message    TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at     TIMESTAMPTZ
);

CREATE INDEX idx_queue_requests_worker ON queue_requests (model_name, status, priority_weight, created_at);

-- audit_log (permanent, never pruned)
CREATE TABLE audit_log (
    id                BIGSERIAL PRIMARY KEY,
    request_id        UUID NOT NULL,
    model_name        VARCHAR(100) NOT NULL,
    mode              request_mode NOT NULL,
    source            audit_source NOT NULL,
    payload           JSONB NOT NULL,
    llm_response      JSONB,
    status            VARCHAR(20) NOT NULL,
    response_time_ms  INT,
    error_message     TEXT,
    queue_request_id  UUID REFERENCES queue_requests(id),
    dispatched_at     TIMESTAMPTZ,
    completed_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_log_request_id ON audit_log (request_id);
CREATE INDEX idx_audit_log_model_mode  ON audit_log (model_name, mode);
CREATE INDEX idx_audit_log_created_at  ON audit_log (created_at);
