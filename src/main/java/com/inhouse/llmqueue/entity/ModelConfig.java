package com.inhouse.llmqueue.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.annotations.UpdateTimestamp;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.List;

@Entity
@Table(name = "model_configs")
@Getter
@Setter
public class ModelConfig {

    @Id
    @Column(name = "model_name")
    private String modelName;

    @Column(name = "rpm_limit", nullable = false)
    private int rpmLimit;

    @Column(name = "batch_threshold_pct", nullable = false)
    private double batchThresholdPct;

    @Column(name = "max_concurrent_requests", nullable = false)
    private int maxConcurrentRequests;

    /** TTFT P95 danger threshold in seconds - requests are hard-stopped when P95 exceeds this. */
    @Column(name = "p95_threshold_seconds", nullable = false)
    private double p95ThresholdSeconds;

    /** Weighted load score threshold - new sessions rejected when score reaches this value. */
    @Column(name = "load_score_threshold", nullable = false)
    private double loadScoreThreshold;

    /** Prometheus /metrics URLs for all vLLM instances hosting this model. */
    @JdbcTypeCode(SqlTypes.ARRAY)
    @Column(name = "metrics_urls", columnDefinition = "TEXT[]")
    private List<String> metricsUrls;

    @Column(name = "is_active", nullable = false)
    private boolean isActive;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private OffsetDateTime updatedAt;

    @PrePersist
    void prePersist() {
        createdAt = OffsetDateTime.now();
        updatedAt = OffsetDateTime.now();
    }
}
