package com.inhouse.llmqueue.entity;

import com.inhouse.llmqueue.enums.AuditSource;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "audit_log")
@Getter
@Setter
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "request_id", nullable = false)
    private UUID requestId;

    @Column(name = "model_name", nullable = false)
    private String modelName;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "mode", nullable = false, columnDefinition = "request_mode")
    private RequestMode mode;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "source", nullable = false, columnDefinition = "audit_source")
    private AuditSource source;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, columnDefinition = "jsonb")
    private Map<String, Object> payload;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "llm_response", columnDefinition = "jsonb")
    private Map<String, Object> llmResponse;

    @Enumerated(EnumType.STRING)
    @JdbcTypeCode(SqlTypes.NAMED_ENUM)
    @Column(name = "status", nullable = false, columnDefinition = "request_status")
    private RequestStatus status;

    @Column(name = "response_time_ms")
    private Integer responseTimeMs;

    @Column(name = "error_message")
    private String errorMessage;

    @Column(name = "queue_request_id")
    private UUID queueRequestId;

    @Column(name = "dispatched_at")
    private OffsetDateTime dispatchedAt;

    @Column(name = "completed_at")
    private OffsetDateTime completedAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private OffsetDateTime createdAt;

    @PrePersist
    void prePersist() {
        createdAt = OffsetDateTime.now();
    }
}
