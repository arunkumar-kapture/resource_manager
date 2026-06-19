package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.entity.AuditLog;
import com.inhouse.llmqueue.enums.AuditSource;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import com.inhouse.llmqueue.repository.AuditLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class AuditService {

    private final AuditLogRepository auditLogRepository;

    public void recordImmediate(UUID requestId, String modelName, RequestMode mode,
                                 Map<String, Object> payload, Map<String, Object> llmResponse,
                                 RequestStatus status, int responseTimeMs, String errorMessage,
                                 OffsetDateTime dispatchedAt) {
        AuditLog log = new AuditLog();
        log.setRequestId(requestId);
        log.setModelName(modelName);
        log.setMode(mode);
        log.setSource(AuditSource.immediate);
        log.setPayload(payload);
        log.setLlmResponse(llmResponse);
        log.setStatus(status);
        log.setResponseTimeMs(responseTimeMs);
        log.setErrorMessage(errorMessage);
        log.setDispatchedAt(dispatchedAt);
        log.setCompletedAt(OffsetDateTime.now());
        auditLogRepository.save(log);
    }

    public void recordQueued(UUID requestId, UUID queueRequestId, String modelName, RequestMode mode,
                              Map<String, Object> payload, Map<String, Object> llmResponse,
                              RequestStatus status, int responseTimeMs, String errorMessage,
                              OffsetDateTime dispatchedAt) {
        AuditLog log = new AuditLog();
        log.setRequestId(requestId);
        log.setQueueRequestId(queueRequestId);
        log.setModelName(modelName);
        log.setMode(mode);
        log.setSource(AuditSource.queue);
        log.setPayload(payload);
        log.setLlmResponse(llmResponse);
        log.setStatus(status);
        log.setResponseTimeMs(responseTimeMs);
        log.setErrorMessage(errorMessage);
        log.setDispatchedAt(dispatchedAt);
        log.setCompletedAt(OffsetDateTime.now());
        auditLogRepository.save(log);
    }
}
