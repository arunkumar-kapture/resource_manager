package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PriorityService {

    private final CapacityService capacityService;
    private final LlmClient llmClient;
    private final QueueService queueService;
    private final AuditService auditService;

    public Map<String, Object> reserveResource(String modelName) {
        UUID sessionId = UUID.randomUUID();
        boolean available = capacityService.hasPriorityCapacity(modelName);
        if (available) {
            capacityService.registerSession(sessionId.toString(), modelName);
            return Map.of(
                    "resource_allocated", true,
                    "session_id", sessionId.toString(),
                    "request_id", sessionId.toString()
            );
        } else {
            return Map.of(
                    "resource_allocated", false,
                    "message", "No resource available - model is under heavy load",
                    "request_id", sessionId.toString()
            );
        }
    }

    public Map<String, Object> dispatch(String modelName, Map<String, Object> payload, String sessionId) {
        UUID requestId = UUID.randomUUID();
        OffsetDateTime dispatchedAt = OffsetDateTime.now();
        long start = System.currentTimeMillis();

        if (sessionId != null) {
            capacityService.touchSession(sessionId);
        }

        try {
            queueService.logDispatched(modelName, RequestMode.priority);
            Map<String, Object> response = llmClient.chat(modelName, payload);
            int ms = (int) (System.currentTimeMillis() - start);
            log.info("[PRIORITY] Request {} dispatched to model {} - completed in {}ms", requestId, modelName, ms);
            auditService.recordImmediate(requestId, modelName, RequestMode.priority,
                    payload, response, RequestStatus.completed, ms, null, dispatchedAt);
            return response;
        } catch (Exception e) {
            int ms = (int) (System.currentTimeMillis() - start);
            log.error("[PRIORITY] Request {} failed for model {}: {}", requestId, modelName, e.getMessage());
            auditService.recordImmediate(requestId, modelName, RequestMode.priority,
                    payload, null, RequestStatus.failed, ms, e.getMessage(), dispatchedAt);
            throw new LlmInvocationException(e.getMessage());
        } finally {
            if (sessionId != null) {
                capacityService.releaseSession(sessionId);
            }
        }
    }
}
