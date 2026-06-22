package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.HashMap;
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

    /**
     * Two cases handled:
     *
     * Case 1 — resource_request: true (first call, no session yet)
     *   → Check P95 + load score
     *   → If capacity available: allocate session, dispatch LLM, return response + session_id
     *   → If no capacity: return no_resource_found (no LLM call)
     *
     * Case 2 — session_id present (subsequent calls)
     *   → Validate session_id exists and is not expired
     *   → Refresh session timestamp (marks session as still active)
     *   → Dispatch LLM, return response
     *   → If session_id invalid/expired: reject with 400
     */
    public Map<String, Object> handle(String modelName, Map<String, Object> payload) {
        boolean isResourceRequest = Boolean.TRUE.equals(payload.get("resource_request"));
        Object sessionIdRaw = payload.get("session_id");
        String sessionIdStr = sessionIdRaw != null ? sessionIdRaw.toString().trim() : null;

        // Case 1: resource_request=true — allocate a new session
        if (isResourceRequest) {
            return handleResourceRequest(modelName, payload);
        }

        // Case 2: session_id present — validate and dispatch
        if (sessionIdStr != null && !sessionIdStr.isEmpty()) {
            UUID sessionId;
            try {
                sessionId = UUID.fromString(sessionIdStr);
            } catch (IllegalArgumentException e) {
                log.warn("[PRIORITY] Rejected request for model {} - malformed session_id '{}'", modelName, sessionIdStr);
                throw new IllegalArgumentException("session_id '" + sessionIdStr + "' is not a valid UUID.");
            }
            return handleSessionRequest(sessionId, modelName, payload);
        }

        // Case 3: resource_request=false or absent and no session_id — reject the request
        log.warn("[PRIORITY] Rejected request for model {} - no session_id provided and resource_request is not true", modelName);
        throw new IllegalArgumentException(
                "Request rejected: session_id is required for priority requests. " +
                "Send 'resource_request: true' first to obtain a session_id.");
    }

    // -------------------------------------------------------------------------
    // Case 1: resource_request = true — allocate session
    // -------------------------------------------------------------------------
    private Map<String, Object> handleResourceRequest(String modelName, Map<String, Object> payload) {
        if (!capacityService.hasPriorityCapacity(modelName)) {
            log.info("[PRIORITY] Resource request denied for model {} - no capacity available", modelName);
            Map<String, Object> denied = new HashMap<>();
            denied.put("resource_allocated", false);
            denied.put("reason", "no_resource_found");
            return denied;
        }

        UUID sessionId = UUID.randomUUID();
        UUID requestId = UUID.randomUUID();
        capacityService.registerSession(sessionId);

        OffsetDateTime dispatchedAt = OffsetDateTime.now();
        long start = System.currentTimeMillis();

        try {
            queueService.logDispatched(modelName, RequestMode.priority);
            Map<String, Object> llmResponse = llmClient.chat(modelName, payload);
            int ms = (int) (System.currentTimeMillis() - start);

            log.info("[PRIORITY] Resource request {} allocated session {} for model {} - completed in {}ms", requestId, sessionId, modelName, ms);

            auditService.recordImmediate(requestId, modelName, RequestMode.priority, payload, llmResponse, RequestStatus.completed, ms, null, dispatchedAt);

            // Wrap LLM response with session metadata
            Map<String, Object> response = new HashMap<>(llmResponse);
            response.put("resource_allocated", true);
            response.put("session_id", sessionId.toString());
            return response;

        } catch (Exception e) {
            int ms = (int) (System.currentTimeMillis() - start);
            capacityService.releaseSession(sessionId);
            log.error("[PRIORITY] Resource request {} failed for model {}: {}", requestId, modelName, e.getMessage());
            auditService.recordImmediate(requestId, modelName, RequestMode.priority, payload, null, RequestStatus.failed, ms, e.getMessage(), dispatchedAt);
            throw new LlmInvocationException(e.getMessage());
        }
    }

    // -------------------------------------------------------------------------
    // Case 2: session_id present — validate and dispatch
    // -------------------------------------------------------------------------
    private Map<String, Object> handleSessionRequest(UUID sessionId, String modelName, Map<String, Object> payload) {
        // Atomically validates and refreshes the session timestamp in one operation
        if (!capacityService.touchAndValidateSession(sessionId)) {
            log.warn("[PRIORITY] Rejected request for model {} - session {} is invalid or expired",
                    modelName, sessionId);
            throw new IllegalArgumentException("Session ID " + sessionId + " is invalid or expired. Please request a resource again.");
        }

        UUID requestId = UUID.randomUUID();
        OffsetDateTime dispatchedAt = OffsetDateTime.now();
        long start = System.currentTimeMillis();

        try {
            queueService.logDispatched(modelName, RequestMode.priority);
            Map<String, Object> llmResponse = llmClient.chat(modelName, payload);
            int ms = (int) (System.currentTimeMillis() - start);

            log.info("[PRIORITY] Session {} request {} for model {} completed in {}ms",
                    sessionId, requestId, modelName, ms);

            auditService.recordImmediate(requestId, modelName, RequestMode.priority,
                    payload, llmResponse, RequestStatus.completed, ms, null, dispatchedAt);

            return llmResponse;

        } catch (Exception e) {
            int ms = (int) (System.currentTimeMillis() - start);
            log.error("[PRIORITY] Session {} request {} failed for model {}: {}",
                    sessionId, requestId, modelName, e.getMessage());
            auditService.recordImmediate(requestId, modelName, RequestMode.priority,
                    payload, null, RequestStatus.failed, ms, e.getMessage(), dispatchedAt);
            throw new LlmInvocationException(e.getMessage());
        }
    }
}
