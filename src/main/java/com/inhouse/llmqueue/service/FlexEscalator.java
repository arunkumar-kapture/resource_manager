package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import com.inhouse.llmqueue.repository.QueueRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class FlexEscalator {

    private final QueueRequestRepository queueRequestRepository;
    private final LlmClient llmClient;
    private final QueueService queueService;
    private final AuditService auditService;

    /**
     * Called via an external Spring bean reference so @Transactional proxy fires.
     * Marks the row as processing, dispatches LLM, then saves completed/failed —
     * all inside one transaction so @Version increments only once at commit.
     */
    @Transactional
    public Map<String, Object> escalate(UUID queuedId, UUID requestId, String modelName,
                                        Map<String, Object> payload, OffsetDateTime dispatchedAt,
                                        long enqueueTime) {
        QueueRequest q = queueRequestRepository.findById(queuedId).orElseThrow();

        // Worker may have grabbed this between our last poll and now — don't double-dispatch
        if (q.getStatus() == RequestStatus.processing) {
            log.info("[FLEX] Request {} already picked up by worker (status=processing), waiting for worker to complete", queuedId);
            return waitForWorkerCompletion(queuedId, requestId, modelName, payload, dispatchedAt, enqueueTime);
        }
        if (q.getStatus() == RequestStatus.completed) {
            log.info("[FLEX] Request {} already completed by worker — returning stored result", queuedId);
            int ms = (int) (System.currentTimeMillis() - enqueueTime);
            auditService.recordQueued(requestId, queuedId, modelName, RequestMode.flex,
                    payload, q.getResult(), RequestStatus.completed, ms, null, dispatchedAt);
            return q.getResult();
        }
        if (q.getStatus() == RequestStatus.failed) {
            log.warn("[FLEX] Request {} already failed in worker — propagating failure", queuedId);
            throw new LlmInvocationException(q.getErrorMessage());
        }

        q.setStatus(RequestStatus.processing);
        queueRequestRepository.save(q);

        long start = System.currentTimeMillis();
        try {
            queueService.logDispatched(modelName, RequestMode.priority);
            Map<String, Object> response = llmClient.chat(modelName, payload);
            int ms = (int) (System.currentTimeMillis() - enqueueTime);
            q.setStatus(RequestStatus.completed);
            q.setResult(response);
            q.setProcessedAt(OffsetDateTime.now());
            queueRequestRepository.save(q);
            log.info("[FLEX] Request {} escalated to priority and completed for model {} in {}ms", queuedId, modelName, ms);
            auditService.recordQueued(requestId, queuedId, modelName, RequestMode.priority,
                    payload, response, RequestStatus.completed, ms, null, dispatchedAt);
            return response;
        } catch (Exception e) {
            int ms = (int) (System.currentTimeMillis() - start);
            q.setStatus(RequestStatus.failed);
            q.setErrorMessage(e.getMessage());
            queueRequestRepository.save(q);
            log.error("[FLEX] Request {} failed during priority escalation for model {}: {}", queuedId, modelName, e.getMessage());
            auditService.recordQueued(requestId, queuedId, modelName, RequestMode.priority,
                    payload, null, RequestStatus.failed, ms, e.getMessage(), dispatchedAt);
            throw new LlmInvocationException(e.getMessage());
        }
    }

    /**
     * Worker already grabbed the request — poll until it finishes rather than dispatching again.
     * Uses a simple 1s poll with a 30s hard cap; after that we give up and throw.
     */
    private Map<String, Object> waitForWorkerCompletion(UUID queuedId, UUID requestId, String modelName,
                                                         Map<String, Object> payload, OffsetDateTime dispatchedAt,
                                                         long enqueueTime) {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
            Optional<QueueRequest> current = queueRequestRepository.findById(queuedId);
            if (current.isEmpty()) break;
            RequestStatus status = current.get().getStatus();
            if (status == RequestStatus.completed) {
                int ms = (int) (System.currentTimeMillis() - enqueueTime);
                auditService.recordQueued(requestId, queuedId, modelName, RequestMode.flex,
                        payload, current.get().getResult(), RequestStatus.completed, ms, null, dispatchedAt);
                return current.get().getResult();
            }
            if (status == RequestStatus.failed) {
                throw new LlmInvocationException(current.get().getErrorMessage());
            }
        }
        throw new LlmInvocationException("Request timed out waiting for worker to complete");
    }
}
