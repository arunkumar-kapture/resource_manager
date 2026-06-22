package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import com.inhouse.llmqueue.repository.QueueRequestRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class FlexService {

    private final CapacityService capacityService;
    private final LlmClient llmClient;
    private final QueueService queueService;
    private final AuditService auditService;
    private final QueueRequestRepository queueRequestRepository;
    private final FlexEscalator flexEscalator;

    @Value("${flex.escalation-seconds:8}")
    private int escalationSeconds;

    public Map<String, Object> handle(String modelName, Map<String, Object> payload) throws InterruptedException {
        UUID requestId = UUID.randomUUID();
        OffsetDateTime dispatchedAt = OffsetDateTime.now();

        if (capacityService.hasFlexCapacity(modelName)) {
            return dispatchImmediate(requestId, modelName, payload, dispatchedAt);
        }

        // Enqueue and poll every 1s waiting for capacity
        QueueRequest queued = queueService.enqueue(modelName, RequestMode.flex, (short) 2, payload, null);
        log.info("[FLEX] Request {} queued for model {} - no capacity available, waiting for a free slot", queued.getId(), modelName);

        long enqueueTime = System.currentTimeMillis();

        while (true) {
            long elapsed = (System.currentTimeMillis() - enqueueTime) / 1000;

            // Check if worker already grabbed and completed it
            Optional<QueueRequest> current = queueRequestRepository.findById(queued.getId());
            if (current.isPresent()) {
                RequestStatus status = current.get().getStatus();
                if (status == RequestStatus.completed) {
                    int ms = (int) (System.currentTimeMillis() - enqueueTime);
                    log.info("[FLEX] Request {} picked up by worker and completed successfully in {}ms", queued.getId(), ms);
                    auditService.recordQueued(requestId, queued.getId(), modelName, RequestMode.flex,
                            payload, current.get().getResult(), RequestStatus.completed, ms, null, dispatchedAt);
                    return current.get().getResult();
                }
                if (status == RequestStatus.failed) {
                    log.error("[FLEX] Request {} failed in worker for model {}: {}", queued.getId(), modelName, current.get().getErrorMessage());
                    auditService.recordQueued(requestId, queued.getId(), modelName, RequestMode.flex,
                            payload, null, RequestStatus.failed,
                            (int) (System.currentTimeMillis() - enqueueTime),
                            current.get().getErrorMessage(), dispatchedAt);
                    throw new LlmInvocationException(current.get().getErrorMessage());
                }
            }

            // At 8s - unconditionally escalate and dispatch as priority, no capacity check
            if (elapsed >= escalationSeconds) {
                log.warn("[FLEX] Request {} waited {}s - escalating to priority dispatch for model {}",
                        queued.getId(), elapsed, modelName);
                return flexEscalator.escalate(queued.getId(), requestId, modelName, payload, dispatchedAt, enqueueTime);
            }

            Thread.sleep(1000);
        }
    }

    private Map<String, Object> dispatchImmediate(UUID requestId, String modelName,
                                                   Map<String, Object> payload, OffsetDateTime dispatchedAt) {
        long start = System.currentTimeMillis();
        try {
            queueService.logDispatched(modelName, RequestMode.flex);
            Map<String, Object> response = llmClient.chat(modelName, payload);
            int ms = (int) (System.currentTimeMillis() - start);
            log.info("[FLEX] Request {} dispatched directly to model {} and completed in {}ms", requestId, modelName, ms);
            auditService.recordImmediate(requestId, modelName, RequestMode.flex,
                    payload, response, RequestStatus.completed, ms, null, dispatchedAt);
            return response;
        } catch (Exception e) {
            int ms = (int) (System.currentTimeMillis() - start);
            log.error("[FLEX] Request {} failed to get response from model {}: {}", requestId, modelName, e.getMessage());
            auditService.recordImmediate(requestId, modelName, RequestMode.flex,
                    payload, null, RequestStatus.failed, ms, e.getMessage(), dispatchedAt);
            throw new LlmInvocationException(e.getMessage());
        }
    }

}
