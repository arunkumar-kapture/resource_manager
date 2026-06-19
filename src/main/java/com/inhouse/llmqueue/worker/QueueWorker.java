package com.inhouse.llmqueue.worker;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import com.inhouse.llmqueue.repository.QueueRequestRepository;
import com.inhouse.llmqueue.repository.RequestLogRepository;
import com.inhouse.llmqueue.service.AuditService;
import com.inhouse.llmqueue.service.CapacityService;
import com.inhouse.llmqueue.service.LlmClient;
import com.inhouse.llmqueue.service.QueueService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class QueueWorker {

    private final QueueRequestRepository queueRequestRepository;
    private final RequestLogRepository requestLogRepository;
    private final CapacityService capacityService;
    private final LlmClient llmClient;
    private final QueueService queueService;
    private final AuditService auditService;

    @Value("${worker.request-log-ttl-minutes:2}")
    private int requestLogTtlMinutes;

    private static final List<String> MODELS = List.of("aether-nova", "aether-pulse", "bolt-halo", "bolt-surge");
    private static final int BATCH_SIZE = 5;

    @Scheduled(fixedDelayString = "${worker.poll-interval-ms:1000}")
    public void poll() {
        for (String modelName : MODELS) {
            try {
                processModel(modelName);
            } catch (Exception e) {
                log.error("[WORKER] Error while polling queue for model {}: {}", modelName, e.getMessage());
            }
        }
    }

    @Transactional
    public void processModel(String modelName) throws InterruptedException {
        // Validate model is active before processing
        try {
            capacityService.getActiveConfig(modelName);
        } catch (Exception e) {
            return;
        }

        // Fetch requests due for processing (scheduled_at <= NOW() or no scheduled_at)
        List<QueueRequest> requests = queueRequestRepository.fetchNextForProcessing(modelName, BATCH_SIZE);
        if (requests.isEmpty()) return;

        int dispatched = 0;
        for (QueueRequest req : requests) {

            // Check resource availability before each request
            if (!capacityService.hasBatchCapacity(modelName)) {
                log.info("[WORKER] Model {} - batch threshold exceeded, pausing - {} requests remain queued",
                        modelName, requests.size() - dispatched);
                return; // stop this cycle, retry on next poll
            }

            processRequest(req);
            dispatched++;

            // 1s gap between dispatches
            if (dispatched < requests.size()) {
                Thread.sleep(1000);
            }

            // After every 5 consecutive dispatches - re-check resource availability
            if (dispatched % BATCH_SIZE == 0) {
                if (!capacityService.hasBatchCapacity(modelName)) {
                    log.info("[WORKER] Model {} - batch threshold exceeded after {} dispatches, pausing",
                            modelName, dispatched);
                    return; // remaining stay queued, next poll cycle will continue
                }
            }
        }
    }

    private void processRequest(QueueRequest req) {
        req.setStatus(RequestStatus.processing);
        req.setUpdatedAt(OffsetDateTime.now());
        queueRequestRepository.save(req);

        OffsetDateTime dispatchedAt = OffsetDateTime.now();
        long start = System.currentTimeMillis();
        try {
            queueService.logDispatched(req.getModelName(), req.getMode());
            var response = llmClient.chat(req.getModelName(), req.getPayload());
            int ms = (int) (System.currentTimeMillis() - start);

            // store response in same queue_requests row
            req.setStatus(RequestStatus.completed);
            req.setResult(response);
            req.setProcessedAt(OffsetDateTime.now());
            req.setUpdatedAt(OffsetDateTime.now());
            queueRequestRepository.save(req);

            log.info("[WORKER] Request {} for model {} completed in {}ms", req.getId(), req.getModelName(), ms);

            // Flex audit is written by FlexService (it owns the response path) - skip here to avoid duplicate
            if (req.getMode() != RequestMode.flex) {
                auditService.recordQueued(req.getId(), req.getId(), req.getModelName(), req.getMode(),
                        req.getPayload(), response, RequestStatus.completed, ms, null, dispatchedAt);
            }
        } catch (Exception e) {
            int ms = (int) (System.currentTimeMillis() - start);
            req.setStatus(RequestStatus.failed);
            req.setErrorMessage(e.getMessage());
            req.setUpdatedAt(OffsetDateTime.now());
            queueRequestRepository.save(req);

            log.error("[WORKER] Request {} for model {} failed: {}", req.getId(), req.getModelName(), e.getMessage());

            if (req.getMode() != RequestMode.flex) {
                auditService.recordQueued(req.getId(), req.getId(), req.getModelName(), req.getMode(),
                        req.getPayload(), null, RequestStatus.failed, ms, e.getMessage(), dispatchedAt);
            }
        }
    }

    @Scheduled(fixedDelayString = "${worker.prune-interval-ms:60000}")
    @Transactional
    public void pruneRequestLog() {
        OffsetDateTime cutoff = OffsetDateTime.now().minusMinutes(requestLogTtlMinutes);
        int deleted = requestLogRepository.deleteOlderThan(cutoff);
        if (deleted > 0) {
            log.info("[WORKER] Pruned {} stale entries from request_log older than {} minutes", deleted, requestLogTtlMinutes);
        }
    }
}
