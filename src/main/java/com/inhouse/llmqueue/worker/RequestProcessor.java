package com.inhouse.llmqueue.worker;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.enums.RequestStatus;
import com.inhouse.llmqueue.repository.QueueRequestRepository;
import com.inhouse.llmqueue.service.AuditService;
import com.inhouse.llmqueue.service.LlmClient;
import com.inhouse.llmqueue.service.QueueService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class RequestProcessor {

    private final QueueRequestRepository queueRequestRepository;
    private final LlmClient llmClient;
    private final QueueService queueService;
    private final AuditService auditService;

    /**
     * Fetches a fresh copy of the row by ID inside its own transaction.
     * Re-checks status == queued before doing anything — guards against
     * FlexService escalation or another worker thread grabbing the same row.
     */
    @Transactional
    public void process(UUID requestId) {
        QueueRequest req = queueRequestRepository.findById(requestId).orElse(null);
        if (req == null || req.getStatus() != RequestStatus.queued) {
            log.debug("[WORKER] Request {} is no longer queued (status={}), skipping",
                    requestId, req == null ? "gone" : req.getStatus());
            return;
        }

        req.setStatus(RequestStatus.processing);
        req.setUpdatedAt(OffsetDateTime.now());
        queueRequestRepository.save(req);

        OffsetDateTime dispatchedAt = OffsetDateTime.now();
        long start = System.currentTimeMillis();

        try {
            queueService.logDispatched(req.getModelName(), req.getMode());
            var response = llmClient.chat(req.getModelName(), req.getPayload());
            int ms = (int) (System.currentTimeMillis() - start);

            req.setStatus(RequestStatus.completed);
            req.setResult(response);
            req.setProcessedAt(OffsetDateTime.now());
            req.setUpdatedAt(OffsetDateTime.now());
            queueRequestRepository.save(req);

            log.info("[WORKER] Request {} for model {} completed in {}ms",
                    req.getId(), req.getModelName(), ms);

            auditService.recordQueued(req.getId(), req.getId(), req.getModelName(), req.getMode(),
                    req.getPayload(), response, RequestStatus.completed, ms, null, dispatchedAt);
        } catch (Exception e) {
            int ms = (int) (System.currentTimeMillis() - start);
            req.setStatus(RequestStatus.failed);
            req.setErrorMessage(e.getMessage());
            req.setUpdatedAt(OffsetDateTime.now());
            queueRequestRepository.save(req);

            log.error("[WORKER] Request {} for model {} failed: {}",
                    req.getId(), req.getModelName(), e.getMessage());

            auditService.recordQueued(req.getId(), req.getId(), req.getModelName(), req.getMode(),
                    req.getPayload(), null, RequestStatus.failed, ms, e.getMessage(), dispatchedAt);
        }
    }
}
