package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.entity.RequestLog;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import com.inhouse.llmqueue.repository.QueueRequestRepository;
import com.inhouse.llmqueue.repository.RequestLogRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class QueueService {

    private final QueueRequestRepository queueRequestRepository;
    private final RequestLogRepository requestLogRepository;

    @Transactional
    public QueueRequest enqueue(String modelName, RequestMode mode, short priorityWeight,
                                Map<String, Object> payload, OffsetDateTime scheduledAt) {
        UUID id = UUID.randomUUID();
        QueueRequest q = new QueueRequest();
        q.setId(id);
        q.setModelName(modelName);
        q.setMode(mode);
        q.setPriorityWeight(priorityWeight);
        q.setPayload(payload);
        q.setStatus(RequestStatus.queued);
        q.setScheduledAt(scheduledAt);
        return queueRequestRepository.save(q);
    }

    public Optional<QueueRequest> findById(UUID id) {
        return queueRequestRepository.findById(id);
    }

    public long queueDepth(String modelName) {
        return queueRequestRepository.countQueued(modelName);
    }

    @Transactional
    public void logDispatched(String modelName, RequestMode mode) {
        RequestLog log = new RequestLog();
        log.setModelName(modelName);
        log.setMode(mode);
        requestLogRepository.save(log);
    }
}
