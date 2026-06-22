package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.enums.RequestMode;
import com.inhouse.llmqueue.enums.RequestStatus;
import com.inhouse.llmqueue.repository.QueueRequestRepository;
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
    private final RpmCounter rpmCounter;

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

    /** Record one dispatch event in Redis — increments the sliding 60s RPM counter. */
    public void logDispatched(String modelName, RequestMode mode) {
        rpmCounter.record(modelName);
    }
}
