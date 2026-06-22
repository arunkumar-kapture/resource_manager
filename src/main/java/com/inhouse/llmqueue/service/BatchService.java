package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.enums.RequestMode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class BatchService {

    private final CapacityService capacityService;
    private final QueueService queueService;

    public Map<String, Object> handle(String modelName, Map<String, Object> payload) {
        OffsetDateTime scheduledAt = extractScheduledAt(payload);
        if (scheduledAt == null) {
            throw new IllegalArgumentException("scheduled_at is required for batch requests. Expected format: 2026-06-19T15:00:00 or 2026-06-19T15:00:00Z");
        }
        payload.remove("scheduled_at");

        capacityService.getActiveConfig(modelName); // validates model is active

        var queued = queueService.enqueue(modelName, RequestMode.batch, (short) 3, payload, scheduledAt);

        log.info("[BATCH] Request {} queued for model {} - scheduled at {} (UTC)", queued.getId(), modelName, scheduledAt);
        return Map.of(
                "request_id", queued.getId().toString(),
                "status", "queued",
                "scheduled_at", scheduledAt.toString(),
                "message", "Request queued - will be processed at " + scheduledAt
        );
    }

    private OffsetDateTime extractScheduledAt(Map<String, Object> payload) {
        Object raw = payload.get("scheduled_at");
        if (raw == null) return null;
        String value = raw.toString().trim();
        try {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                    .withOffsetSameInstant(ZoneOffset.UTC);
        } catch (Exception e1) {
            try {
                return LocalDateTime.parse(value, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                        .atOffset(ZoneOffset.UTC);
            } catch (Exception e2) {
                log.warn("[BATCH] Invalid scheduled_at value '{}' - expected: 2026-06-19T15:00:00 or 2026-06-19T15:00:00Z", value);
                return null;
            }
        }
    }
}
