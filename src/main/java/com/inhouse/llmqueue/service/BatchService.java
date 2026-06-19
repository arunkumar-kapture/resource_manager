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
        // always queue - batch requests never dispatch immediately
        OffsetDateTime scheduledAt = extractScheduledAt(payload);
        payload.remove("scheduled_at");

        capacityService.getActiveConfig(modelName); // validates model is active

        var queued = queueService.enqueue(modelName, RequestMode.batch, (short) 3, payload, scheduledAt);

        if (scheduledAt != null) {
            log.info("[BATCH] Request {} queued for model {} - scheduled at {} (UTC)", queued.getId(), modelName, scheduledAt);
            return Map.of(
                    "request_id", queued.getId().toString(),
                    "status", "queued",
                    "scheduled_at", scheduledAt.toString(),
                    "message", "Request queued - will be processed at " + scheduledAt
            );
        }

        log.info("[BATCH] Request {} queued for model {} - will be processed when resources are available", queued.getId(), modelName);
        return Map.of(
                "request_id", queued.getId().toString(),
                "status", "queued",
                "message", "Request queued - will be processed when resources are available"
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
                log.warn("[BATCH] Invalid scheduled_at value '{}' - expected: 2026-06-18T10:30:00 or 2026-06-18T10:30:00Z", value);
                return null;
            }
        }
    }
}
