package com.inhouse.llmqueue.service;

import com.inhouse.llmqueue.entity.ModelConfig;
import com.inhouse.llmqueue.metrics.MetricsScraper;
import com.inhouse.llmqueue.metrics.VllmMetrics;
import com.inhouse.llmqueue.repository.ModelConfigRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Service
@RequiredArgsConstructor
public class CapacityService {

    private final RpmCounter rpmCounter;
    private final ModelConfigRepository modelConfigRepository;
    private final MetricsScraper metricsScraper;

    private static final long SESSION_TTL_SECONDS = 90;

    // requestId -> allocation timestamp; evicted after SESSION_TTL_SECONDS
    private final ConcurrentHashMap<UUID, Instant> activeSessions = new ConcurrentHashMap<>();

    public long currentRpm(String modelName) {
        return rpmCounter.count(modelName);
    }

    /** Called when a priority request is accepted and dispatched. */
    public void registerSession(UUID requestId) {
        evictExpiredSessions();
        activeSessions.put(requestId, Instant.now());
        log.info("[PRIORITY] Session registered: {} | Active sessions: {}", requestId, activeSessions.size());
    }

    /**
     * Atomically validates the session and refreshes its timestamp in one operation.
     * Returns true if the session existed and was within TTL (and is now touched).
     * Returns false if the session was absent or expired.
     * Replaces the separate isSessionValid + touchSession calls to eliminate the
     * window where the scheduled eviction could remove the session between the two calls.
     */
    public boolean touchAndValidateSession(UUID sessionId) {
        Instant cutoff = Instant.now().minusSeconds(SESSION_TTL_SECONDS);
        // computeIfPresent is atomic on ConcurrentHashMap — either updates or does nothing
        Instant updated = activeSessions.computeIfPresent(sessionId,
                (id, prev) -> prev.isAfter(cutoff) ? Instant.now() : null);
        // returning null from computeIfPresent removes the key; non-null means valid + touched
        boolean valid = updated != null;
        if (valid) {
            log.debug("[PRIORITY] Session touched: {} | Active sessions: {}", sessionId, activeSessions.size());
        }
        return valid;
    }

    /** Called when a priority session ends (LLM call failed on resource_request). */
    public void releaseSession(UUID sessionId) {
        activeSessions.remove(sessionId);
        log.info("[PRIORITY] Session released: {} | Active sessions: {}", sessionId, activeSessions.size());
    }

    // Runs every 60s as a safety net — catches sessions that were never released (e.g. crashed requests)
    @Scheduled(fixedDelay = 60_000)
    public void evictExpiredSessionsScheduled() {
        evictExpiredSessions();
        if (!activeSessions.isEmpty()) {
            log.info("[PRIORITY] Scheduled eviction complete | Active sessions: {}", activeSessions.size());
        }
    }

    private void evictExpiredSessions() {
        Instant cutoff = Instant.now().minusSeconds(SESSION_TTL_SECONDS);
        activeSessions.entrySet().removeIf(entry -> {
            if (entry.getValue().isBefore(cutoff)) {
                log.debug("[PRIORITY] Session evicted (TTL expired): {}", entry.getKey());
                return true;
            }
            return false;
        });
    }

    public ModelConfig getActiveConfig(String modelName) {
        return modelConfigRepository.findById(modelName)
                .filter(ModelConfig::isActive)
                .orElseThrow(() -> new ModelUnavailableException(modelName));
    }

    /**
     * Flex capacity check - three conditions must ALL be true to serve from queue:
     * 1. RPM count in last 60s < rpm_limit
     * 2. Cluster P95 TTFT < p95_threshold_seconds
     * 3. Weighted load score < load_score_threshold
     *
     * If any condition fails → hold the request in queue, keep waiting.
     */
    public boolean hasFlexCapacity(String modelName) {
        ModelConfig config = getActiveConfig(modelName);

        // Condition 1: RPM check
        long rpm = currentRpm(modelName);
        if (rpm >= config.getRpmLimit()) {
            log.debug("[FLEX] Model {} - RPM {} >= limit {}, holding in queue", modelName, rpm, config.getRpmLimit());
            return false;
        }

        // Conditions 2 & 3: metrics-based checks (same as priority but non-blocking - just wait)
        VllmMetrics metrics = metricsScraper.getMetrics(modelName);
        if (metrics == null) {
            return true; // no metrics yet - allow (cold start)
        }

        double p95Ttft   = metrics.getP95TtftSeconds();
        double p95Thresh = config.getP95ThresholdSeconds();
        if (p95Ttft > 0 && p95Ttft >= p95Thresh) {
            log.debug("[FLEX] Model {} - P95 TTFT {}ms >= threshold {}ms, holding in queue",
                    modelName, Math.round(p95Ttft * 1000), Math.round(p95Thresh * 1000));
            return false;
        }

        double running  = metrics.getNumRequestsRunning();
        double waiting  = metrics.getNumRequestsWaiting();
        int    maxConc  = config.getMaxConcurrentRequests();
        double s1 = running / maxConc;
        double s2 = p95Ttft > 0 ? p95Ttft / p95Thresh : 0.0;
        double s3 = waiting / maxConc;
        double loadScore = (s1 * 0.4) + (s2 * 0.4) + (s3 * 0.2);

        if (loadScore >= config.getLoadScoreThreshold()) {
            log.debug("[FLEX] Model {} - load score {} >= threshold {}, holding in queue",
                    modelName, String.format("%.2f", loadScore), config.getLoadScoreThreshold());
            return false;
        }

        return true;
    }

    /**
     * Batch capacity check:
     *   freeRatio = 1 - (currentRpm / rpmLimit)
     *   freeRatio >= batchThresholdPct -> proceed
     *
     * batchThresholdPct = 0.7 means at least 70% of rpm capacity must be free.
     */
    public boolean hasBatchCapacity(String modelName) {
        ModelConfig config = getActiveConfig(modelName);
        long rpm = currentRpm(modelName);
        double freeRatio = 1.0 - (double) rpm / config.getRpmLimit();
        boolean available = freeRatio >= config.getBatchThresholdPct();
        if (!available) {
            log.debug("[BATCH] Model {} - free capacity {}% < required {}%, holding",
                    modelName,
                    String.format("%.0f", freeRatio * 100),
                    String.format("%.0f", config.getBatchThresholdPct() * 100));
        }
        return available;
    }

    /**
     * Priority capacity check - two gates, both must pass:
     *
     * Gate 1 - P95 TTFT hard stop:
     *   If avg P95 TTFT across all vLLM instances >= p95ThresholdSeconds (default 0.8s),
     *   the model is processing heavy requests. Hard stop - reject immediately.
     *
     * Gate 2 - Weighted load score:
     *   score = (numRunning / maxConcurrent) * 0.4
     *         + (p95TtftSeconds / p95ThresholdSeconds) * 0.4
     *         + (numWaiting  / maxConcurrent) * 0.2
     *   If score >= 1.0 → reject (system approaching saturation).
     *
     * Both thresholds (maxConcurrentRequests, p95ThresholdSeconds) are configured per-model via /admin/models.
     */
    
    public boolean hasPriorityCapacity(String modelName) {
        ModelConfig config = getActiveConfig(modelName);
        VllmMetrics metrics = metricsScraper.getMetrics(modelName);

        if (metrics == null) {
            log.warn("[PRIORITY] No metrics available for model {} - allowing request (cold start)", modelName);
            return true;
        }

        double running   = metrics.getNumRequestsRunning();
        double waiting   = metrics.getNumRequestsWaiting();
        double p95Ttft   = metrics.getP95TtftSeconds();
        int    maxConc   = config.getMaxConcurrentRequests();
        double p95Thresh = config.getP95ThresholdSeconds();

        // Gate 1: P95 TTFT danger threshold - hard stop
        if (p95Ttft > 0 && p95Ttft >= p95Thresh) {
            log.info("[PRIORITY] Request rejected for model {} - P95 TTFT {}ms >= danger threshold {}ms (heavy load hard stop)",
                    modelName,
                    Math.round(p95Ttft * 1000),
                    Math.round(p95Thresh * 1000));
            return false;
        }

        // Gate 2: weighted load score
        double s1 = running / maxConc;                            // 0.4 weight - active requests
        double s2 = (p95Ttft > 0 ? p95Ttft / p95Thresh : 0.0);    // 0.4 weight - latency pressure
        double s3 = waiting / maxConc;                            // 0.2 weight - queue backpressure
        double loadScore = (s1 * 0.4) + (s2 * 0.4) + (s3 * 0.2);
        double loadThreshold = config.getLoadScoreThreshold();

        if (loadScore >= loadThreshold) {
            log.info("[PRIORITY] Request rejected for model {} - load score {} >= threshold {} (running={}/{}, p95={}ms, waiting={})",
                    modelName, String.format("%.2f", loadScore), String.format("%.2f", loadThreshold),
                    (int) running, maxConc,
                    Math.round(p95Ttft * 1000),
                    (int) waiting);
            return false;
        }

        log.debug("[PRIORITY] Model {} capacity OK - load score {} (running={}, p95={}ms, waiting={})",
                modelName, String.format("%.2f", loadScore),
                (int) running, Math.round(p95Ttft * 1000), (int) waiting);
        return true;
    }

    public int freeSlots(String modelName) {
        ModelConfig config = getActiveConfig(modelName);
        long rpm = currentRpm(modelName);
        return (int) Math.max(0, config.getRpmLimit() - rpm);
    }
}
