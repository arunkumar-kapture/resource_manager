package com.inhouse.llmqueue.controller;

import com.inhouse.llmqueue.dto.UpdateModelConfigRequest;
import com.inhouse.llmqueue.entity.AuditLog;
import com.inhouse.llmqueue.entity.ModelConfig;
import com.inhouse.llmqueue.metrics.MetricsScraper;
import com.inhouse.llmqueue.metrics.VllmMetrics;
import com.inhouse.llmqueue.repository.AuditLogRepository;
import com.inhouse.llmqueue.repository.ModelConfigRepository;
import com.inhouse.llmqueue.service.CapacityService;
import com.inhouse.llmqueue.service.QueueService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/admin")
@RequiredArgsConstructor
public class AdminController {

    private final ModelConfigRepository modelConfigRepository;
    private final AuditLogRepository auditLogRepository;
    private final CapacityService capacityService;
    private final QueueService queueService;
    private final MetricsScraper metricsScraper;

    @GetMapping("/models")
    public List<ModelConfig> listModels() {
        return modelConfigRepository.findAll();
    }

    @Operation(summary = "Update model config - model name is taken from the payload")
    @RequestBody(
        required = true,
        content = @Content(
            mediaType = "application/json",
            examples = @ExampleObject(value = """
                {
                    "model": "aether-nova",
                    "rpm_limit": 60,
                    "batch_threshold_pct": 0.30,
                    "max_concurrent_requests": 10,
                    "p95_threshold_seconds": 2.0,
                    "load_score_threshold": 0.8,
                    "metrics_urls": [
                        "http://vllm-node-1:8000/metrics",
                        "http://vllm-node-2:8000/metrics"
                    ],
                    "active": true
                }
                """)
        )
    )
    @PutMapping("/models")
    public ResponseEntity<ModelConfig> updateModel(
            @org.springframework.web.bind.annotation.RequestBody Map<String, Object> body) {
        Object modelNameRaw = body.get("model");
        if (modelNameRaw == null || modelNameRaw.toString().isBlank()) {
            throw new IllegalArgumentException("'model' field is required in the request payload");
        }
        String modelName = modelNameRaw.toString();

        ModelConfig config = modelConfigRepository.findById(modelName)
                .orElseThrow(() -> new RuntimeException("Model not found: " + modelName));

        if (body.containsKey("rpm_limit")) {
            int v = ((Number) body.get("rpm_limit")).intValue();
            if (v <= 0) throw new IllegalArgumentException("rpm_limit must be > 0");
            config.setRpmLimit(v);
        }
        if (body.containsKey("batch_threshold_pct")) {
            double v = ((Number) body.get("batch_threshold_pct")).doubleValue();
            if (v <= 0 || v > 1) throw new IllegalArgumentException("batch_threshold_pct must be between 0 and 1");
            config.setBatchThresholdPct(v);
        }
        if (body.containsKey("max_concurrent_requests")) {
            int v = ((Number) body.get("max_concurrent_requests")).intValue();
            if (v <= 0) throw new IllegalArgumentException("max_concurrent_requests must be > 0");
            config.setMaxConcurrentRequests(v);
        }
        if (body.containsKey("p95_threshold_seconds")) {
            double v = ((Number) body.get("p95_threshold_seconds")).doubleValue();
            if (v <= 0) throw new IllegalArgumentException("p95_threshold_seconds must be > 0");
            config.setP95ThresholdSeconds(v);
        }
        if (body.containsKey("load_score_threshold")) {
            double v = ((Number) body.get("load_score_threshold")).doubleValue();
            if (v <= 0) throw new IllegalArgumentException("load_score_threshold must be > 0");
            config.setLoadScoreThreshold(v);
        }
        if (body.containsKey("metrics_urls")) {
            @SuppressWarnings("unchecked")
            List<String> urls = (List<String>) body.get("metrics_urls");
            if (urls == null || urls.isEmpty()) throw new IllegalArgumentException("metrics_urls must not be empty");
            config.setMetricsUrls(urls);
        }
        if (body.containsKey("active")) {
            config.setActive((Boolean) body.get("active"));
        }

        return ResponseEntity.ok(modelConfigRepository.save(config));
    }

    @DeleteMapping("/models/{modelName}")
    public ResponseEntity<Void> deactivateModel(@PathVariable String modelName) {
        ModelConfig config = modelConfigRepository.findById(modelName)
                .orElseThrow(() -> new RuntimeException("Model not found: " + modelName));
        config.setActive(false);
        modelConfigRepository.save(config);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/audit-logs")
    public Page<AuditLog> auditLogs(
            @RequestParam(required = false) String modelName,
            @RequestParam(required = false) String mode,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String source,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        return auditLogRepository.search(modelName, mode, status, source, from, to,
                PageRequest.of(page, size, Sort.by(Sort.Direction.DESC, "createdAt")));
    }

    @GetMapping("/stats")
    public Map<String, Object> stats() {
        List<ModelConfig> models = modelConfigRepository.findAll();
        Map<String, Object> result = new HashMap<>();
        for (ModelConfig m : models) {
            Map<String, Object> stat = new HashMap<>();
            long rpm = capacityService.currentRpm(m.getModelName());
            int freeSlots = capacityService.freeSlots(m.getModelName());
            long queueDepth = queueService.queueDepth(m.getModelName());
            VllmMetrics metrics = metricsScraper.getMetrics(m.getModelName());

            stat.put("rpm_limit", m.getRpmLimit());
            stat.put("current_rpm", rpm);
            stat.put("free_slots", freeSlots);
            stat.put("queue_depth", queueDepth);
            stat.put("max_concurrent_requests", m.getMaxConcurrentRequests());
            stat.put("p95_threshold_seconds", m.getP95ThresholdSeconds());
            stat.put("is_active", m.isActive());
            if (metrics != null) {
                stat.put("num_requests_running", metrics.getNumRequestsRunning());
                stat.put("num_requests_waiting", metrics.getNumRequestsWaiting());
                stat.put("kv_cache_usage_pct", metrics.getKvCacheUsagePct());
                stat.put("p95_ttft_ms", Math.round(metrics.getP95TtftSeconds() * 1000));
            }
            result.put(m.getModelName(), stat);
        }
        return result;
    }
}
