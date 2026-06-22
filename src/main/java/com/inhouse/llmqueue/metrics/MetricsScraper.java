package com.inhouse.llmqueue.metrics;

import com.inhouse.llmqueue.entity.ModelConfig;
import com.inhouse.llmqueue.repository.ModelConfigRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
@RequiredArgsConstructor
public class MetricsScraper {

    private final ModelConfigRepository modelConfigRepository;

    // modelName -> latest aggregated metrics (returned to CapacityService)
    private final Map<String, VllmMetrics> cache = new ConcurrentHashMap<>();

    // modelName -> url -> engineId -> previous scrape's raw bucket snapshot
    // Used to compute delta between consecutive scrapes for accurate windowed P95
    private final Map<String, Map<String, Map<String, EngineBucketSnapshot>>> prevSnapshots = new ConcurrentHashMap<>();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    // -------------------------------------------------------------------------
    // Raw per-engine bucket snapshot — stored between scrapes to compute delta
    // -------------------------------------------------------------------------
    private static class EngineBucketSnapshot {
        double ttftCount;                          // cumulative +Inf count at last scrape
        final TreeMap<Double, Double> buckets = new TreeMap<>(); // le -> cumulative count
        boolean warmup = false;                    // first scrape after restart — skip delta, use as baseline only
        long scrapedAtMs = 0;                      // wall-clock time of this snapshot (ms)
    }

    // -------------------------------------------------------------------------
    // Per-engine parsed values from a single scrape
    // -------------------------------------------------------------------------
    private static class EngineMetrics {
        double numRequestsRunning;
        double numRequestsWaiting;
        double kvCacheUsagePct;
        final TreeMap<Double, Double> buckets = new TreeMap<>(); // le -> cumulative count (current)
        double ttftCount; // cumulative count at +Inf
    }

    // -------------------------------------------------------------------------
    // Per-VM result after delta P95 computed
    // -------------------------------------------------------------------------
    private static class InstanceMetrics {
        double numRequestsRunning;
        double numRequestsWaiting;
        double kvCacheUsagePct;
        double p95TtftSeconds; // delta-based, weighted across engines
        double deltaCount;     // total delta requests across engines (for VM-level weighting)
    }

    // -------------------------------------------------------------------------
    // Main scrape loop — runs every 5s
    // -------------------------------------------------------------------------
    @Scheduled(fixedDelayString = "${metrics.scrape-interval-ms:5000}")
    public void scrapeAll() {
        List<ModelConfig> configs = modelConfigRepository.findAllByIsActiveTrue();
        for (ModelConfig config : configs) {
            List<String> urls = config.getMetricsUrls();
            if (urls == null || urls.isEmpty()) {
                log.debug("[METRICS] No metrics URLs configured for model {} - skipping", config.getModelName());
                continue;
            }

            List<InstanceMetrics> instances = new ArrayList<>();

            for (String url : urls) {
                try {
                    String body = fetchMetrics(url);
                    InstanceMetrics inst = parseInstance(config.getModelName(), url, body);
                    instances.add(inst);
                } catch (Exception e) {
                    log.error("[METRICS] Failed to scrape [{}] for model {}: {}", url, config.getModelName(), e.getMessage());
                }
            }

            if (instances.isEmpty()) {
                // All URLs failed — remove stale entry so capacity checks get null (cold-start allow)
                // rather than acting on arbitrarily old data
                cache.remove(config.getModelName());
                log.warn("[METRICS] All scrape URLs failed for model {} — clearing cached metrics", config.getModelName());
                continue;
            }

            VllmMetrics aggregated = aggregate(config.getModelName(), instances);
            cache.put(config.getModelName(), aggregated);

            log.debug("[METRICS] Model {} - running={}, waiting={}, p95={}ms (last 5s, {} VM(s))",
                    config.getModelName(),
                    (int) aggregated.getNumRequestsRunning(),
                    (int) aggregated.getNumRequestsWaiting(),
                    Math.round(aggregated.getP95TtftSeconds() * 1000),
                    instances.size());
        }
    }

    public VllmMetrics getMetrics(String modelName) {
        return cache.get(modelName);
    }

    // -------------------------------------------------------------------------
    // Parse one VM's Prometheus output, compute delta P95 per engine
    // -------------------------------------------------------------------------

    private InstanceMetrics parseInstance(String modelName, String url, String body) {
        Map<String, EngineMetrics> engines = new LinkedHashMap<>();

        for (String line : body.split("\n")) {
            if (line.startsWith("#")) continue;
            if (!line.contains("model_name=\"" + modelName + "\"")) continue;

            String engineId = parseLabel(line, "engine");
            if (engineId == null) engineId = "default";

            EngineMetrics e = engines.computeIfAbsent(engineId, k -> new EngineMetrics());

            if (line.contains("vllm:num_requests_running")) {
                e.numRequestsRunning = parseValue(line);
            } else if (line.contains("vllm:num_requests_waiting")) {
                e.numRequestsWaiting = parseValue(line);
            } else if (line.contains("vllm:kv_cache_usage_perc")) {
                e.kvCacheUsagePct = parseValue(line);
            } else if (line.contains("vllm:time_to_first_token_seconds_count") && !line.contains("_created")) {
                e.ttftCount = parseValue(line);
            } else if (line.contains("vllm:time_to_first_token_seconds_bucket")) {
                double le = parseLe(line);
                if (!Double.isNaN(le)) {
                    e.buckets.put(le, parseValue(line));
                }
            }
        }

        // Retrieve or initialise previous snapshots for this VM URL
        Map<String, Map<String, EngineBucketSnapshot>> modelPrev = prevSnapshots.computeIfAbsent(modelName, k -> new ConcurrentHashMap<>());
        Map<String, EngineBucketSnapshot> urlPrev = modelPrev.computeIfAbsent(url, k -> new ConcurrentHashMap<>());

        InstanceMetrics inst = new InstanceMetrics();
        double totalKv = 0;
        double weightedP95Sum = 0;
        double weightedP95Total = 0;

        for (Map.Entry<String, EngineMetrics> entry : engines.entrySet()) {
            String engineId = entry.getKey();
            EngineMetrics curr = entry.getValue();

            inst.numRequestsRunning += curr.numRequestsRunning;
            inst.numRequestsWaiting += curr.numRequestsWaiting;
            totalKv += curr.kvCacheUsagePct;

            EngineBucketSnapshot prev = urlPrev.get(engineId);

            if (prev != null && prev.warmup) {
                log.debug("[METRICS] model={} engine={} — baseline scrape, skipping first delta (scraper restart)", modelName, engineId);
            } else if (prev != null && curr.ttftCount > prev.ttftCount && !curr.buckets.isEmpty()
                    && (prev.scrapedAtMs == 0 || (System.currentTimeMillis() - prev.scrapedAtMs) < 15_000)) {
                // Guard: if prev snapshot is older than 15s (missed scrape cycles due to network failure),
                // the delta spans more than one window — skip it and use current as new baseline.
                // 15s = 3× the 5s scrape interval, giving one missed cycle tolerance.

                // Compute delta buckets: subtract previous cumulative from current
                TreeMap<Double, Double> deltaBuckets = new TreeMap<>();
                boolean engineCounterReset = false;
                for (Map.Entry<Double, Double> b : curr.buckets.entrySet()) {
                    double le = b.getKey();
                    double currCount = b.getValue();
                    double prevCount = prev.buckets.getOrDefault(le, 0.0);
                    double delta = currCount - prevCount;
                    if (delta < 0) {
                        // A negative delta on any bucket means this engine's vLLM process restarted
                        // mid-cycle — the histogram is no longer monotonic and will produce wrong p95.
                        // Skip this engine's delta entirely and use current as the new baseline.
                        engineCounterReset = true;
                        break;
                    }
                    if (delta > 0) deltaBuckets.put(le, delta);
                }

                if (engineCounterReset) {
                    log.warn("[METRICS] model={} engine={} — counter reset detected (negative bucket delta), using current as new baseline", modelName, engineId);
                    EngineBucketSnapshot resetSnap = new EngineBucketSnapshot();
                    resetSnap.ttftCount = curr.ttftCount;
                    resetSnap.warmup = false;
                    curr.buckets.forEach((le, cnt) -> resetSnap.buckets.put(le, cnt));
                    urlPrev.put(engineId, resetSnap);
                    continue;
                }

                double deltaCount = curr.ttftCount - prev.ttftCount;
                if (!deltaBuckets.isEmpty() && deltaCount > 0) {
                    double engineP95 = computeP95(deltaBuckets, deltaCount);
                    weightedP95Sum   += engineP95 * deltaCount;
                    weightedP95Total += deltaCount;
                    inst.deltaCount  += deltaCount;
                }
            }
            // else: first scrape for this engine — no previous to diff against, skip P95

            // Save current as new previous snapshot; mark warmup only on first-ever scrape
            EngineBucketSnapshot snap = new EngineBucketSnapshot();
            snap.ttftCount = curr.ttftCount;
            snap.warmup = (prev == null);
            snap.scrapedAtMs = System.currentTimeMillis();
            curr.buckets.forEach((le, cnt) -> snap.buckets.put(le, cnt));
            urlPrev.put(engineId, snap);
        }

        inst.kvCacheUsagePct = engines.isEmpty() ? 0 : totalKv / engines.size();
        inst.p95TtftSeconds  = weightedP95Total > 0 ? weightedP95Sum / weightedP95Total : 0.0;
        return inst;
    }

    // -------------------------------------------------------------------------
    // Aggregate across all VMs
    // -------------------------------------------------------------------------
    private VllmMetrics aggregate(String modelName, List<InstanceMetrics> instances) {
        VllmMetrics agg = new VllmMetrics();
        agg.setModelName(modelName);
        agg.setScrapedAt(System.currentTimeMillis());

        double totalRunning = 0, totalWaiting = 0, totalKv = 0;
        double weightedP95Sum = 0, weightedP95Total = 0;

        for (InstanceMetrics inst : instances) {
            totalRunning += inst.numRequestsRunning;
            totalWaiting += inst.numRequestsWaiting;
            totalKv      += inst.kvCacheUsagePct;

            // Weight each VM's P95 by its delta request count
            if (inst.p95TtftSeconds > 0 && inst.deltaCount > 0) {
                weightedP95Sum   += inst.p95TtftSeconds * inst.deltaCount;
                weightedP95Total += inst.deltaCount;
            }
        }

        agg.setNumRequestsRunning(totalRunning);
        agg.setNumRequestsWaiting(totalWaiting);
        agg.setKvCacheUsagePct(totalKv / instances.size());
        double finalP95 = weightedP95Total > 0 ? weightedP95Sum / weightedP95Total : 0.0;
        // If no requests happened in this window, p95 stays 0 (no signal - gates allow through)
        agg.setP95TtftSeconds(finalP95);

        return agg;
    }

    // -------------------------------------------------------------------------
    // P95 via linear interpolation on delta buckets
    // -------------------------------------------------------------------------

    private double computeP95(TreeMap<Double, Double> deltaBuckets, double deltaTotal) {
        if (deltaBuckets.isEmpty() || deltaTotal <= 0) return 0.0;
        double target = deltaTotal * 0.95;

        // deltaBuckets are delta-of-cumulative-histogram buckets: each le value still contains
        // all requests <= le within the 5s window (cumulative across le, delta across time).
        // So compare each bucket's cumulative count directly to target.
        double prevLe = 0.0;
        double prevCount = 0.0;

        for (Map.Entry<Double, Double> entry : deltaBuckets.entrySet()) {
            double le = entry.getKey();
            double count = entry.getValue();
            if (Double.isInfinite(le)) continue;

            if (count >= target) {
                if (count == prevCount) return prevLe;
                double fraction = (target - prevCount) / (count - prevCount);
                return prevLe + (le - prevLe) * fraction;
            }
            prevLe = le;
            prevCount = count;
        }

        return prevLe; // all requests fell within last finite bucket
    }

    // -------------------------------------------------------------------------
    // Prometheus parsing helpers
    // -------------------------------------------------------------------------
    private String fetchMetrics(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    private String parseLabel(String line, String labelName) {
        String key = labelName + "=\"";
        int start = line.indexOf(key);
        if (start < 0) return null;
        start += key.length();
        int end = line.indexOf("\"", start);
        if (end < 0) return null;
        return line.substring(start, end);
    }

    private double parseLe(String line) {
        try {
            int start = line.indexOf("le=\"") + 4;
            int end = line.indexOf("\"", start);
            String leStr = line.substring(start, end);
            return leStr.equals("+Inf") ? Double.POSITIVE_INFINITY : Double.parseDouble(leStr);
        } catch (Exception e) {
            return Double.NaN;
        }
    }

    private double parseValue(String line) {
        try {
            String[] parts = line.trim().split("\\s+");
            return Double.parseDouble(parts[parts.length - 1]);
        } catch (Exception e) {
            return 0.0;
        }
    }
}
