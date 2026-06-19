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

    // modelName -> aggregated metrics snapshot
    private final Map<String, VllmMetrics> cache = new ConcurrentHashMap<>();

    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Scheduled(fixedDelayString = "${metrics.scrape-interval-ms:5000}")
    public void scrapeAll() {
        List<ModelConfig> configs = modelConfigRepository.findAllByIsActiveTrue();
        for (ModelConfig config : configs) {
            List<String> urls = config.getMetricsUrls();
            if (urls == null || urls.isEmpty()) {
                log.debug("[METRICS] No metrics URLs configured for model {} - skipping", config.getModelName());
                continue;
            }

            VllmMetrics prev = cache.get(config.getModelName());
            List<InstanceMetrics> instanceSnapshots = new ArrayList<>();

            for (String url : urls) {
                try {
                    String body = fetchMetrics(url);
                    InstanceMetrics snapshot = parseInstance(config.getModelName(), body);
                    instanceSnapshots.add(snapshot);
                } catch (Exception e) {
                    log.error("[METRICS] Failed to scrape [{}] for model {}: {}", url, config.getModelName(), e.getMessage());
                }
            }

            if (instanceSnapshots.isEmpty()) continue;

            VllmMetrics aggregated = aggregate(config.getModelName(), instanceSnapshots, prev);
            cache.put(config.getModelName(), aggregated);
            log.debug("[METRICS] Model {} - running={}, waiting={}, kv_cache={}%, avg_ttft={}ms, p95_ttft={}ms (across {} instance(s))",
                    config.getModelName(),
                    (int) aggregated.getNumRequestsRunning(),
                    (int) aggregated.getNumRequestsWaiting(),
                    String.format("%.1f", aggregated.getKvCacheUsagePct() * 100),
                    Math.round(aggregated.getRecentAvgTtftSeconds() * 1000),
                    Math.round(aggregated.getP95TtftSeconds() * 1000),
                    instanceSnapshots.size());
        }
    }

    public VllmMetrics getMetrics(String modelName) {
        return cache.get(modelName);
    }

    // -------------------------------------------------------------------------
    // Internal structures - one EngineMetrics per GPU, one InstanceMetrics per VM
    // -------------------------------------------------------------------------

    private static class EngineMetrics {
        double numRequestsRunning;
        double numRequestsWaiting;
        double kvCacheUsagePct;
        double ttftSum;
        double ttftCount;
        // sorted histogram for this engine: le -> cumulative count
        final TreeMap<Double, Double> ttftBuckets = new TreeMap<>();
    }

    private static class InstanceMetrics {
        // sum of running/waiting across all engines on this VM
        double numRequestsRunning;
        double numRequestsWaiting;
        // average kv_cache across engines
        double kvCacheUsagePct;
        // sum of ttftSum/ttftCount across engines
        double ttftSum;
        double ttftCount;
        // P95 computed per engine then averaged - engines may have different bucket boundaries
        double p95TtftSeconds;
    }

    /**
     * Parse one VM's Prometheus output. Groups lines by engine= label,
     * computes P95 per engine independently (each engine may have different
     * bucket boundaries), then combines into a single InstanceMetrics.
     */
    private InstanceMetrics parseInstance(String modelName, String body) {
        // engine id -> per-engine metrics
        Map<String, EngineMetrics> engines = new LinkedHashMap<>();

        for (String line : body.split("\n")) {
            if (line.startsWith("#")) continue;
            if (!line.contains("model_name=\"" + modelName + "\"")) continue;

            String engineId = parseLabel(line, "engine");
            // lines without an engine label (rare) go into a synthetic "default" engine
            if (engineId == null) engineId = "default";

            EngineMetrics e = engines.computeIfAbsent(engineId, k -> new EngineMetrics());

            if (line.contains("vllm:num_requests_running")) {
                e.numRequestsRunning = parseValue(line);
            } else if (line.contains("vllm:num_requests_waiting")) {
                e.numRequestsWaiting = parseValue(line);
            } else if (line.contains("vllm:kv_cache_usage_perc")) {
                e.kvCacheUsagePct = parseValue(line);
            } else if (line.contains("vllm:time_to_first_token_seconds_sum") && !line.contains("_created")) {
                e.ttftSum = parseValue(line);
            } else if (line.contains("vllm:time_to_first_token_seconds_count") && !line.contains("_created")) {
                e.ttftCount = parseValue(line);
            } else if (line.contains("vllm:time_to_first_token_seconds_bucket")) {
                double le = parseLe(line);
                if (!Double.isNaN(le)) {
                    e.ttftBuckets.put(le, parseValue(line));
                }
            }
        }

        // combine engines into a single InstanceMetrics
        InstanceMetrics inst = new InstanceMetrics();
        double totalKv = 0;
        // weighted P95: each engine's P95 weighted by its ttftCount
        // so an engine that processed more requests has more influence
        double weightedP95Sum   = 0;
        double weightedP95Total = 0;

        for (EngineMetrics e : engines.values()) {
            inst.numRequestsRunning += e.numRequestsRunning;
            inst.numRequestsWaiting += e.numRequestsWaiting;
            totalKv                 += e.kvCacheUsagePct;
            inst.ttftSum            += e.ttftSum;
            inst.ttftCount          += e.ttftCount;

            if (!e.ttftBuckets.isEmpty() && e.ttftCount > 0) {
                double engineP95 = computeP95(e.ttftBuckets, e.ttftCount);
                weightedP95Sum   += engineP95 * e.ttftCount;
                weightedP95Total += e.ttftCount;
            }
        }

        inst.kvCacheUsagePct = engines.isEmpty() ? 0 : totalKv / engines.size();
        inst.p95TtftSeconds  = weightedP95Total > 0 ? weightedP95Sum / weightedP95Total : 0.0;

        return inst;
    }

    /**
     * Compute P95 from a single instance's histogram buckets.
     *
     * The histogram is cumulative - each bucket le=X contains the count of
     * all requests that finished in <= X seconds. To find P95:
     *   target = total_count * 0.95
     * Walk the sorted buckets until the cumulative count >= target; interpolate
     * linearly between the previous bucket boundary and the current one.
     *
     * Example (simplified):
     *   total = 200, target = 190
     *   le=0.10 → 50,  le=0.25 → 120,  le=0.50 → 195
     *   → 190 falls between le=0.25 (120) and le=0.50 (195)
     *   → fraction = (190 - 120) / (195 - 120) = 0.933
     *   → p95 = 0.25 + (0.50 - 0.25) * 0.933 = 0.483s
     */
    private double computeP95(TreeMap<Double, Double> buckets, double totalCount) {
        if (buckets.isEmpty() || totalCount <= 0) return 0.0;
        double target = totalCount * 0.95;

        double prevLe = 0.0;
        double prevCount = 0.0;

        for (Map.Entry<Double, Double> entry : buckets.entrySet()) {
            double le = entry.getKey();
            double count = entry.getValue();
            if (Double.isInfinite(le)) continue; // skip le=+Inf bucket

            if (count >= target) {
                if (count == prevCount) return prevLe; // no new requests in this band
                double fraction = (target - prevCount) / (count - prevCount);
                return prevLe + (le - prevLe) * fraction;
            }
            prevLe = le;
            prevCount = count;
        }

        // all requests fell in the last finite bucket
        return prevLe;
    }

    /**
     * Aggregate metrics across all VMs (instances):
     *
     * - numRunning / numWaiting: sum across VMs (total cluster load)
     * - kv_cache: average across VMs
     * - TTFT sum+count: summed for delta-based avg TTFT computation
     * - P95 TTFT: each VM already has its own avg-P95 (averaged across its engines);
     *   we then average those VM-level P95 values to get the cluster P95.
     */
    private VllmMetrics aggregate(String modelName, List<InstanceMetrics> snapshots, VllmMetrics prev) {
        VllmMetrics agg = new VllmMetrics();
        agg.setModelName(modelName);
        agg.setScrapedAt(System.currentTimeMillis());

        double totalRunning = 0, totalWaiting = 0, totalKv = 0;
        double totalTtftSum = 0, totalTtftCount = 0;
        // weighted P95 across VMs - a VM that processed more requests has more influence
        double weightedP95Sum   = 0;
        double weightedP95Total = 0;

        for (InstanceMetrics s : snapshots) {
            totalRunning   += s.numRequestsRunning;
            totalWaiting   += s.numRequestsWaiting;
            totalKv        += s.kvCacheUsagePct;
            totalTtftSum   += s.ttftSum;
            totalTtftCount += s.ttftCount;
            if (s.p95TtftSeconds > 0 && s.ttftCount > 0) {
                weightedP95Sum   += s.p95TtftSeconds * s.ttftCount;
                weightedP95Total += s.ttftCount;
            }
        }

        agg.setNumRequestsRunning(totalRunning);
        agg.setNumRequestsWaiting(totalWaiting);
        agg.setKvCacheUsagePct(totalKv / snapshots.size());
        agg.setTtftSum(totalTtftSum);
        agg.setTtftCount(totalTtftCount);
        agg.setP95TtftSeconds(weightedP95Total > 0 ? weightedP95Sum / weightedP95Total : 0.0);

        // delta-based recent avg TTFT across all engines of all VMs
        if (prev != null && totalTtftCount > prev.getTtftCount()) {
            double deltaSum   = totalTtftSum   - prev.getTtftSum();
            double deltaCount = totalTtftCount - prev.getTtftCount();
            agg.setRecentAvgTtftSeconds(deltaSum / deltaCount);
        } else if (totalTtftCount > 0) {
            agg.setRecentAvgTtftSeconds(totalTtftSum / totalTtftCount);
        }

        agg.setPrevTtftSum(prev != null ? prev.getTtftSum() : 0);
        agg.setPrevTtftCount(prev != null ? prev.getTtftCount() : 0);

        return agg;
    }

    private String fetchMetrics(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build();
        return httpClient.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    /** Extract the value of a Prometheus label, e.g. parseLabel(line, "engine") → "0". Returns null if absent. */
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
