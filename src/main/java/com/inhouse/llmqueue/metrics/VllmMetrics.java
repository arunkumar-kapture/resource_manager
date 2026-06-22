package com.inhouse.llmqueue.metrics;

import lombok.Data;

@Data
public class VllmMetrics {
    private String modelName;
    private double numRequestsRunning;
    private double numRequestsWaiting;
    private double kvCacheUsagePct;
    // cluster P95 TTFT computed from delta buckets over last scrape window
    private double p95TtftSeconds;
    private long scrapedAt;
}
