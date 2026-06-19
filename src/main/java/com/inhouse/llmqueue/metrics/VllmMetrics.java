package com.inhouse.llmqueue.metrics;

import lombok.Data;

@Data
public class VllmMetrics {
    private String modelName;
    private double numRequestsRunning;
    private double numRequestsWaiting;
    private double kvCacheUsagePct;
    private double ttftSum;
    private double ttftCount;
    // previous scrape values for delta-based avg TTFT computation
    private double prevTtftSum;
    private double prevTtftCount;
    private double recentAvgTtftSeconds;
    // P95 TTFT averaged across all vLLM instances (computed each scrape cycle)
    private double p95TtftSeconds;
    private long scrapedAt;
}
