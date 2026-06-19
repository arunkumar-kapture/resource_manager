package com.inhouse.llmqueue.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class UpdateModelConfigRequest {
    private Integer rpmLimit;
    private Double batchThresholdPct;
    private Integer maxConcurrentRequests;
    private Double p95ThresholdSeconds;
    private Double loadScoreThreshold;
    private List<String> metricsUrls;
    private Boolean active;
}
