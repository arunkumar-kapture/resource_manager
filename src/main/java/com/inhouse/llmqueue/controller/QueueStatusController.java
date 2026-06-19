package com.inhouse.llmqueue.controller;

import com.inhouse.llmqueue.entity.QueueRequest;
import com.inhouse.llmqueue.service.QueueService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
public class QueueStatusController {

    private final QueueService queueService;

    @GetMapping("/queue/{requestId}")
    public ResponseEntity<Map<String, Object>> status(@PathVariable UUID requestId) {
        return queueService.findById(requestId)
                .map(q -> {
                    Map<String, Object> body = new HashMap<>();
                    body.put("request_id", q.getId().toString());
                    body.put("status", q.getStatus().name());
                    body.put("model_name", q.getModelName());
                    body.put("mode", q.getMode().name());
                    body.put("created_at", q.getCreatedAt());
                    body.put("updated_at", q.getUpdatedAt());
                    if (q.getResult() != null) body.put("result", q.getResult());
                    if (q.getErrorMessage() != null) body.put("error", q.getErrorMessage());
                    if (q.getProcessedAt() != null) body.put("processed_at", q.getProcessedAt());
                    return ResponseEntity.ok(body);
                })
                .orElse(ResponseEntity.notFound().build());
    }
}
