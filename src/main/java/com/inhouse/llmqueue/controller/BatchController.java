package com.inhouse.llmqueue.controller;

import com.inhouse.llmqueue.service.BatchService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@Tag(name = "Batch", description = "Queues request with optional scheduled_at time, processes with 1s gap between dispatches")
public class BatchController {

    private final BatchService batchService;

    @Operation(summary = "Submit a batch request")
    @RequestBody(
        required = true,
        content = @Content(
            mediaType = "application/json",
            schema = @Schema(type = "object"),
            examples = @ExampleObject(value = """
                {
                    "model": "aether-nova",
                    "messages": [
                        { "role": "system", "content": "You are a helpful assistant." },
                        { "role": "user", "content": "Hello" }
                    ],
                    "max_tokens": 500,
                    "chat_template_kwargs": { "enable_thinking": false },
                    "temperature": 0.2,
                    "scheduled_at": "2026-06-18T10:30:00Z"
                }
                """)
        )
    )
    @PostMapping("/llm/batch/v1/chat/completions")
    public ResponseEntity<Map<String, Object>> handle(
            @org.springframework.web.bind.annotation.RequestBody Map<String, Object> payload) {
        String modelName = extractModelName(payload);
        Map<String, Object> response = batchService.handle(modelName, payload);
        if ("queued".equals(response.get("status"))) {
            return ResponseEntity.status(202).body(response);
        }
        return ResponseEntity.ok(response);
    }

    private String extractModelName(Map<String, Object> payload) {
        Object model = payload.get("model");
        if (model == null || model.toString().isBlank()) {
            throw new IllegalArgumentException("'model' field is required in the request payload");
        }
        return model.toString();
    }
}
