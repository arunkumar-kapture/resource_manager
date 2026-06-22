package com.inhouse.llmqueue.controller;

import com.inhouse.llmqueue.service.PriorityService;
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
@Tag(name = "Priority", description = "Priority requests - capacity checked then dispatched immediately")
public class PriorityController {

    private final PriorityService priorityService;

    @Operation(summary = "Submit a priority request")
    @RequestBody(
        required = true,
        content = @Content(
            mediaType = "application/json",
            schema = @Schema(type = "object"),
            examples = {
                @ExampleObject(name = "1 - First call: request a session",
                    value = """
                        {
                            "model": "bolt-halo",
                            "resource_request": true,
                            "messages": [
                                { "role": "system", "content": "You are a helpful assistant." },
                                { "role": "user", "content": "Hello" }
                            ],
                            "max_tokens": 500,
                            "temperature": 0.2
                        }
                        """),
                @ExampleObject(name = "2 - Subsequent calls: use the session",
                    value = """
                        {
                            "model": "bolt-halo",
                            "session_id": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
                            "messages": [
                                { "role": "system", "content": "You are a helpful assistant." },
                                { "role": "user", "content": "Follow-up question" }
                            ],
                            "max_tokens": 500,
                            "temperature": 0.2
                        }
                        """)
            }
        )
    )
    @PostMapping("/llm/priority/v1/chat/completions")
    public ResponseEntity<Map<String, Object>> handle(
            @org.springframework.web.bind.annotation.RequestBody Map<String, Object> payload) {

        String modelName = extractModelName(payload);
        Map<String, Object> response = priorityService.handle(modelName, payload);
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
