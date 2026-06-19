package com.inhouse.llmqueue.controller;

import com.inhouse.llmqueue.service.FlexService;
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
@Tag(name = "Flex", description = "Queues request if no capacity, escalates after 9s, times out at 30s")
public class FlexController {

    private final FlexService flexService;

    @Operation(summary = "Submit a flex request")
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
                    "temperature": 0.2
                }
                """)
        )
    )
    @PostMapping("/llm/flex/v1/chat/completions")
    public ResponseEntity<Map<String, Object>> handle(
            @org.springframework.web.bind.annotation.RequestBody Map<String, Object> payload) throws InterruptedException {
        String modelName = extractModelName(payload);
        Map<String, Object> response = flexService.handle(modelName, payload);
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
