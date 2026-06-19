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
@Tag(name = "Priority", description = "Priority requests - reserves a slot and dispatches in a single call")
public class PriorityController {

    private final PriorityService priorityService;

    @Operation(
        summary = "Submit a priority request"
    )
    @RequestBody(
        required = true,
        content = @Content(
            mediaType = "application/json",
            schema = @Schema(type = "object"),
            examples = {
                @ExampleObject(name = "First call - reserve and dispatch", value = """
                    {
                        "model": "aether-nova",
                        "reserve_resource": true,
                        "messages": [
                            { "role": "system", "content": "You are a helpful assistant." },
                            { "role": "user", "content": "Hello" }
                        ],
                        "max_tokens": 500,
                        "chat_template_kwargs": { "enable_thinking": false },
                        "temperature": 0.2
                    }
                    """),
                @ExampleObject(name = "Subsequent call - existing session", value = """
                    {
                        "model": "aether-nova",
                        "session_id": "your-session-id-from-first-call",
                        "messages": [
                            { "role": "system", "content": "You are a helpful assistant." },
                            { "role": "user", "content": "How are you?" }
                        ],
                        "max_tokens": 500,
                        "chat_template_kwargs": { "enable_thinking": false },
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

        Object reserveRaw = payload.get("reserve_resource");
        boolean reserveResource = reserveRaw instanceof Boolean b && b;

        payload.remove("reserve_resource");
        payload.remove("session_id");

        if (reserveResource) {
            // first call - check capacity, reserve slot, then dispatch
            Map<String, Object> reservation = priorityService.reserveResource(modelName);
            boolean allocated = (boolean) reservation.get("resource_allocated");
            if (!allocated) {
                return ResponseEntity.status(503).body(reservation);
            }
            // slot reserved - dispatch immediately with the session
            String sessionId = (String) reservation.get("session_id");
            Map<String, Object> response = priorityService.dispatch(modelName, payload, sessionId);
            response = new java.util.HashMap<>(response);
            response.put("session_id", sessionId);
            return ResponseEntity.ok(response);
        }

        // subsequent call - session already exists, dispatch directly
        Map<String, Object> response = priorityService.dispatch(modelName, payload, null);
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
