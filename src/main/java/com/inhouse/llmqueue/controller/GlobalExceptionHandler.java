package com.inhouse.llmqueue.controller;

import com.inhouse.llmqueue.service.LlmInvocationException;
import com.inhouse.llmqueue.service.ModelUnavailableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ModelUnavailableException.class)
    public ResponseEntity<Map<String, String>> handleModelUnavailable(ModelUnavailableException e) {
        return ResponseEntity.status(503).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.status(400).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(LlmInvocationException.class)
    public ResponseEntity<Map<String, String>> handleLlmError(LlmInvocationException e) {
        return ResponseEntity.status(502).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, String>> handleGeneric(Exception e) {
        log.error("Unhandled exception: {}", e.getMessage(), e);
        return ResponseEntity.status(500).body(Map.of("error", "Internal server error"));
    }
}
