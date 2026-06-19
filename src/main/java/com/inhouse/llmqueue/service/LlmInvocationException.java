package com.inhouse.llmqueue.service;

public class LlmInvocationException extends RuntimeException {
    public LlmInvocationException(String message) {
        super(message);
    }
}
