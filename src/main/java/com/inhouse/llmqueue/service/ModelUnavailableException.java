package com.inhouse.llmqueue.service;

public class ModelUnavailableException extends RuntimeException {
    public ModelUnavailableException(String modelName) {
        super("Model '" + modelName + "' is not active or not found");
    }
}
