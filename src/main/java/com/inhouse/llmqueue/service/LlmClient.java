package com.inhouse.llmqueue.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

@Slf4j
@Component
public class LlmClient {

    private final String baseUrl;
    private final String authToken;
    private final int readTimeout;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public LlmClient(
            @Value("${llm.base-url}") String baseUrl,
            @Value("${llm.auth-token}") String authToken,
            @Value("${llm.connect-timeout-seconds:10}") int connectTimeout,
            @Value("${llm.read-timeout-seconds:120}") int readTimeout,
            ObjectMapper objectMapper) {
        this.baseUrl = baseUrl;
        this.authToken = authToken;
        this.readTimeout = readTimeout;
        this.objectMapper = objectMapper;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(connectTimeout))
                .build();
    }

    public Map<String, Object> chat(String modelName, Map<String, Object> payload) throws IOException, InterruptedException {
        String url = baseUrl + "/" + modelName + "/v1/chat/completions";
        String body = objectMapper.writeValueAsString(payload);

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(readTimeout))
                .header("Authorization", "Bearer " + authToken)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() >= 400) {
            throw new LlmInvocationException("LLM returned HTTP " + response.statusCode() + ": " + response.body());
        }

        return objectMapper.readValue(response.body(),
                objectMapper.getTypeFactory().constructMapType(Map.class, String.class, Object.class));
    }
}
