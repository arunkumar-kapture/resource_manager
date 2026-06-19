package com.inhouse.llmqueue;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class LlmQueueApplication {
    public static void main(String[] args) {
        SpringApplication.run(LlmQueueApplication.class, args);
    }
}
