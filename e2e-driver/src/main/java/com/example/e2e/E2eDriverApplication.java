package com.example.e2e;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * End-to-end driver/validator. Publishes an encrypted scenario to the {@code requests} topic, then
 * consumes + decrypts the results topic (and DLT) and validates that each published message produced
 * the expected {@code DownstreamResult}. Runs against an already-running stack (local compose or a
 * deployed environment) — everything is configured via {@code KAFKA_BOOTSTRAP} / {@code KMS_ENDPOINT}
 * / topic env vars. Exits non-zero if any expectation fails or times out.
 */
@SpringBootApplication
@EnableKafka
public class E2eDriverApplication {

    public static void main(String[] args) {
        SpringApplication.run(E2eDriverApplication.class, args);
    }
}
