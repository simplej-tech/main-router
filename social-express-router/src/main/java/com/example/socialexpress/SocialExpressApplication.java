package com.example.socialexpress;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Entry point for the social-express-router: consumes the {@code social-express} topic, calls the
 * social API, and produces the aggregated {@code DownstreamResult} to the shared results topic.
 */
@SpringBootApplication
@EnableKafka
public class SocialExpressApplication {

    public static void main(String[] args) {
        SpringApplication.run(SocialExpressApplication.class, args);
    }
}
