package com.example.router;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

@SpringBootApplication
@EnableKafka
public class RouterApplication {
    public static void main(String[] args) {
        SpringApplication.run(RouterApplication.class, args);
    }
}
