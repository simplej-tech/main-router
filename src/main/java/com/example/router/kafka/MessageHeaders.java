package com.example.router.kafka;

/**
 * Kafka header constants the router reads to decide which output topic to route to.
 * Values "standard" and "express" map to topics standard-downstream and social-express respectively;
 * anything else falls back to standard-downstream.
 */
public final class MessageHeaders {

    public static final String DESTINATION = "destination";

    public static final String DESTINATION_STANDARD = "standard";
    public static final String DESTINATION_EXPRESS = "express";

    private MessageHeaders() {}
}
