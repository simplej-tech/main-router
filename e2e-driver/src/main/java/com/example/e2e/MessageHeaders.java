package com.example.e2e;

/**
 * Kafka header the router routes on. Inlined per repo (wire contract shared with the router +
 * publisher). Value {@code express} → social-express; anything else → standard-downstream.
 */
public final class MessageHeaders {

    public static final String DESTINATION = "destination";
    public static final String DESTINATION_STANDARD = "standard";
    public static final String DESTINATION_EXPRESS = "express";

    private MessageHeaders() {}
}
