package com.example.e2e.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/** Wire DTO published to {@code requests} — same {id, destination, payload} envelope as publisher-cli. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RequestMessage(String id, String destination, String payload) {}
