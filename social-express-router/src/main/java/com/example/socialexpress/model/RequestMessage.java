package com.example.socialexpress.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Wire DTO consumed off {@code social-express} — the same {id, destination, payload} envelope the
 * router forwards. Inlined per repo (wire contract), matching the sibling consumers.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RequestMessage(String id, String destination, String payload) {}
