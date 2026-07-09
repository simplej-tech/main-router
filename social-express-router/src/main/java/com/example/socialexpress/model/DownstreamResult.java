package com.example.socialexpress.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * Result published to the shared results topic ({@code app.topics.results}). Field-for-field
 * wire-compatible with {@code downstream-router}'s {@code DownstreamResult} so a single consumer can
 * read results from both producers. This router only calls social, so bio/match fields are always
 * null; {@code outcome} is {@link #OUTCOME_OK} on success, else {@link #OUTCOME_ERROR} with
 * {@code error} set. Inlined per repo (wire contract).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DownstreamResult(
        String id,
        String destination,
        String outcome,
        String bioStatus,
        String matchStatus,
        Double matchScore,
        String socialStatus,
        Long socialReach,
        String error,
        long processedAtEpochMs) {

    public static final String OUTCOME_OK = "ok";
    public static final String OUTCOME_ERROR = "error";
}
