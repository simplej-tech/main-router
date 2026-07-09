package com.example.e2e.model;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * The result envelope both {@code downstream-router} and {@code social-express-router} publish to the
 * results topic. Wire-compatible copy (inlined per repo) so the driver can deserialize and validate.
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
