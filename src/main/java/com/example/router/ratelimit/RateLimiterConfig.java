package com.example.router.ratelimit;

import com.example.kafka.Profiles;
import com.example.kafka.ratelimit.DisabledRateLimiter;
import com.example.kafka.ratelimit.RateLimiterWrapper;
import com.example.kafka.ratelimit.RateLimiterWrapperImpl;
import com.google.common.util.concurrent.RateLimiter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * Wires the router's rate limiters. Two independent flows are throttled separately:
 * <ul>
 *   <li>{@code routerRateLimiter} — the consume→produce (Kafka) path. {@link RouterListener} injects
 *       it via {@code @Qualifier("routerRateLimiter")}.</li>
 *   <li>{@code auditRateLimiter} — the DynamoDB audit-write path. {@code AuditService} injects it via
 *       {@code @Qualifier("auditRateLimiter")} and acquires a permit per flush.</li>
 * </ul>
 * No composite — the two limiters are unrelated and tuned independently. Each name has two
 * profile-gated definitions:
 * <ul>
 *   <li>profile ON  → {@link RateLimiterWrapperImpl} at {@code kafka.rate-limit.<name>} permits/sec.</li>
 *   <li>profile OFF → {@link DisabledRateLimiter} no-op.</li>
 * </ul>
 */
@Configuration
public class RateLimiterConfig {

    static final String ROUTER = "routerRateLimiter";
    static final String AUDIT = "auditRateLimiter";

    @Bean(ROUTER)
    @Profile(Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper routerRateLimiterEnabled(
            @Value("${kafka.rate-limit.router:50.0}") double permitsPerSecond) {
        return new RateLimiterWrapperImpl(RateLimiter.create(permitsPerSecond));
    }

    @Bean(ROUTER)
    @Profile("!" + Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper routerRateLimiterDisabled() {
        return new DisabledRateLimiter();
    }

    @Bean(AUDIT)
    @Profile(Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper auditRateLimiterEnabled(
            @Value("${kafka.rate-limit.audit:25.0}") double permitsPerSecond) {
        return new RateLimiterWrapperImpl(RateLimiter.create(permitsPerSecond));
    }

    @Bean(AUDIT)
    @Profile("!" + Profiles.KAFKA_RATE_LIMIT_ENABLED)
    public RateLimiterWrapper auditRateLimiterDisabled() {
        return new DisabledRateLimiter();
    }
}
