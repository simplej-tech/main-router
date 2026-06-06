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
 * Wires the single router-flow rate limiter. Only one named bean here — no composite, because the
 * router has just one flow to throttle. {@link RouterListener} injects this directly via
 * {@code @Qualifier("routerRateLimiter")}.
 *
 * <p>Two profile-gated definitions sharing the same bean name:
 * <ul>
 *   <li>profile ON  → {@link RateLimiterWrapperImpl} at {@code kafka.rate-limit.router} permits/sec.</li>
 *   <li>profile OFF → {@link DisabledRateLimiter} no-op.</li>
 * </ul>
 */
@Configuration
public class RateLimiterConfig {

    static final String ROUTER = "routerRateLimiter";

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
}
