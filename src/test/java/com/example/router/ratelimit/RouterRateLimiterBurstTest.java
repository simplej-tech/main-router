package com.example.router.ratelimit;

import com.example.kafka.Profiles;
import com.example.kafka.ratelimit.RateLimiterWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Layer 1 — behavioral guard that the router's limiter is built with Guava's <em>bursty</em> factory
 * ({@code RateLimiter.create(double)} → {@code SmoothBursty}), not the warm-up variant
 * ({@code RateLimiter.create(rate, warmupPeriod, unit)} → {@code SmoothWarmingUp}). No broker — built
 * through {@link RateLimiterConfig} via {@link ApplicationContextRunner}, so it fails if anyone swaps
 * the factory in the config.
 *
 * <p><b>Why this is worth a test:</b> {@code RouterRateLimitThrottlingIT} publishes a WARMUP prefix to
 * drain the burst Guava banks while idle ({@code rate × 1.0s} permits). That entire steady-state-floor
 * strategy is only valid for a bursty limiter — a warm-up limiter banks no usable burst and would
 * silently invalidate the IT's math.
 *
 * <p><b>Why it asserts behavior, not type:</b> {@code SmoothBursty} is package-private in Guava, so
 * {@code instanceof} is impossible. We assert the defining behavior instead: after sitting idle ≥ 1s
 * the limiter has banked ~{@code rate} permits, and draining them costs ~0 wait. A {@code SmoothWarmingUp}
 * limiter would charge a ramp-up penalty on those same acquisitions.
 *
 * <p><b>Why the deliberate sleep:</b> a freshly created limiter starts with {@code storedPermits = 0};
 * bursty and warm-up are indistinguishable until permits are actually banked. The ~1.1s idle is what
 * fills the bucket and makes the difference observable. This is the one rate-limit unit test that pays
 * real wall-clock — keep it out of the fast {@link RateLimiterConfigWiringTest}.
 */
class RouterRateLimiterBurstTest {

    private static final double RATE = 5.0;

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RateLimiterConfig.class)
            .withInitializer(c -> c.getEnvironment().addActiveProfile(Profiles.KAFKA_RATE_LIMIT_ENABLED))
            .withPropertyValues("kafka.rate-limit.router=" + RATE);

    @Test
    void routerLimiterIsBursty_banksPermitsWhileIdleAndServesThemFree() {
        runner.run(ctx -> {
            RateLimiterWrapper limiter = ctx.getBean("routerRateLimiter", RateLimiterWrapper.class);

            // Let the bucket fill: SmoothBursty banks up to rate × 1.0s permits after ~1s idle.
            Thread.sleep(1_100);

            // Drain the banked burst. For SmoothBursty these are stored permits → ~0 wait each.
            // A SmoothWarmingUp limiter would charge a ramp-up penalty on these same acquisitions.
            double totalWaited = 0;
            for (int i = 0; i < (int) RATE; i++) {
                totalWaited += limiter.acquire();
            }

            assertThat(totalWaited)
                    .as("a bursty limiter serves its banked (rate × 1s) permits with no wait; "
                            + "a warm-up limiter would charge a ramp-up penalty")
                    .isLessThan(0.1);
        });
    }
}
