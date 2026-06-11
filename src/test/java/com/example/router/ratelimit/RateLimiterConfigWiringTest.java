package com.example.router.ratelimit;

import com.example.kafka.Profiles;
import com.example.kafka.ratelimit.DisabledRateLimiter;
import com.example.kafka.ratelimit.RateLimiterWrapper;
import com.example.kafka.ratelimit.RateLimiterWrapperImpl;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Layer 1 — wiring/config test (no broker). Validates the {@code kafka-rate-limit-enabled} profile
 * selects the right bean type for both independent limiters (router flow + audit flow) and that the
 * {@code kafka.rate-limit.*} properties bind to the Guava limiter's rate. Guava's throttling behavior
 * itself is covered by common-configs' {@code RateLimiterWrapperImplTest}; not retested here.
 */
class RateLimiterConfigWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withUserConfiguration(RateLimiterConfig.class);

    @Test
    void profileOff_bothLimitersAreDisabledNoOps() {
        runner.run(ctx -> {
            assertThat(ctx.getBean("routerRateLimiter", RateLimiterWrapper.class)).isInstanceOf(DisabledRateLimiter.class);
            assertThat(ctx.getBean("auditRateLimiter", RateLimiterWrapper.class)).isInstanceOf(DisabledRateLimiter.class);
        });
    }

    @Test
    void profileOn_bothLimitersAreGuavaBacked_withRatesFromProperties() {
        runner.withInitializer(c -> c.getEnvironment().addActiveProfile(Profiles.KAFKA_RATE_LIMIT_ENABLED))
                .withPropertyValues(
                        "kafka.rate-limit.router=900.0",
                        "kafka.rate-limit.audit=40.0")
                .run(ctx -> {
                    RateLimiterWrapper router = ctx.getBean("routerRateLimiter", RateLimiterWrapper.class);
                    RateLimiterWrapper audit = ctx.getBean("auditRateLimiter", RateLimiterWrapper.class);

                    assertThat(router).isInstanceOf(RateLimiterWrapperImpl.class);
                    assertThat(audit).isInstanceOf(RateLimiterWrapperImpl.class);
                    assertThat(router.getRate()).isEqualTo(900.0);
                    assertThat(audit.getRate()).isEqualTo(40.0);
                });
    }

    @Test
    void profileOn_ratesFallBackToYmlDefaultsWhenUnset() {
        runner.withInitializer(c -> c.getEnvironment().addActiveProfile(Profiles.KAFKA_RATE_LIMIT_ENABLED))
                .run(ctx -> {
                    // RateLimiterConfig declares ${kafka.rate-limit.router:50.0} / ${...audit:25.0}.
                    assertThat(ctx.getBean("routerRateLimiter", RateLimiterWrapper.class).getRate()).isEqualTo(50.0);
                    assertThat(ctx.getBean("auditRateLimiter", RateLimiterWrapper.class).getRate()).isEqualTo(25.0);
                });
    }
}
