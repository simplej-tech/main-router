package com.example.router.it;

import com.example.router.RouterApplication;
import com.example.router.audit.AuditService;
import com.example.router.kafka.MessageHeaders;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Layer 3 — behavioral throughput IT. Proves that activating {@code kafka-rate-limit-enabled} makes
 * the real Guava {@code routerRateLimiter} actually throttle the live consume→produce flow end-to-end
 * (EmbeddedKafka → RouterListener → produce). The encryption path stays real via a fixed-key KMS stub
 * ({@link TestEncryptionConfig}).
 *
 * <p>{@link AuditService} is mocked: that keeps DynamoDB out of the test entirely AND gives a clean
 * observability hook — {@code audit.add(...)} is invoked once per routed record, immediately after the
 * per-record {@code routerRateLimiter.acquire()}. A stubbed answer records a timestamp on each call,
 * so we get precise per-message completion times without consuming the output topics.
 *
 * <p><b>Warm-up to defeat Guava's burst.</b> {@code RateLimiter.create(rate)} is a SmoothBursty limiter
 * that banks up to {@code rate * 1.0s} permits while idle (4 at rate 4), so the first ~4 messages would
 * pass for free and corrupt a naive {@code (N-1)/rate} floor. We publish {@code WARMUP + N} messages as
 * one continuous stream; the WARMUP prefix (>= burst capacity) drains the banked permits, after which
 * the limiter is in steady state. We then measure ONLY the window between the completion of message
 * {@code #WARMUP} and {@code #(WARMUP+N)} — N permits gated one per {@code 1/rate}s, an honest floor of
 * {@code N/rate} that doesn't lean on pipeline overhead. FLOOR only (never a tight ceiling); 0.8
 * tolerance absorbs low-side jitter.
 */
@SpringBootTest(
        classes = {RouterApplication.class, TestEncryptionConfig.class},
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "spring.profiles.include=kafka-rate-limit-enabled",
                "kafka.admin.enabled=false",
                "kafka.rate-limit.router=4.0"
        })
@EmbeddedKafka(
        topics = {"requests", "standard-downstream", "social-express"},
        partitions = 1,
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        })
class RouterRateLimitThrottlingIT {

    private static final int N = 8;
    private static final int WARMUP = 6;   // >= Guava SmoothBursty burst capacity (rate * 1.0s = 4), padded
    private static final double ROUTER_RATE = 4.0;

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @MockBean
    private AuditService audit;

    @Test
    void enablingProfile_throttlesRoutingToTheConfiguredRouterRate() {
        List<Long> addNanos = Collections.synchronizedList(new ArrayList<>());
        doAnswer(inv -> {
            addNanos.add(System.nanoTime());
            return null;
        }).when(audit).add(any(), any(), any());

        // One continuous stream: WARMUP prefix drains the burst, the rest flow at the steady rate.
        for (int i = 0; i < WARMUP + N; i++) {
            publish("msg-" + i, "standard");
        }
        await().atMost(Duration.ofSeconds(40))
                .untilAsserted(() -> assertThat(addNanos).hasSize(WARMUP + N));

        // Measure only the steady-state window: completion of msg #WARMUP .. #(WARMUP+N).
        double elapsedSeconds = (addNanos.get(WARMUP + N - 1) - addNanos.get(WARMUP - 1)) / 1_000_000_000.0;

        double floorSeconds = N / ROUTER_RATE * 0.8;
        assertThat(elapsedSeconds)
                .as("steady-state routing of %d messages at %.1f/s should take at least %.2fs", N, ROUTER_RATE, floorSeconds)
                .isGreaterThanOrEqualTo(floorSeconds);
    }

    private void publish(String id, String destination) {
        ProducerRecord<String, byte[]> record =
                new ProducerRecord<>("requests", null, id, ("payload-" + id).getBytes(StandardCharsets.UTF_8));
        record.headers().add(MessageHeaders.DESTINATION, destination.getBytes(StandardCharsets.UTF_8));
        kafkaTemplate.executeInTransaction(t -> {
            try {
                return t.send(record).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
