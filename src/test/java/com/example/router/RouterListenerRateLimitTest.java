package com.example.router;

import com.example.kafka.ratelimit.RateLimiterWrapper;
import com.example.kafka.utils.Result;
import com.example.router.audit.AuditService;
import com.example.router.kafka.MessageHeaders;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Layer 2 — flow-logic test (no broker, no timing). Drives {@code onMessage} with a hand-built batch
 * and verifies the rate limiter is acquired exactly ONCE PER RECORD (not once per batch), proving the
 * per-record placement inside the loop survives the batch-listener change. Also pins the audit
 * staging contract: one {@code add} per record, one {@code persist} per batch.
 */
@SuppressWarnings("unchecked")
class RouterListenerRateLimitTest {

    @Test
    void acquiresOneRouterPermitPerRecord_regardlessOfBatchSize() {
        KafkaTemplate<String, byte[]> template = mock(KafkaTemplate.class);
        RateLimiterWrapper routerRateLimiter = mock(RateLimiterWrapper.class);
        AuditService audit = mock(AuditService.class);

        RouterListener listener = new RouterListener(
                template, routerRateLimiter, audit, "standard-downstream", "social-express");

        listener.onMessage(List.of(rec("a", "standard"), rec("b", "express"), rec("c", null)));

        // One permit per record — the throttle is inside the loop, so batch size doesn't dilute it.
        verify(routerRateLimiter, times(3)).acquire();
        // Each record produced.
        verify(template, times(3)).send(any(ProducerRecord.class));
        // Audit staged once per record, flushed once for the whole batch.
        verify(audit, times(3)).add(any(), any(), any());
        verify(audit, times(1)).persist();
    }

    private static ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> rec(String key, String destination) {
        ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> record = new ConsumerRecord<>(
                "requests", 0, 0L, key, Result.valid(("payload-" + key).getBytes(StandardCharsets.UTF_8)));
        if (destination != null) {
            record.headers().add(MessageHeaders.DESTINATION, destination.getBytes(StandardCharsets.UTF_8));
        }
        return record;
    }
}
