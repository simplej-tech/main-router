package com.example.router.audit;

import com.example.kafka.ratelimit.RateLimiterWrapper;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Stages and flushes router audit-log entries to DynamoDB.
 *
 * <p>{@link #add} stages one entry; {@link #persist} batch-writes everything staged and clears the
 * buffer. Staging is per-thread (a {@link ThreadLocal}) so concurrent listener threads never share a
 * batch — each {@code onMessage} invocation owns its own add/persist cycle.
 *
 * <p><b>Not transactional with Kafka.</b> These writes go straight to DynamoDB and are NOT enrolled
 * in the {@code kafkaTransactionManager} transaction. If the Kafka producer transaction later aborts,
 * any rows already persisted here are not rolled back.
 */
@Component
public class AuditService {

    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<RouterAuditEntry> table;
    private final RateLimiterWrapper auditRateLimiter;

    private final ThreadLocal<List<RouterAuditEntry>> staged = ThreadLocal.withInitial(ArrayList::new);

    public AuditService(DynamoDbEnhancedClient enhancedClient,
                        DynamoDbTable<RouterAuditEntry> table,
                        @Qualifier("auditRateLimiter") RateLimiterWrapper auditRateLimiter) {
        this.enhancedClient = enhancedClient;
        this.table = table;
        this.auditRateLimiter = auditRateLimiter;
    }

    /** Stage a "Produced" audit entry for a routed message. */
    public void add(String key, String destination, String target) {
        RouterAuditEntry entry = new RouterAuditEntry();
        entry.setId(key);
        entry.setDestination(destination);
        entry.setTarget(target);
        entry.setEventTimestamp(Instant.now().toString());
        staged.get().add(entry);
    }

    /** Batch-write everything staged on this thread, then clear the buffer. No-op if nothing staged. */
    public void persist() {
        List<RouterAuditEntry> entries = staged.get();
        if (entries.isEmpty()) {
            staged.remove();
            return;
        }
        // Throttle the Dynamo write path independently of routing. One permit per flush (per batch);
        // see RateLimiterConfig#auditRateLimiter and kafka.rate-limit.audit.
        auditRateLimiter.acquire();
        try {
            WriteBatch.Builder<RouterAuditEntry> batch = WriteBatch.builder(RouterAuditEntry.class)
                    .mappedTableResource(table);
            entries.forEach(batch::addPutItem);
            enhancedClient.batchWriteItem(BatchWriteItemEnhancedRequest.builder()
                    .addWriteBatch(batch.build())
                    .build());
        } finally {
            staged.remove();
        }
    }
}
