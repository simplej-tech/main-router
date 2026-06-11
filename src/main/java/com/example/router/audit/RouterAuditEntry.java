package com.example.router.audit;

import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;

/**
 * One audit-log row written when the router produces a routed message. Mapped to DynamoDB via the
 * Enhanced client's bean schema ({@code TableSchema.fromBean}). Partition key is the message key.
 */
@DynamoDbBean
public class RouterAuditEntry {

    private String id;
    private String destination;
    private String target;
    private String eventTimestamp;

    @DynamoDbPartitionKey
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getTarget() {
        return target;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public String getEventTimestamp() {
        return eventTimestamp;
    }

    public void setEventTimestamp(String eventTimestamp) {
        this.eventTimestamp = eventTimestamp;
    }
}
