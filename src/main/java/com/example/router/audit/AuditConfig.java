package com.example.router.audit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.URI;

/**
 * Wires the DynamoDB Enhanced client used for router audit-log writes. Points at the same LocalStack
 * endpoint as KMS by default ({@code app.audit.dynamo.endpoint}); credentials are dummy because
 * LocalStack accepts any. The {@code routerAuditTable} bean binds {@link RouterAuditEntry} to the
 * configured table name.
 */
@Configuration
public class AuditConfig {

    @Bean
    public DynamoDbClient dynamoDbClient(
            @Value("${app.audit.dynamo.region}") String region,
            @Value("${app.audit.dynamo.endpoint}") String endpoint) {
        return DynamoDbClient.builder()
                .region(Region.of(region))
                .endpointOverride(URI.create(endpoint))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .build();
    }

    @Bean
    public DynamoDbEnhancedClient dynamoDbEnhancedClient(DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }

    @Bean
    public DynamoDbTable<RouterAuditEntry> routerAuditTable(
            DynamoDbEnhancedClient enhancedClient,
            @Value("${app.audit.dynamo.table}") String table) {
        return enhancedClient.table(table, TableSchema.fromBean(RouterAuditEntry.class));
    }
}
