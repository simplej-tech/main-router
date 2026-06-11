package com.example.router.it;

import com.example.kafka.kms.DataKeyProvider;
import com.example.kafka.kms.DataKeyResult;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

/**
 * Keeps the encryption path real (the router produces via the encrypting serializer and consumes via
 * the decrypting deserializer) but swaps the KMS-backed {@link DataKeyProvider} for a fixed-key stub,
 * so the IT needs no LocalStack/KMS. The lib's {@code dataKeyProvider} is {@code @ConditionalOnMissingBean},
 * so this wins. Mirrors downstream-router's TestEncryptionConfig.
 */
@TestConfiguration(proxyBeanMethods = false)
public class TestEncryptionConfig {

    @Bean
    public DataKeyProvider stubDataKeyProvider() {
        SecretKey key = new SecretKeySpec(new byte[32], "AES");
        byte[] encrypted = "stub-encrypted-dek".getBytes(StandardCharsets.UTF_8);
        return new DataKeyProvider() {
            @Override
            public DataKeyResult generateDataKey(String keyId) {
                return new DataKeyResult(key, encrypted);
            }

            @Override
            public SecretKey decryptDataKey(byte[] encryptedKey) {
                return key;
            }
        };
    }
}
