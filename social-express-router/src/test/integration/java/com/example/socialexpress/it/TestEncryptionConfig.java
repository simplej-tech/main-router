package com.example.socialexpress.it;

import com.example.kafka.kms.DataKeyProvider;
import com.example.kafka.kms.DataKeyResult;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;

/**
 * Keeps the encryption path real (AES-256-GCM serialize/deserialize) but swaps the KMS-backed
 * {@link DataKeyProvider} for a deterministic fixed-key stub, so the IT needs no LocalStack/KMS.
 * The lib's {@code dataKeyProvider} is {@code @ConditionalOnMissingBean}, so this wins.
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
