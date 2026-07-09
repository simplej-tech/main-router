package com.example.socialexpress.it;

import com.example.kafka.utils.Result;
import com.example.socialexpress.SocialExpressApplication;
import com.example.socialexpress.model.DownstreamResult;
import com.example.socialexpress.model.RequestMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end IT for the social-express-router: publishes an encrypted {@link RequestMessage} to
 * {@code social-express}, the app calls the (WireMock-stubbed) social API, and the app produces an
 * encrypted {@link DownstreamResult} to the shared results topic. A collector {@code @KafkaListener}
 * reads the results topic (decrypting via the same lib config) and the test asserts the aggregate.
 */
@SpringBootTest(
        classes = {SocialExpressApplication.class, TestEncryptionConfig.class,
                SocialExpressRouterIT.ResultsCollectorConfig.class},
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "kafka.admin.enabled=false"
        })
@EmbeddedKafka(
        topics = {"social-express", "downstream-results"},
        partitions = 1,
        brokerProperties = {
                "transaction.state.log.replication.factor=1",
                "transaction.state.log.min.isr=1",
                "offsets.topic.replication.factor=1"
        })
class SocialExpressRouterIT {

    private static final WireMockServer WIREMOCK = new WireMockServer(options().dynamicPort());
    static final LinkedBlockingQueue<DownstreamResult> RESULTS = new LinkedBlockingQueue<>();

    static {
        WIREMOCK.start();
    }

    @AfterAll
    static void stopWiremock() {
        WIREMOCK.stop();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("app.social.base-url", () -> "http://localhost:" + WIREMOCK.port());
    }

    @Autowired
    private KafkaTemplate<String, byte[]> kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void encryptedMessageOnSocialExpress_producesAggregatedResult() throws Exception {
        RESULTS.clear();
        WIREMOCK.stubFor(post(urlPathEqualTo("/social")).willReturn(aResponse()
                .withStatus(200)
                .withHeader("Content-Type", "application/json")
                .withBody("{\"id\":\"resp\",\"status\":\"ok\",\"reach\":1000}")));

        publish("exp-1", "payload-exp-1");

        DownstreamResult result = await().atMost(Duration.ofSeconds(20))
                .until(RESULTS::poll, r -> r != null);

        assertThat(result.id()).isEqualTo("exp-1");
        assertThat(result.destination()).isEqualTo("express");
        assertThat(result.outcome()).isEqualTo(DownstreamResult.OUTCOME_OK);
        assertThat(result.socialStatus()).isEqualTo("ok");
        assertThat(result.socialReach()).isEqualTo(1000L);
        // Only social is called by this router — bio/match stay null.
        assertThat(result.bioStatus()).isNull();
        assertThat(result.matchStatus()).isNull();
        assertThat(result.error()).isNull();
    }

    private void publish(String id, String payload) throws Exception {
        RequestMessage message = new RequestMessage(id, "express", payload);
        byte[] bytes = objectMapper.writeValueAsBytes(message);
        // Transactional producer: a bare send() outside a txn throws, so wrap it.
        kafkaTemplate.executeInTransaction(t -> {
            try {
                return t.send("social-express", id, bytes).get(10, TimeUnit.SECONDS);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** Reads the encrypted results topic via the app's (decrypting, batch) container factory. */
    @TestConfiguration(proxyBeanMethods = false)
    static class ResultsCollectorConfig {
        @Bean
        ResultsCollector resultsCollector(ObjectMapper mapper) {
            return new ResultsCollector(mapper);
        }
    }

    static class ResultsCollector {
        private final ObjectMapper mapper;

        ResultsCollector(ObjectMapper mapper) {
            this.mapper = mapper;
        }

        @KafkaListener(id = "results-collector", topics = "${app.topics.results}", groupId = "results-collector")
        public void onResults(List<ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>>> records) throws Exception {
            for (ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> record : records) {
                byte[] plaintext = record.value() == null ? null : record.value().contentOrNull();
                if (plaintext != null) {
                    RESULTS.add(mapper.readValue(plaintext, DownstreamResult.class));
                }
            }
        }
    }
}
