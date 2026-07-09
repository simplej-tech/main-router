package com.example.e2e;

import com.example.e2e.model.DownstreamResult;
import com.example.e2e.model.RequestMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.function.BooleanSupplier;

/**
 * Drives one end-to-end run and validates the results topic. Two phases:
 * <ol>
 *   <li><b>Happy path</b> — publish standard + express messages; each must produce an {@code ok}
 *       {@link DownstreamResult} on the results topic with the right routing shape.</li>
 *   <li><b>Dead-letter</b> (optional, {@code app.e2e.dlt.enabled}) — flip downstream-service to
 *       failing via its control endpoint, publish standard messages, and assert each lands on the DLT
 *       topic with {@code outcome=error}; then reset the downstream. Kept small so bio's circuit
 *       breaker (min 5 calls) stays closed and the listener never pauses mid-phase.</li>
 * </ol>
 * Prints a per-message report and exits non-zero on any failure/timeout.
 */
@Component
public class E2eRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(E2eRunner.class);

    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final ObjectMapper mapper;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final ResultCollector collector;
    private final ConfigurableApplicationContext context;
    private final HttpClient http = HttpClient.newHttpClient();

    private final String requestsTopic;
    private final String resultsTopic;
    private final String dltTopic;
    private final int standardCount;
    private final int expressCount;
    private final int awaitSeconds;
    private final boolean dltEnabled;
    private final int dltCount;
    private final String controlUrl;

    public E2eRunner(KafkaTemplate<String, byte[]> kafkaTemplate,
                     ObjectMapper mapper,
                     KafkaListenerEndpointRegistry listenerRegistry,
                     ResultCollector collector,
                     ConfigurableApplicationContext context,
                     @Value("${app.topics.requests}") String requestsTopic,
                     @Value("${app.topics.results}") String resultsTopic,
                     @Value("${app.topics.dlt}") String dltTopic,
                     @Value("${app.e2e.standard-count}") int standardCount,
                     @Value("${app.e2e.express-count}") int expressCount,
                     @Value("${app.e2e.await-seconds}") int awaitSeconds,
                     @Value("${app.e2e.dlt.enabled}") boolean dltEnabled,
                     @Value("${app.e2e.dlt.count}") int dltCount,
                     @Value("${app.e2e.dlt.control-url}") String controlUrl) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper = mapper;
        this.listenerRegistry = listenerRegistry;
        this.collector = collector;
        this.context = context;
        this.requestsTopic = requestsTopic;
        this.resultsTopic = resultsTopic;
        this.dltTopic = dltTopic;
        this.standardCount = standardCount;
        this.expressCount = expressCount;
        this.awaitSeconds = awaitSeconds;
        this.dltEnabled = dltEnabled;
        this.dltCount = dltCount;
        this.controlUrl = controlUrl;
    }

    private enum Kind { RESULT, DEAD_LETTER }

    /** One expected message: its id, the routing destination, and where it should end up. */
    private record Expectation(String id, String destination, Kind kind) {}

    @Override
    public void run(ApplicationArguments args) {
        int exitCode = 1;
        try {
            String runId = "e2e-" + UUID.randomUUID().toString().substring(0, 8);
            List<Expectation> expectations = new ArrayList<>();

            log.info("E2E run {} — {} standard + {} express (happy); dlt={} x{}; results '{}' / dlt '{}'",
                    runId, standardCount, expressCount, dltEnabled, dltCount, resultsTopic, dltTopic);

            if (!awaitCollectorAssigned()) {
                log.error("E2E FAILED: results collector was not assigned partitions for [{}, {}] within {}s — "
                        + "is the broker reachable and are the topics created?", resultsTopic, dltTopic, awaitSeconds);
                exit(1);
                return;
            }

            // Phase 1 — happy path.
            List<Expectation> happy = buildHappyScenario(runId);
            expectations.addAll(happy);
            publish(happy);
            if (!awaitUntil(() -> happy.stream().allMatch(e -> collector.seen(e.id())), awaitSeconds)) {
                log.warn("E2E: not all happy-path results arrived within {}s — validating what did.", awaitSeconds);
            }

            // Phase 2 — dead-letter path (force downstream failures, expect DLT).
            if (dltEnabled) {
                List<Expectation> dlt = buildDltScenario(runId);
                expectations.addAll(dlt);
                setDownstreamFailing(true);
                try {
                    publish(dlt);
                    if (!awaitUntil(() -> dlt.stream().allMatch(e -> collector.seen(e.id())), awaitSeconds)) {
                        log.warn("E2E: not all dead-letter records arrived within {}s — validating what did.", awaitSeconds);
                    }
                } finally {
                    setDownstreamFailing(false);  // always reset the mock, even on error
                }
            }

            exitCode = validateAndReport(expectations);
        } catch (Exception e) {
            log.error("E2E FAILED with exception", e);
            exitCode = 1;
        }
        exit(exitCode);
    }

    private List<Expectation> buildHappyScenario(String runId) {
        List<Expectation> expectations = new ArrayList<>();
        for (int i = 0; i < standardCount; i++) {
            expectations.add(new Expectation(runId + "-std-" + i, MessageHeaders.DESTINATION_STANDARD, Kind.RESULT));
        }
        for (int i = 0; i < expressCount; i++) {
            expectations.add(new Expectation(runId + "-exp-" + i, MessageHeaders.DESTINATION_EXPRESS, Kind.RESULT));
        }
        return expectations;
    }

    private List<Expectation> buildDltScenario(String runId) {
        List<Expectation> expectations = new ArrayList<>();
        for (int i = 0; i < dltCount; i++) {
            expectations.add(new Expectation(runId + "-dlt-" + i, MessageHeaders.DESTINATION_STANDARD, Kind.DEAD_LETTER));
        }
        return expectations;
    }

    /** Publish a set of requests in one producer transaction (encrypted by the configured serializer). */
    private void publish(List<Expectation> expectations) {
        if (expectations.isEmpty()) {
            return;
        }
        kafkaTemplate.executeInTransaction(t -> {
            for (Expectation e : expectations) {
                RequestMessage message = new RequestMessage(e.id(), e.destination(), "payload-" + e.id());
                try {
                    byte[] value = mapper.writeValueAsBytes(message);
                    ProducerRecord<String, byte[]> record = new ProducerRecord<>(requestsTopic, e.id(), value);
                    record.headers().add(MessageHeaders.DESTINATION, e.destination().getBytes(StandardCharsets.UTF_8));
                    t.send(record);
                } catch (Exception ex) {
                    throw new RuntimeException("failed to publish " + e.id(), ex);
                }
            }
            return null;
        });
        log.info("E2E: published {} request(s) to '{}'", expectations.size(), requestsTopic);
    }

    private int validateAndReport(List<Expectation> expectations) {
        List<String> report = new ArrayList<>();
        int passed = 0;
        for (Expectation e : expectations) {
            String failure = validate(e);
            String tag = e.kind() == Kind.DEAD_LETTER ? e.destination() + ",dlt" : e.destination();
            if (failure == null) {
                passed++;
                report.add(String.format("  PASS  %-20s (%s)", e.id(), tag));
            } else {
                report.add(String.format("  FAIL  %-20s (%s) — %s", e.id(), tag, failure));
            }
        }

        int total = expectations.size();
        StringBuilder sb = new StringBuilder("\n========== E2E RESULT ==========\n");
        report.forEach(line -> sb.append(line).append('\n'));
        sb.append(String.format("--------------------------------%n%d/%d passed%n================================", passed, total));
        boolean allPass = passed == total;
        if (allPass) {
            log.info(sb.toString());
        } else {
            log.error(sb.toString());
        }
        return allPass ? 0 : 1;
    }

    /** @return null when the expectation is met, else a human-readable failure reason. */
    private String validate(Expectation e) {
        if (e.kind() == Kind.DEAD_LETTER) {
            if (collector.result(e.id()) != null) {
                return "expected dead-letter but landed on results topic";
            }
            DownstreamResult dl = collector.deadLetter(e.id());
            if (dl == null) {
                return "no record on dlt '" + dltTopic + "' within " + awaitSeconds + "s";
            }
            if (!DownstreamResult.OUTCOME_ERROR.equals(dl.outcome())) {
                return "dlt outcome expected error, got " + dl.outcome();
            }
            return null;
        }

        DownstreamResult dl = collector.deadLetter(e.id());
        if (dl != null) {
            return "unexpectedly dead-lettered: outcome=" + dl.outcome() + " error=" + dl.error();
        }
        DownstreamResult r = collector.result(e.id());
        if (r == null) {
            return "no result on '" + resultsTopic + "' within " + awaitSeconds + "s";
        }
        if (!DownstreamResult.OUTCOME_OK.equals(r.outcome())) {
            return "outcome=" + r.outcome() + " error=" + r.error();
        }
        if (!e.destination().equals(r.destination())) {
            return "destination mismatch: expected " + e.destination() + " got " + r.destination();
        }
        if (MessageHeaders.DESTINATION_EXPRESS.equals(e.destination())) {
            if (r.socialStatus() == null) {
                return "express result missing socialStatus";
            }
            if (r.bioStatus() != null || r.matchStatus() != null) {
                return "express result unexpectedly has bio/match fields";
            }
        } else {
            if (r.bioStatus() == null || r.matchStatus() == null) {
                return "standard result missing bio/match status";
            }
        }
        return null;
    }

    /** POST to downstream-service's /control/fail or /control/ok to force / clear failures. */
    private void setDownstreamFailing(boolean failing) {
        String path = failing ? "/control/fail" : "/control/ok";
        try {
            HttpRequest req = HttpRequest.newBuilder(URI.create(controlUrl + path))
                    .timeout(Duration.ofSeconds(5))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            HttpResponse<Void> resp = http.send(req, HttpResponse.BodyHandlers.discarding());
            log.info("E2E: downstream {} -> HTTP {}", path, resp.statusCode());
        } catch (Exception e) {
            // In the DLT phase this is fatal (we can't force failures); surface it loudly.
            throw new RuntimeException("failed to toggle downstream via " + controlUrl + path
                    + " (is downstream-service reachable? disable with app.e2e.dlt.enabled=false)", e);
        }
    }

    private boolean awaitCollectorAssigned() {
        MessageListenerContainer container = listenerRegistry.getListenerContainer("e2e-results-collector");
        if (container == null) {
            return false;
        }
        return awaitUntil(() -> {
            Collection<TopicPartition> assigned = container.getAssignedPartitions();
            if (assigned == null || assigned.isEmpty()) {
                return false;
            }
            boolean hasResults = assigned.stream().anyMatch(tp -> tp.topic().equals(resultsTopic));
            boolean hasDlt = assigned.stream().anyMatch(tp -> tp.topic().equals(dltTopic));
            return hasResults && hasDlt;
        }, awaitSeconds);
    }

    private boolean awaitUntil(BooleanSupplier condition, int timeoutSeconds) {
        long deadline = System.nanoTime() + timeoutSeconds * 1_000_000_000L;
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return true;
            }
            try {
                Thread.sleep(250);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return condition.getAsBoolean();
    }

    private void exit(int code) {
        System.exit(SpringApplication.exit(context, () -> code));
    }
}
