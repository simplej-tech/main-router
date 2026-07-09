package com.example.e2e;

import com.example.e2e.model.DownstreamResult;
import com.example.kafka.utils.Result;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Background collector for the results + DLT topics. Uses a UNIQUE consumer group per run (UUID
 * suffix, like the admin channel) with {@code auto-offset-reset=latest}, so a run only sees results
 * produced after it starts and never replays history. Decrypts via the same common-configs container
 * factory as any consumer and indexes the {@link DownstreamResult}s by message id.
 */
@Component
public class ResultCollector {

    private static final Logger log = LoggerFactory.getLogger(ResultCollector.class);

    private final ObjectMapper mapper;
    private final String dltTopic;

    private final Map<String, DownstreamResult> results = new ConcurrentHashMap<>();
    private final Map<String, DownstreamResult> deadLettered = new ConcurrentHashMap<>();

    public ResultCollector(ObjectMapper mapper, @Value("${app.topics.dlt}") String dltTopic) {
        this.mapper = mapper;
        this.dltTopic = dltTopic;
    }

    @KafkaListener(
            id = "e2e-results-collector",
            topics = {"${app.topics.results}", "${app.topics.dlt}"},
            groupId = "${app.e2e.group-prefix:e2e-driver}-#{T(java.util.UUID).randomUUID().toString()}")
    public void onResults(List<ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>>> records) {
        for (ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> record : records) {
            Result<byte[], Pair<Exception, byte[]>> value = record.value();
            byte[] plaintext = value == null ? null : value.contentOrNull();
            if (plaintext == null) {
                log.warn("E2E collector: undecryptable record on {} key={}", record.topic(), record.key());
                continue;
            }
            try {
                DownstreamResult result = mapper.readValue(plaintext, DownstreamResult.class);
                if (dltTopic.equals(record.topic())) {
                    deadLettered.put(result.id(), result);
                } else {
                    results.put(result.id(), result);
                }
            } catch (Exception e) {
                log.warn("E2E collector: failed to parse result on {} key={}: {}", record.topic(), record.key(), e.toString());
            }
        }
    }

    public DownstreamResult result(String id) {
        return results.get(id);
    }

    public DownstreamResult deadLetter(String id) {
        return deadLettered.get(id);
    }

    /** True once the id has landed on either the results or the DLT topic. */
    public boolean seen(String id) {
        return results.containsKey(id) || deadLettered.containsKey(id);
    }
}
