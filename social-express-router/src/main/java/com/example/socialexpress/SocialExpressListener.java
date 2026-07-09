package com.example.socialexpress;

import com.example.kafka.utils.Result;
import com.example.socialexpress.model.DownstreamResult;
import com.example.socialexpress.model.RequestMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Consumes {@code social-express}, decrypts + parses each record, calls the social API via
 * {@link SocialProcessor}, and publishes the aggregated {@link DownstreamResult} to the shared
 * results topic ({@code app.topics.results}).
 *
 * <p>{@code @Transactional("kafkaTransactionManager")} makes consume-process-produce atomic: the
 * offset commit and the result publish share one Kafka transaction (exactly-once). The result value
 * rides the same encrypting serializer as every other message.
 */
@Component
public class SocialExpressListener {

    private static final Logger log = LoggerFactory.getLogger(SocialExpressListener.class);

    private final ObjectMapper mapper;
    private final SocialProcessor processor;
    private final KafkaTemplate<String, byte[]> kafkaTemplate;
    private final String resultsTopic;

    public SocialExpressListener(ObjectMapper mapper,
                                 SocialProcessor processor,
                                 KafkaTemplate<String, byte[]> kafkaTemplate,
                                 @Value("${app.topics.results}") String resultsTopic) {
        this.mapper = mapper;
        this.processor = processor;
        this.kafkaTemplate = kafkaTemplate;
        this.resultsTopic = resultsTopic;
    }

    @KafkaListener(id = "social-express-router", topics = "${app.topics.social-express}")
    @Transactional("kafkaTransactionManager")
    public void onBatch(List<ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>>> records) throws Exception {
        for (ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> record : records) {
            Result<byte[], Pair<Exception, byte[]>> result = record.value();
            if (result == null) {
                log.warn("SOCIAL-EXPRESS got null value for key={}", record.key());
                continue;
            }
            byte[] plaintext = result.contentOrNull();
            if (plaintext == null) {
                Pair<Exception, byte[]> err = result.exception();
                log.error("SOCIAL-EXPRESS decryption failed for key={}: {}", record.key(), err.getLeft().getMessage());
                continue;
            }
            RequestMessage message = mapper.readValue(plaintext, RequestMessage.class);
            DownstreamResult downstreamResult = processor.process(message);
            // Joins the active kafkaTransactionManager transaction; flushed atomically with the offset
            // commit when onBatch returns. Value is JSON then encrypted by the configured serializer.
            kafkaTemplate.send(resultsTopic, downstreamResult.id(), mapper.writeValueAsBytes(downstreamResult));
        }
    }
}
