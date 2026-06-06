package com.example.router;

import com.example.kafka.ratelimit.RateLimiterWrapper;
import com.example.kafka.utils.Result;
import com.example.router.kafka.MessageHeaders;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class RouterListener {

    private static final Logger log = LoggerFactory.getLogger(RouterListener.class);

    private final KafkaTemplate<String, byte[]> template;
    private final RateLimiterWrapper routerRateLimiter;
    private final String standardTopic;
    private final String expressTopic;

    public RouterListener(KafkaTemplate<String, byte[]> template,
                          @Qualifier("routerRateLimiter") RateLimiterWrapper routerRateLimiter,
                          @Value("${app.topics.standard-downstream}") String standardTopic,
                          @Value("${app.topics.social-express}") String expressTopic) {
        this.template = template;
        this.routerRateLimiter = routerRateLimiter;
        this.standardTopic = standardTopic;
        this.expressTopic = expressTopic;
    }

    @KafkaListener(topics = "${app.topics.requests}")
    public void onMessage(ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> record) {
        routerRateLimiter.acquire();

        String destination = readDestination(record);
        String target = MessageHeaders.DESTINATION_EXPRESS.equals(destination) ? expressTopic : standardTopic;

        byte[] value = record.value() == null ? null : record.value().contentOrNull();
        ProducerRecord<String, byte[]> out = new ProducerRecord<>(target, null, record.key(), value);
        record.headers().forEach(h -> out.headers().add(h));
        template.send(out);

        log.info("Routed key={} destination={} -> {}", record.key(), destination, target);
    }

    private static String readDestination(ConsumerRecord<String, Result<byte[], Pair<Exception, byte[]>>> record) {
        Header header = record.headers().lastHeader(MessageHeaders.DESTINATION);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
