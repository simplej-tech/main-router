package com.example.socialexpress;

import com.example.social.SocialClient;
import com.example.social.SocialRequest;
import com.example.social.SocialResponse;
import com.example.socialexpress.model.DownstreamResult;
import com.example.socialexpress.model.RequestMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Calls the social API for one {@link RequestMessage} and folds the outcome into a
 * {@link DownstreamResult}. Runs synchronously on the listener thread so the produce of the result
 * can join the listener's Kafka transaction (exactly-once consume-process-produce).
 *
 * <p>A social failure is caught and returned as an {@link DownstreamResult#OUTCOME_ERROR} result
 * rather than thrown, so a bad response commits (with an error result) instead of rolling the batch
 * back and replaying forever. Swap the catch for a {@code DefaultErrorHandler}/DLT if you'd rather
 * retry on failure.
 */
@Component
public class SocialProcessor {

    private static final Logger log = LoggerFactory.getLogger(SocialProcessor.class);

    private final SocialClient socialClient;

    public SocialProcessor(SocialClient socialClient) {
        this.socialClient = socialClient;
    }

    public DownstreamResult process(RequestMessage message) {
        long processedAt = System.currentTimeMillis();
        try {
            SocialResponse social = socialClient.lookup(new SocialRequest(message.id(), message.payload()));
            log.info("SOCIAL-EXPRESS received id={} payload={} social={}",
                    message.id(), message.payload(), social.status());
            return new DownstreamResult(
                    message.id(), message.destination(), DownstreamResult.OUTCOME_OK,
                    null, null, null,
                    social.status(), social.reach(),
                    null, processedAt);
        } catch (Exception e) {
            log.warn("SOCIAL-EXPRESS processing failed id={}: {}", message.id(), e.toString());
            return new DownstreamResult(
                    message.id(), message.destination(), DownstreamResult.OUTCOME_ERROR,
                    null, null, null, null, null, e.getMessage(), processedAt);
        }
    }
}
