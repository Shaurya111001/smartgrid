package com.smartgrid.account.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.account.dto.UserEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class UserEventProducer {

    private static final Logger log = LoggerFactory.getLogger(UserEventProducer.class);
    public static final String TOPIC = "user-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public UserEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    /**
     * Publishes synchronously: blocks until the broker acknowledges the write (or the
     * timeout/error path throws). Callers rely on this to know the event is actually durable
     * in Kafka before treating the underlying user mutation as successful — event sourcing only
     * holds if "published" means "acknowledged", not just "handed to the producer".
     */
    public void publish(UserEvent event) {
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize UserEvent: {}", event, e);
            throw new RuntimeException("Failed to serialize UserEvent", e);
        }

        try {
            SendResult<String, String> result =
                    kafkaTemplate.send(TOPIC, event.getUserId(), json).get(5, TimeUnit.SECONDS);
            var meta = result.getRecordMetadata();
            log.info("Published {} for userId={} -> {}-{}@{}",
                    event.getEventType(), event.getUserId(),
                    meta.topic(), meta.partition(), meta.offset());
        } catch (ExecutionException e) {
            log.error("Kafka rejected {} for userId={}: {}", event.getEventType(), event.getUserId(), e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            throw new RuntimeException("Failed to publish " + event.getEventType() + " for userId=" + event.getUserId(), e.getCause() != null ? e.getCause() : e);
        } catch (TimeoutException e) {
            log.error("Timed out waiting for broker ack of {} for userId={}", event.getEventType(), event.getUserId());
            throw new RuntimeException("Timed out publishing " + event.getEventType() + " for userId=" + event.getUserId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while publishing " + event.getEventType() + " for userId=" + event.getUserId(), e);
        }
    }
}
