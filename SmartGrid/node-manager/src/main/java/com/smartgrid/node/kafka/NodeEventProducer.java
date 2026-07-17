package com.smartgrid.node.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.node.dto.NodeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class NodeEventProducer {

    private static final Logger log = LoggerFactory.getLogger(NodeEventProducer.class);
    public static final String TOPIC = "node-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public NodeEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    public void publish(NodeEvent event) {
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize NodeEvent: {}", event, e);
            throw new RuntimeException("Failed to serialize NodeEvent", e);
        }

        try {
            SendResult<String, String> result =
                    kafkaTemplate.send(TOPIC, event.getNodeId(), json).get(5, TimeUnit.SECONDS);
            var meta = result.getRecordMetadata();
            log.info("Published {} for nodeId={} -> {}-{}@{}",
                    event.getEventType(), event.getNodeId(), meta.topic(), meta.partition(), meta.offset());
        } catch (ExecutionException e) {
            log.error("Kafka rejected {} for nodeId={}: {}", event.getEventType(), event.getNodeId(), e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            throw new RuntimeException("Failed to publish " + event.getEventType() + " for nodeId=" + event.getNodeId(), e.getCause() != null ? e.getCause() : e);
        } catch (TimeoutException e) {
            log.error("Timed out waiting for broker ack of {} for nodeId={}", event.getEventType(), event.getNodeId());
            throw new RuntimeException("Timed out publishing " + event.getEventType() + " for nodeId=" + event.getNodeId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while publishing " + event.getEventType() + " for nodeId=" + event.getNodeId(), e);
        }
    }
}
