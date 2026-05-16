package com.smartgrid.node.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.node.dto.NodeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class NodeEventProducer {

    private static final Logger log = LoggerFactory.getLogger(NodeEventProducer.class);
    private static final String TOPIC = "node-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public NodeEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                             ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    public void publish(NodeEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(TOPIC, event.getNodeId(), json);
            log.info("Published {} for nodeId={}", event.getEventType(), event.getNodeId());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize NodeEvent: {}", event, e);
            throw new RuntimeException("Failed to serialize NodeEvent", e);
        }
    }
}
