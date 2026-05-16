package com.smartgrid.billing.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Replays node-events on startup to build a nodeId → userId lookup.
 * The billing service needs this because measurement-events carry
 * nodeId but billing is per-user.
 */
@Service
public class NodeCacheService {

    private static final Logger log = LoggerFactory.getLogger(NodeCacheService.class);
    private static final String TOPIC = "node-events";

    private final Map<String, String> nodeToUser = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public NodeCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** Returns the userId that owns this node, or null if unknown. */
    public String getUserIdForNode(String nodeId) {
        return nodeToUser.get(nodeId);
    }

    @EventListener(ApplicationReadyEvent.class)
    @Order(1)
    public void replayNodeEvents() {
        log.info("▶ Replaying node-events to build nodeId→userId cache...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "billing-node-cache-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int replayed = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    emptyPolls++;
                } else {
                    emptyPolls = 0;
                    for (ConsumerRecord<String, String> r : records) {
                        applyEvent(r.value());
                        replayed++;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("⚠ Could not replay node-events: {}", e.getMessage());
        }

        log.info("✔ Node cache built — {} event(s) replayed, {} mapping(s)", replayed, nodeToUser.size());
    }

    @KafkaListener(topics = TOPIC, groupId = "billing-node-cache-live")
    public void consumeLiveEvent(String json) {
        log.debug("Live node event received: {}", json);
        applyEvent(json);
    }

    private void applyEvent(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            String eventType = root.path("eventType").asText();
            String nodeId    = root.path("nodeId").asText();
            String userId    = root.path("userId").asText();

            switch (eventType) {
                case "NodeCreated", "NodeUpdated" -> nodeToUser.put(nodeId, userId);
                case "NodeDeleted"                -> nodeToUser.remove(nodeId);
            }
        } catch (Exception e) {
            log.error("Failed to parse node event: {}", json, e);
        }
    }
}
