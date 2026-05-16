package com.smartgrid.node.kafka;

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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Replays the user-events topic on startup to build an in-memory
 * set of known userIds. Used by NodeController to validate that
 * a user exists before creating a node.
 */
@Service
public class UserCacheService {

    private static final Logger log = LoggerFactory.getLogger(UserCacheService.class);
    private static final String TOPIC = "user-events";

    private final Set<String> knownUserIds = ConcurrentHashMap.newKeySet();
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public UserCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public boolean userExists(String userId) {
        return knownUserIds.contains(userId);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void replayUserEvents() {
        log.info("▶ Replaying user-events to build user cache...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "node-user-cache-" + UUID.randomUUID());
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
                    for (ConsumerRecord<String, String> record : records) {
                        applyUserEvent(record.value());
                        replayed++;
                    }
                }
            }
        } catch (Exception e) {
            log.warn("⚠ Could not replay user-events (Kafka may not be available): {}", e.getMessage());
        }

        log.info("✔ User cache built — {} event(s) replayed, {} known user(s)",
                 replayed, knownUserIds.size());
    }

    @KafkaListener(topics = TOPIC, groupId = "node-manager-user-cache")
    public void consumeLiveEvent(String json) {
        log.debug("Live user event received: {}", json);
        applyUserEvent(json);
    }

    private void applyUserEvent(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            String eventType = root.path("eventType").asText();
            String userId    = root.path("userId").asText();

            switch (eventType) {
                case "UserRegistered", "UserUpdated" -> knownUserIds.add(userId);
                case "UserDeleted"                   -> knownUserIds.remove(userId);
            }
        } catch (Exception e) {
            log.error("Failed to parse user event: {}", json, e);
        }
    }
}
