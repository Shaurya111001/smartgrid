package com.smartgrid.presentation.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.presentation.model.NodeView;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Replays node-events on startup → builds Map<userId, List<NodeView>>.
 */
@Service
public class NodeCacheService {

    private static final Logger log = LoggerFactory.getLogger(NodeCacheService.class);
    private static final String TOPIC = "node-events";

    /** userId → list of nodes belonging to that user */
    private final Map<String, List<NodeView>> userNodes = new ConcurrentHashMap<>();
    /** nodeId → NodeView for quick lookup on updates/deletes */
    private final Map<String, NodeView> nodeIndex = new ConcurrentHashMap<>();

    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public NodeCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<NodeView> getNodesForUser(String userId) {
        return userNodes.getOrDefault(userId, Collections.emptyList());
    }

    @EventListener(ApplicationReadyEvent.class)
    @Order(1)
    public void replayNodeEvents() {
        log.info("▶ Replaying node-events for presentation cache...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "presentation-node-cache-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int replayed = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            int emptyPolls = 0;
            int totalPolls = 0;
            while (emptyPolls < 3 && totalPolls < 60) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                totalPolls++;
                if (consumer.assignment().isEmpty()) {
                    continue; // still rebalancing, don't count towards catch-up yet
                }
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

        log.info("✔ Node cache built — {} event(s), {} user(s)", replayed, userNodes.size());
    }

    @KafkaListener(topics = TOPIC, groupId = "presentation-node-cache-live")
    public void consumeLiveEvent(String json) {
        log.debug("Live node event received: {}", json);
        applyEvent(json);
    }

    private void applyEvent(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            String eventType  = root.path("eventType").asText();
            String nodeId     = root.path("nodeId").asText();
            String userId     = root.path("userId").asText();
            String districtId = root.path("districtId").asText();
            String type       = root.path("type").asText();

            switch (eventType) {
                case "NodeCreated" -> {
                    NodeView nv = new NodeView(nodeId, userId, districtId, type);
                    nodeIndex.put(nodeId, nv);
                    userNodes.computeIfAbsent(userId, k -> Collections.synchronizedList(new ArrayList<>())).add(nv);
                }
                case "NodeUpdated" -> {
                    // Remove old, add updated
                    NodeView old = nodeIndex.get(nodeId);
                    if (old != null) {
                        List<NodeView> list = userNodes.get(old.getUserId());
                        if (list != null) list.removeIf(n -> n.getNodeId().equals(nodeId));
                    }
                    NodeView nv = new NodeView(nodeId, userId, districtId, type);
                    nodeIndex.put(nodeId, nv);
                    userNodes.computeIfAbsent(userId, k -> Collections.synchronizedList(new ArrayList<>())).add(nv);
                }
                case "NodeDeleted" -> {
                    NodeView old = nodeIndex.remove(nodeId);
                    if (old != null) {
                        List<NodeView> list = userNodes.get(old.getUserId());
                        if (list != null) list.removeIf(n -> n.getNodeId().equals(nodeId));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse node event: {}", json, e);
        }
    }
}
