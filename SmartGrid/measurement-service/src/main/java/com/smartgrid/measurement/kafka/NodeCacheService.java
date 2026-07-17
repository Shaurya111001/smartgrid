package com.smartgrid.measurement.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class NodeCacheService {

    private static final Logger log = LoggerFactory.getLogger(NodeCacheService.class);
    private static final String TOPIC = "node-events";

    private final Map<String, NodeInfo> cache = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public NodeCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public NodeInfo getNode(String nodeId) {
        return cache.get(nodeId);
    }

    public java.util.Set<String> getAllNodeIds() {
        return cache.keySet();
    }

    public boolean nodeExists(String nodeId) {
        return cache.containsKey(nodeId);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void replayNodeEvents() {
        log.info("▶ Replaying node-events to build node cache...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "measurement-node-cache-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int replayed = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(TOPIC);
            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo pi : partitionInfos) {
                partitions.add(new TopicPartition(pi.topic(), pi.partition()));
            }

            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            int totalPolls = 0;
            while (!caughtUp(consumer, partitions, endOffsets) && totalPolls < 60) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                totalPolls++;
                for (ConsumerRecord<String, String> r : records) {
                    applyEvent(r.value());
                    replayed++;
                }
            }
        } catch (Exception e) {
            log.warn("⚠ Could not replay node-events: {}", e.getMessage());
        }

        log.info("✔ Node cache built — {} event(s) replayed, {} node(s) cached",
                 replayed, cache.size());
    }

    private boolean caughtUp(KafkaConsumer<String, String> consumer,
                              List<TopicPartition> partitions,
                              Map<TopicPartition, Long> endOffsets) {
        for (TopicPartition tp : partitions) {
            if (consumer.position(tp) < endOffsets.get(tp)) {
                return false;
            }
        }
        return true;
    }

    @KafkaListener(topics = TOPIC, groupId = "measurement-node-cache-live")
    public void consumeLiveEvent(String json) {
        log.debug("Live node event received: {}", json);
        applyEvent(json);
    }

    private void applyEvent(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            String eventType  = root.path("eventType").asText();
            String nodeId     = root.path("nodeId").asText();

            switch (eventType) {
                case "NodeCreated", "NodeUpdated" -> {
                    String districtId = root.path("districtId").asText();
                    String type       = root.path("type").asText();
                    cache.put(nodeId, new NodeInfo(districtId, type));
                }
                case "NodeDeleted" -> cache.remove(nodeId);
            }
        } catch (Exception e) {
            log.error("Failed to parse node event: {}", json, e);
        }
    }

    public record NodeInfo(String districtId, String type) {}
}
