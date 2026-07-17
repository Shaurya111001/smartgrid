package com.smartgrid.billing.kafka;

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
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Replays measurement-events on startup to rebuild the per-user
 * accumulated usage map. Also listens to new events in real-time
 * via Spring Kafka @KafkaListener (configured in MeasurementConsumer).
 */
@Service
public class MeasurementReplayService {

    private static final Logger log = LoggerFactory.getLogger(MeasurementReplayService.class);
    private static final String TOPIC = "measurement-events";

    private final Map<String, Double> userUsage = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;
    private final NodeCacheService nodeCacheService;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public MeasurementReplayService(ObjectMapper objectMapper, NodeCacheService nodeCacheService) {
        this.objectMapper     = objectMapper;
        this.nodeCacheService = nodeCacheService;
    }

    public Map<String, Double> getUserUsage() {
        return userUsage;
    }

    /**
     * Accumulate usage for a given nodeId.
     * Resolves nodeId → userId via the node cache.
     */
    public void accumulate(String nodeId, double energyValue) {
        String userId = nodeCacheService.getUserIdForNode(nodeId);
        if (userId != null) {
            userUsage.merge(userId, Math.abs(energyValue), Double::sum);
        } else {
            log.warn("Ignoring measurement for unknown nodeId={}", nodeId);
        }
    }

    /** Drain the accumulated usage map and return a snapshot. */
    public Map<String, Double> drainUsage() {
        Map<String, Double> snapshot = new ConcurrentHashMap<>(userUsage);
        userUsage.clear();
        return snapshot;
    }

    /**
     * Re-merges a previously drained amount back into the accumulator. Used when a
     * billing-events publish fails after drainUsage() already removed it, so a transient
     * Kafka error doesn't permanently drop that usage from the next billing cycle.
     */
    public void restoreUsage(String userId, double kwh) {
        userUsage.merge(userId, kwh, Double::sum);
    }

    @EventListener(ApplicationReadyEvent.class)
    @Order(2)  // After NodeCacheService
    public void replayOnStartup() {
        log.info("▶ Replaying measurement-events to rebuild usage map...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "billing-replay-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        int replayed = 0;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // Manual partition assignment instead of subscribe(): replay is a one-shot read of
            // the whole topic by a throwaway consumer, so there's no need for consumer-group
            // rebalancing -- and subscribe() loses the race between "partition assigned" and
            // "starting offset actually resolved", which can make poll() return empty results
            // even after consumer.assignment() is non-empty, fooling an empty-poll-count
            // heuristic into stopping before anything is ever read. Tracking real end offsets
            // avoids that race entirely.
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
            log.warn("⚠ Could not replay measurement-events: {}", e.getMessage());
        }

        log.info("✔ Usage replay complete — {} event(s), {} user(s) with usage",
                 replayed, userUsage.size());
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

    private void applyEvent(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            String nodeId      = root.path("nodeId").asText();
            double energyValue = root.path("energyValue").asDouble();
            accumulate(nodeId, energyValue);
        } catch (Exception e) {
            log.error("Failed to parse measurement event: {}", json, e);
        }
    }
}
