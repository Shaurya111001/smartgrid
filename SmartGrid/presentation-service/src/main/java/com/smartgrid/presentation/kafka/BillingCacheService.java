package com.smartgrid.presentation.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.presentation.model.UsageRecord;
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
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Replays billing-events on startup → builds Map<userId, List<UsageRecord>>.
 */
@Service
public class BillingCacheService {

    private static final Logger log = LoggerFactory.getLogger(BillingCacheService.class);
    private static final String TOPIC = "billing-events";

    private final Map<String, List<UsageRecord>> userBills = new ConcurrentHashMap<>();
    private final ObjectMapper objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public BillingCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<UsageRecord> getBillsForUser(String userId) {
        return userBills.getOrDefault(userId, Collections.emptyList());
    }

    @EventListener(ApplicationReadyEvent.class)
    @Order(2)
    public void replayBillingEvents() {
        log.info("▶ Replaying billing-events for presentation cache...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "presentation-billing-cache-" + UUID.randomUUID());
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
            log.warn("⚠ Could not replay billing-events: {}", e.getMessage());
        }

        log.info("✔ Billing cache built — {} event(s), {} user(s)", replayed, userBills.size());
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

    @KafkaListener(topics = TOPIC, groupId = "presentation-billing-cache-live")
    public void consumeLiveEvent(String json) {
        log.debug("Live billing event received: {}", json);
        applyEvent(json);
    }

    private void applyEvent(String json) {
        try {
            JsonNode root = objectMapper.readTree(json);
            String userId        = root.path("userId").asText();
            double totalUsageKwh = root.path("totalUsageKwh").asDouble();
            double cost          = root.path("cost").asDouble();
            Instant periodStart  = Instant.parse(root.path("periodStart").asText());
            Instant periodEnd    = Instant.parse(root.path("periodEnd").asText());

            UsageRecord record = new UsageRecord(userId, totalUsageKwh, cost, periodStart, periodEnd);
            userBills.computeIfAbsent(userId, k -> Collections.synchronizedList(new ArrayList<>())).add(record);
        } catch (Exception e) {
            log.error("Failed to parse billing event: {}", json, e);
        }
    }
}
