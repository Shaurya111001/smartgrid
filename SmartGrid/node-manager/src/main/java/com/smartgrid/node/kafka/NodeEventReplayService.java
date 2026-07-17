package com.smartgrid.node.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.node.dto.NodeEvent;
import com.smartgrid.node.model.Node;
import com.smartgrid.node.repository.NodeRepository;
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

@Service
public class NodeEventReplayService {

    private static final Logger log = LoggerFactory.getLogger(NodeEventReplayService.class);
    private static final String TOPIC = NodeEventProducer.TOPIC;

    private final NodeRepository nodeRepository;
    private final ObjectMapper   objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public NodeEventReplayService(NodeRepository nodeRepository, ObjectMapper objectMapper) {
        this.nodeRepository = nodeRepository;
        this.objectMapper   = objectMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    @Order(2)
    public void replayOnStartup() {
        log.info("▶ Starting node-events replay from beginning...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "node-replay-" + UUID.randomUUID());
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
                for (ConsumerRecord<String, String> record : records) {
                    applyEvent(record.value());
                    replayed++;
                }
            }
        } catch (Exception e) {
            log.warn("⚠ Could not replay node-events (Kafka may not be available): {}", e.getMessage());
        }

        log.info("✔ Node replay complete — {} event(s) applied, {} node(s) in store",
                 replayed, nodeRepository.count());
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
            NodeEvent event = objectMapper.readValue(json, NodeEvent.class);
            switch (event.getEventType()) {
                case "NodeCreated", "NodeUpdated" -> {
                    Node node = new Node(event.getNodeId(), event.getUserId(),
                                         event.getDistrictId(), event.getType());
                    nodeRepository.save(node);
                    log.debug("Replayed {} → {}", event.getEventType(), node);
                }
                case "NodeDeleted" -> {
                    nodeRepository.deleteById(event.getNodeId());
                    log.debug("Replayed NodeDeleted → nodeId={}", event.getNodeId());
                }
                default -> log.warn("Unknown event type during replay: {}", event.getEventType());
            }
        } catch (Exception e) {
            log.error("Failed to replay event: {}", json, e);
        }
    }
}
