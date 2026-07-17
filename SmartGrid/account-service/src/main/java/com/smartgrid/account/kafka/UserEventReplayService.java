package com.smartgrid.account.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.account.dto.UserEvent;
import com.smartgrid.account.model.User;
import com.smartgrid.account.repository.UserRepository;
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
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Service
public class UserEventReplayService {

    private static final Logger log = LoggerFactory.getLogger(UserEventReplayService.class);
    private static final String TOPIC = UserEventProducer.TOPIC;

    private final UserRepository userRepository;
    private final ObjectMapper   objectMapper;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public UserEventReplayService(UserRepository userRepository, ObjectMapper objectMapper) {
        this.userRepository = userRepository;
        this.objectMapper   = objectMapper;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void replayOnStartup() {
        log.info("▶ Starting user-events replay from beginning...");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,           "account-replay-" + UUID.randomUUID());
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
            log.warn("⚠ Could not replay user-events (Kafka may not be available yet): {}", e.getMessage());
        }

        log.info("✔ Replay complete — {} event(s) applied, {} user(s) in store",
                 replayed, userRepository.count());
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
            UserEvent event = objectMapper.readValue(json, UserEvent.class);
            switch (event.getEventType()) {
                case "UserRegistered", "UserUpdated" -> {
                    User user = new User(event.getUserId(), event.getName(), event.getEmail());
                    userRepository.save(user);
                    log.debug("Replayed {} → {}", event.getEventType(), user);
                }
                case "UserDeleted" -> {
                    userRepository.deleteById(event.getUserId());
                    log.debug("Replayed UserDeleted → userId={}", event.getUserId());
                }
                default -> log.warn("Unknown event type during replay: {}", event.getEventType());
            }
        } catch (Exception e) {
            log.error("Failed to replay event: {}", json, e);
        }
    }
}
