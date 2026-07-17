package com.smartgrid.measurement.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.measurement.dto.MeasurementEvent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class MeasurementEventProducer {

    private static final Logger log = LoggerFactory.getLogger(MeasurementEventProducer.class);
    private static final String TOPIC = "measurement-events";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public MeasurementEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                                    ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper  = objectMapper;
    }

    public void publish(MeasurementEvent event) {
        String json;
        try {
            json = objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize MeasurementEvent: {}", event, e);
            throw new RuntimeException("Failed to serialize MeasurementEvent", e);
        }

        try {
            SendResult<String, String> result =
                    kafkaTemplate.send(TOPIC, event.getDistrictId(), json).get(5, TimeUnit.SECONDS);
            var meta = result.getRecordMetadata();
            log.info("Published MeasurementReported districtId={} nodeId={} energy={} -> {}-{}@{}",
                     event.getDistrictId(), event.getNodeId(), event.getEnergyValue(),
                     meta.topic(), meta.partition(), meta.offset());
        } catch (ExecutionException e) {
            log.error("Kafka rejected measurement for nodeId={}: {}", event.getNodeId(), e.getCause() != null ? e.getCause().getMessage() : e.getMessage());
            throw new RuntimeException("Failed to publish measurement for nodeId=" + event.getNodeId(), e.getCause() != null ? e.getCause() : e);
        } catch (TimeoutException e) {
            log.error("Timed out waiting for broker ack of measurement for nodeId={}", event.getNodeId());
            throw new RuntimeException("Timed out publishing measurement for nodeId=" + event.getNodeId(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while publishing measurement for nodeId=" + event.getNodeId(), e);
        }
    }
}
