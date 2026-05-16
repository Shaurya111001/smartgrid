package com.smartgrid.measurement.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.smartgrid.measurement.dto.MeasurementEvent;

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
        try {
            String json = objectMapper.writeValueAsString(event);
            // Use districtId as key so records are partitioned by district (keeps per-district order)
            kafkaTemplate.send(TOPIC, event.getDistrictId(), json);
            log.info("Published MeasurementReported districtId={} nodeId={} energy={}",
                     event.getDistrictId(), event.getNodeId(), event.getEnergyValue());
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize MeasurementEvent: {}", event, e);
            throw new RuntimeException("Failed to serialize MeasurementEvent", e);
        }
    }
}
