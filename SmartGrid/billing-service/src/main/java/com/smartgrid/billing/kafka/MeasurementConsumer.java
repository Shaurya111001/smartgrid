package com.smartgrid.billing.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class MeasurementConsumer {

    private static final Logger log = LoggerFactory.getLogger(MeasurementConsumer.class);

    private final MeasurementReplayService replayService;
    private final ObjectMapper objectMapper;

    public MeasurementConsumer(MeasurementReplayService replayService,
                               ObjectMapper objectMapper) {
        this.replayService = replayService;
        this.objectMapper  = objectMapper;
    }

    @KafkaListener(topics = "measurement-events", groupId = "billing-live")
    public void onMeasurementReported(String message) {
        try {
            JsonNode root = objectMapper.readTree(message);
            String nodeId      = root.path("nodeId").asText();
            double energyValue = root.path("energyValue").asDouble();

            replayService.accumulate(nodeId, energyValue);
            log.debug("Accumulated measurement: nodeId={}, energy={}", nodeId, energyValue);
        } catch (Exception e) {
            log.error("Failed to process measurement event: {}", message, e);
        }
    }
}
