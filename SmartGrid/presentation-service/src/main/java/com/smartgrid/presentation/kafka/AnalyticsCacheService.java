package com.smartgrid.presentation.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class AnalyticsCacheService {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsCacheService.class);
    private final ObjectMapper objectMapper;

    // topic to listen for windowed analytics (optional override)
    @Value("${analytics.topic.window:district-window-stats}")
    private String windowTopic;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // districtId -> last known analytics JSON as Map
    private final Map<String, Map<String, Object>> cache = new ConcurrentHashMap<>();

    public AnalyticsCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Map<String, Map<String, Object>> getAll() { return cache; }

    public Map<String, Object> get(String districtId) { return cache.get(districtId); }

    @KafkaListener(topics = "${analytics.topic.window:district-window-stats}", groupId = "presentation-analytics")
    public void consumeWindowEvent(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            String districtId = node.path("districtId").asText();
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) objectMapper.convertValue(node, Map.class);
            cache.put(districtId, map);
            log.debug("Updated analytics for district {}: {}", districtId, map);
        } catch (Exception e) {
            log.error("Failed to parse analytics event: {}", json, e);
        }
    }
}
