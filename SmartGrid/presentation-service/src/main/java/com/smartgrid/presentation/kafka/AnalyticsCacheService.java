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

    // districtId -> last known windowed-stats JSON as Map, and separately the
    // last known SOC JSON as Map. Kept apart (rather than one map both topics
    // write into) because each topic's event only carries its own fields --
    // merging into a single map on write would let whichever topic's message
    // landed last silently erase the other's fields for that district.
    private final Map<String, Map<String, Object>> windowCache = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Object>> socCache = new ConcurrentHashMap<>();

    public AnalyticsCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public Map<String, Map<String, Object>> getAll() {
        Map<String, Map<String, Object>> merged = new java.util.HashMap<>();
        for (String districtId : java.util.stream.Stream
                .concat(windowCache.keySet().stream(), socCache.keySet().stream())
                .collect(java.util.stream.Collectors.toSet())) {
            merged.put(districtId, mergeDistrict(districtId));
        }
        return merged;
    }

    public Map<String, Object> get(String districtId) {
        if (!windowCache.containsKey(districtId) && !socCache.containsKey(districtId)) {
            return null;
        }
        return mergeDistrict(districtId);
    }

    private Map<String, Object> mergeDistrict(String districtId) {
        Map<String, Object> merged = new java.util.HashMap<>();
        Map<String, Object> window = windowCache.get(districtId);
        Map<String, Object> soc = socCache.get(districtId);
        if (window != null) merged.putAll(window);
        if (soc != null) merged.putAll(soc);
        merged.put("districtId", districtId);
        return merged;
    }

    @KafkaListener(topics = "${analytics.topic.window:district-window-stats}", groupId = "presentation-analytics")
    public void consumeWindowEvent(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            String districtId = node.path("districtId").asText();
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) objectMapper.convertValue(node, Map.class);
            windowCache.put(districtId, map);
            log.debug("Updated window stats for district {}: {}", districtId, map);
        } catch (Exception e) {
            log.error("Failed to parse analytics window event: {}", json, e);
        }
    }

    @KafkaListener(topics = "${analytics.topic.soc:district-soc-state}", groupId = "presentation-analytics")
    public void consumeSocEvent(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            String districtId = node.path("districtId").asText();
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) objectMapper.convertValue(node, Map.class);
            socCache.put(districtId, map);
            log.debug("Updated SOC state for district {}: {}", districtId, map);
        } catch (Exception e) {
            log.error("Failed to parse analytics SOC event: {}", json, e);
        }
    }
}
