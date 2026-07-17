package com.smartgrid.presentation.kafka;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class SimulationMetricsCacheService {

    private static final Logger log = LoggerFactory.getLogger(SimulationMetricsCacheService.class);

    // Bound how many step/rank events we keep per run so a long-running or
    // repeatedly-triggered simulation can't grow this cache unbounded.
    private static final int MAX_EVENTS_PER_RUN = 500;

    private final ObjectMapper objectMapper;

    // runId -> ordered list of per-step/per-rank metric events
    private final Map<String, List<Map<String, Object>>> cache = new ConcurrentHashMap<>();

    public SimulationMetricsCacheService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public List<Map<String, Object>> get(String runId) {
        List<Map<String, Object>> events = cache.get(runId);
        if (events == null) {
            return null;
        }
        return events.stream()
                .sorted(Comparator
                        .comparingInt((Map<String, Object> e) -> ((Number) e.getOrDefault("step", 0)).intValue())
                        .thenComparingInt(e -> ((Number) e.getOrDefault("rank", 0)).intValue()))
                .toList();
    }

    @KafkaListener(topics = "${simulation.topic.metrics:simulation-metrics}", groupId = "presentation-simulation-metrics")
    public void consumeMetricsEvent(String json) {
        try {
            JsonNode node = objectMapper.readTree(json);
            String runId = node.path("runId").asText();
            if (runId.isEmpty()) {
                log.warn("Ignoring simulation-metrics event without runId: {}", json);
                return;
            }
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) objectMapper.convertValue(node, Map.class);
            List<Map<String, Object>> events = cache.computeIfAbsent(runId, k -> new CopyOnWriteArrayList<>());
            events.add(map);
            while (events.size() > MAX_EVENTS_PER_RUN) {
                events.remove(0);
            }
            log.debug("Recorded simulation metric for run {}: {}", runId, map);
        } catch (Exception e) {
            log.error("Failed to parse simulation-metrics event: {}", json, e);
        }
    }
}
