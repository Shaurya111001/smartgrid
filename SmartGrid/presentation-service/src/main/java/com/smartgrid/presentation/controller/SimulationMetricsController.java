package com.smartgrid.presentation.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smartgrid.presentation.kafka.SimulationMetricsCacheService;

@RestController
@RequestMapping("/simulation-metrics")
public class SimulationMetricsController {

    private final SimulationMetricsCacheService simulationMetricsCacheService;

    public SimulationMetricsController(SimulationMetricsCacheService simulationMetricsCacheService) {
        this.simulationMetricsCacheService = simulationMetricsCacheService;
    }

    @GetMapping("/{runId}")
    public ResponseEntity<List<Map<String, Object>>> get(@PathVariable("runId") String runId) {
        var events = simulationMetricsCacheService.get(runId);
        if (events == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(events);
    }
}
