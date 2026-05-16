package com.smartgrid.presentation.controller;

import java.util.List;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.smartgrid.presentation.kafka.AnalyticsCacheService;

@RestController
@RequestMapping("/analytics")
public class AnalyticsController {

    private final AnalyticsCacheService analyticsCacheService;

    public AnalyticsController(AnalyticsCacheService analyticsCacheService) {
        this.analyticsCacheService = analyticsCacheService;
    }

    @GetMapping("/districts")
    public ResponseEntity<List<Map<String, Object>>> getAll() {
        return ResponseEntity.ok(List.copyOf(analyticsCacheService.getAll().values()));
    }

    @GetMapping("/districts/{id}")
    public ResponseEntity<Map<String, Object>> get(@PathVariable("id") String districtId) {
        var v = analyticsCacheService.get(districtId);
        if (v == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(v);
    }
}
