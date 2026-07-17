package com.smartgrid.simulation.controller;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.smartgrid.simulation.dto.SimulationRunRequest;
import com.smartgrid.simulation.dto.SimulationRunStatus;
import com.smartgrid.simulation.service.SimulationRunnerService;

@RestController
@RequestMapping("/simulations")
public class SimulationController {

    private static final Logger log = LoggerFactory.getLogger(SimulationController.class);

    private final SimulationRunnerService simulationRunnerService;

    public SimulationController(SimulationRunnerService simulationRunnerService) {
        this.simulationRunnerService = simulationRunnerService;
    }

    @PostMapping
    public ResponseEntity<?> startRun(@RequestBody SimulationRunRequest request) {
        try {
            String runId = simulationRunnerService.startRun(request);
            log.info("Started simulation run {}: {}", runId, request);
            return ResponseEntity.status(HttpStatus.ACCEPTED)
                    .body(simulationRunnerService.getStatus(runId));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/{runId}")
    public ResponseEntity<SimulationRunStatus> getStatus(@PathVariable("runId") String runId) {
        SimulationRunStatus status = simulationRunnerService.getStatus(runId);
        if (status == null) return ResponseEntity.notFound().build();
        return ResponseEntity.ok(status);
    }
}
