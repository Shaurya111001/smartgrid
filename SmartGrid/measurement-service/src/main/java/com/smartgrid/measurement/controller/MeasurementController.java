package com.smartgrid.measurement.controller;

import com.smartgrid.measurement.dto.MeasurementEvent;
import com.smartgrid.measurement.dto.MeasurementRequest;
import com.smartgrid.measurement.kafka.MeasurementEventProducer;
import com.smartgrid.measurement.kafka.NodeCacheService;
import com.smartgrid.measurement.kafka.NodeCacheService.NodeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/measurements")
public class MeasurementController {

    private static final Logger log = LoggerFactory.getLogger(MeasurementController.class);

    private final MeasurementEventProducer eventProducer;
    private final NodeCacheService         nodeCacheService;

    public MeasurementController(MeasurementEventProducer eventProducer,
                                  NodeCacheService nodeCacheService) {
        this.eventProducer    = eventProducer;
        this.nodeCacheService = nodeCacheService;
    }

    @PostMapping
    public ResponseEntity<?> reportMeasurement(@RequestBody MeasurementRequest request) {

        if (!nodeCacheService.nodeExists(request.getNodeId())) {
            return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                    .body(Map.of("error", "Unknown node: " + request.getNodeId()));
        }

        NodeInfo info = nodeCacheService.getNode(request.getNodeId());

        MeasurementEvent event = new MeasurementEvent(
                request.getNodeId(),
                info.districtId(),
                info.type(),
                request.getEnergyValue()
        );

        eventProducer.publish(event);

        log.info("Measurement reported: nodeId={}, energy={}", request.getNodeId(), request.getEnergyValue());
        return ResponseEntity.status(HttpStatus.CREATED).body(event);
    }
}
