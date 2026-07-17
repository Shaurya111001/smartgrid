package com.smartgrid.measurement.simulator;

import com.smartgrid.measurement.dto.MeasurementRequest;
import com.smartgrid.measurement.kafka.NodeCacheService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Random;
import java.util.Set;

@Component
@Profile("simulator")
public class FakeSensorSimulator implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(FakeSensorSimulator.class);
    private static final long INTERVAL_MS = 60_000;

    private final RestTemplate restTemplate;
    private final NodeCacheService nodeCacheService;

    public FakeSensorSimulator(RestTemplateBuilder builder, NodeCacheService nodeCacheService) {
        this.restTemplate = builder.rootUri("http://localhost:8083").build();
        this.nodeCacheService = nodeCacheService;
    }

    @Override
    public void run(String... args) {
        Thread simulatorThread = new Thread(this::simulateForever, "fake-sensor-simulator");
        simulatorThread.setDaemon(true);
        simulatorThread.start();
    }

    private void simulateForever() {
        Random random = new Random();

        log.info("🔌 FakeSensorSimulator started — sending a reading for every known node every {} ms", INTERVAL_MS);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Set<String> nodeIds = nodeCacheService.getAllNodeIds();
                if (nodeIds.isEmpty()) {
                    log.debug("No nodes known yet, skipping this cycle");
                } else {
                    for (String nodeId : nodeIds) {
                        sendReading(nodeId, random);
                    }
                }
                Thread.sleep(INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Simulator interrupted, shutting down.");
                break;
            }
        }
    }

    private void sendReading(String nodeId, Random random) {
        try {
            double value = Math.round((random.nextGaussian() * 2 + 5) * 100.0) / 100.0;
            MeasurementRequest req = new MeasurementRequest(nodeId, value);

            ResponseEntity<String> response =
                    restTemplate.postForEntity("/measurements", req, String.class);

            log.info("📊 Sent reading: nodeId={}, energy={}, status={}",
                     nodeId, value, response.getStatusCode());
        } catch (Exception e) {
            log.warn("⚠ Simulator error for nodeId={} (will retry next cycle): {}", nodeId, e.getMessage());
        }
    }
}
