package com.smartgrid.measurement.simulator;

import com.smartgrid.measurement.dto.MeasurementRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Profile;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Random;

/**
 * Fake sensor simulator — sends random measurements to POST /measurements
 * every 5 seconds. Activate with profile "simulator":
 *
 *   mvn spring-boot:run -Dspring-boot.run.profiles=simulator
 *
 * Configure the target nodeId via the environment variable SIMULATOR_NODE_ID
 * (defaults to "n_042").
 */
@Component
@Profile("simulator")
public class FakeSensorSimulator implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(FakeSensorSimulator.class);

    private final RestTemplate restTemplate;

    public FakeSensorSimulator(RestTemplateBuilder builder) {
        this.restTemplate = builder.rootUri("http://localhost:8083").build();
    }

    @Override
    public void run(String... args) {
        String nodeId = System.getenv().getOrDefault("SIMULATOR_NODE_ID", "n_042");
        Random random = new Random();

        log.info("🔌 FakeSensorSimulator started — sending readings for nodeId={} every 5s", nodeId);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                double value = Math.round((random.nextGaussian() * 2 + 5) * 100.0) / 100.0;
                MeasurementRequest req = new MeasurementRequest(nodeId, value);

                ResponseEntity<String> response =
                        restTemplate.postForEntity("/measurements", req, String.class);

                log.info("📊 Sent reading: nodeId={}, energy={}, status={}",
                         nodeId, value, response.getStatusCode());

                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.info("Simulator interrupted, shutting down.");
                break;
            } catch (Exception e) {
                log.warn("⚠ Simulator error (will retry in 5s): {}", e.getMessage());
                try { Thread.sleep(5000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }
}
