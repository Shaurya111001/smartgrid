package com.smartgrid.simulation.service;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.smartgrid.simulation.dto.SimulationRunRequest;
import com.smartgrid.simulation.dto.SimulationRunStatus;
import com.smartgrid.simulation.dto.SimulationRunStatus.State;

@Service
public class SimulationRunnerService {

    private static final Logger log = LoggerFactory.getLogger(SimulationRunnerService.class);

    private static final Set<String> ALLOWED_SCENARIOS = Set.of("sparse", "dense");
    private static final Set<String> ALLOWED_STRATEGIES = Set.of("roundrobin", "weighted", "nodepartition");
    private static final int MIN_DISTRICTS = 1;
    private static final int MAX_DISTRICTS = 20;
    private static final int MIN_PROCESSES = 1;
    private static final int MAX_PROCESSES = 8;

    @Value("${simulation.binary-path}")
    private String binaryPath;

    @Value("${simulation.working-dir}")
    private String workingDir;

    @Value("${simulation.kafka-bootstrap-servers}")
    private String kafkaBootstrapServers;

    // runId -> current status. In-memory only: run history doesn't need to
    // survive a restart of this service, it's a live trigger/monitor, not a
    // system of record (the simulation's own output already lands in Kafka).
    private final Map<String, SimulationRunStatus> runs = new ConcurrentHashMap<>();

    public String startRun(SimulationRunRequest request) {
        validate(request);

        String runId = UUID.randomUUID().toString();
        runs.put(runId, new SimulationRunStatus(runId, State.RUNNING, null));

        // --allow-run-as-root is required because the container runs mpirun as
        // root; it's a no-op (and harmless) when not running as root, so it's
        // always included rather than conditioned on the current user.
        List<String> command = List.of(
                "mpirun", "-np", String.valueOf(request.processes()), "--oversubscribe", "--allow-run-as-root",
                binaryPath, request.scenario(), request.strategy(), String.valueOf(request.districts()));

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.directory(new File(workingDir));
        processBuilder.environment().put("KAFKA_BOOTSTRAP_SERVERS", kafkaBootstrapServers);
        processBuilder.environment().put("SIMULATION_RUN_ID", runId);
        processBuilder.redirectErrorStream(true);

        // Run on its own thread so this HTTP request can return immediately with
        // the runId; the frontend polls getStatus()/the metrics topic afterward
        // rather than holding a connection open for the whole simulation.
        Thread runnerThread = new Thread(() -> execute(runId, processBuilder), "simulation-run-" + runId);
        runnerThread.setDaemon(true);
        runnerThread.start();

        return runId;
    }

    private void execute(String runId, ProcessBuilder processBuilder) {
        // Keep only the tail so a long run can't grow this unbounded; it exists
        // purely to explain a non-zero exit code (mpirun/the binary write their
        // errors to stdout/stderr, both merged here via redirectErrorStream).
        java.util.ArrayDeque<String> tailOutput = new java.util.ArrayDeque<>();
        try {
            Process process = processBuilder.start();
            try (var reader = process.inputReader()) {
                reader.lines().forEach(line -> {
                    log.debug("[{}] {}", runId, line);
                    tailOutput.addLast(line);
                    if (tailOutput.size() > 20) tailOutput.removeFirst();
                });
            }
            int exitCode = process.waitFor();
            runs.put(runId, new SimulationRunStatus(runId, exitCode == 0 ? State.COMPLETED : State.FAILED, exitCode));
            if (exitCode == 0) {
                log.info("Simulation run {} finished with exit code {}", runId, exitCode);
            } else {
                log.warn("Simulation run {} failed with exit code {}. Last output:\n{}",
                        runId, exitCode, String.join("\n", tailOutput));
            }
        } catch (IOException e) {
            log.error("Failed to launch simulation run {}", runId, e);
            runs.put(runId, new SimulationRunStatus(runId, State.FAILED, null));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Simulation run {} interrupted", runId);
            runs.put(runId, new SimulationRunStatus(runId, State.FAILED, null));
        }
    }

    public SimulationRunStatus getStatus(String runId) {
        return runs.get(runId);
    }

    private void validate(SimulationRunRequest request) {
        if (request.scenario() == null || !ALLOWED_SCENARIOS.contains(request.scenario())) {
            throw new IllegalArgumentException("scenario must be one of " + ALLOWED_SCENARIOS);
        }
        if (request.strategy() == null || !ALLOWED_STRATEGIES.contains(request.strategy())) {
            throw new IllegalArgumentException("strategy must be one of " + ALLOWED_STRATEGIES);
        }
        if (request.districts() < MIN_DISTRICTS || request.districts() > MAX_DISTRICTS) {
            throw new IllegalArgumentException("districts must be between " + MIN_DISTRICTS + " and " + MAX_DISTRICTS);
        }
        if (request.processes() < MIN_PROCESSES || request.processes() > MAX_PROCESSES) {
            throw new IllegalArgumentException("processes must be between " + MIN_PROCESSES + " and " + MAX_PROCESSES);
        }
    }
}
