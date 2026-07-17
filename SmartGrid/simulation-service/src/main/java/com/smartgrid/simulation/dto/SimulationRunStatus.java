package com.smartgrid.simulation.dto;

public record SimulationRunStatus(String runId, State state, Integer exitCode) {

    public enum State {
        RUNNING, COMPLETED, FAILED
    }
}
