package com.smartgrid.simulation.dto;

public record SimulationRunRequest(String scenario, String strategy, int districts, int processes) {
}
