package com.smartgrid.measurement.dto;

/**
 * Incoming request body for POST /measurements.
 * The client sends nodeId + energyValue; districtId and type
 * are resolved from the node cache.
 */
public class MeasurementRequest {

    private String nodeId;
    private double energyValue;

    public MeasurementRequest() {}

    public MeasurementRequest(String nodeId, double energyValue) {
        this.nodeId      = nodeId;
        this.energyValue = energyValue;
    }

    public String getNodeId()                        { return nodeId; }
    public void   setNodeId(String nodeId)           { this.nodeId = nodeId; }

    public double getEnergyValue()                           { return energyValue; }
    public void   setEnergyValue(double energyValue)        { this.energyValue = energyValue; }
}
