package com.smartgrid.measurement.dto;

import java.time.Instant;

public class MeasurementEvent {

    private String  eventType;     // MeasurementReported
    private String  nodeId;
    private String  districtId;
    private String  type;          // PRODUCER | CONSUMER
    private double  energyValue;
    private Instant timestamp;

    public MeasurementEvent() {}

    public MeasurementEvent(String nodeId, String districtId, String type, double energyValue) {
        this.eventType   = "MeasurementReported";
        this.nodeId      = nodeId;
        this.districtId  = districtId;
        this.type        = type;
        this.energyValue = energyValue;
        this.timestamp   = Instant.now();
    }

    public String  getEventType()                    { return eventType; }
    public void    setEventType(String eventType)    { this.eventType = eventType; }

    public String  getNodeId()                       { return nodeId; }
    public void    setNodeId(String nodeId)          { this.nodeId = nodeId; }

    public String  getDistrictId()                           { return districtId; }
    public void    setDistrictId(String districtId)          { this.districtId = districtId; }

    public String  getType()                         { return type; }
    public void    setType(String type)              { this.type = type; }

    public double  getEnergyValue()                          { return energyValue; }
    public void    setEnergyValue(double energyValue)        { this.energyValue = energyValue; }

    public Instant getTimestamp()                     { return timestamp; }
    public void    setTimestamp(Instant timestamp)    { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "MeasurementEvent{nodeId='" + nodeId + "', districtId='" + districtId +
               "', type='" + type + "', energyValue=" + energyValue +
               ", timestamp=" + timestamp + '}';
    }
}
