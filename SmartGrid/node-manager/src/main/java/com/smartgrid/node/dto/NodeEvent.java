package com.smartgrid.node.dto;

import java.time.Instant;

public class NodeEvent {

    private String  eventType;
    private String  nodeId;
    private String  userId;
    private String  districtId;
    private String  type;
    private Instant timestamp;

    public NodeEvent() {}

    public NodeEvent(String eventType, String nodeId, String userId,
                     String districtId, String type) {
        this.eventType  = eventType;
        this.nodeId     = nodeId;
        this.userId     = userId;
        this.districtId = districtId;
        this.type       = type;
        this.timestamp  = Instant.now();
    }

    public String  getEventType()                    { return eventType; }
    public void    setEventType(String eventType)    { this.eventType = eventType; }

    public String  getNodeId()                       { return nodeId; }
    public void    setNodeId(String nodeId)          { this.nodeId = nodeId; }

    public String  getUserId()                       { return userId; }
    public void    setUserId(String userId)          { this.userId = userId; }

    public String  getDistrictId()                           { return districtId; }
    public void    setDistrictId(String districtId)          { this.districtId = districtId; }

    public String  getType()                         { return type; }
    public void    setType(String type)              { this.type = type; }

    public Instant getTimestamp()                     { return timestamp; }
    public void    setTimestamp(Instant timestamp)    { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "NodeEvent{eventType='" + eventType + "', nodeId='" + nodeId +
               "', userId='" + userId + "', districtId='" + districtId +
               "', type='" + type + "', timestamp=" + timestamp + '}';
    }
}
