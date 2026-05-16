package com.smartgrid.presentation.model;

public class NodeView {

    private String nodeId;
    private String userId;
    private String districtId;
    private String type;

    public NodeView() {}

    public NodeView(String nodeId, String userId, String districtId, String type) {
        this.nodeId     = nodeId;
        this.userId     = userId;
        this.districtId = districtId;
        this.type       = type;
    }

    public String getNodeId()                  { return nodeId; }
    public void   setNodeId(String nodeId)     { this.nodeId = nodeId; }

    public String getUserId()                  { return userId; }
    public void   setUserId(String userId)     { this.userId = userId; }

    public String getDistrictId()                      { return districtId; }
    public void   setDistrictId(String districtId)     { this.districtId = districtId; }

    public String getType()                { return type; }
    public void   setType(String type)     { this.type = type; }
}
