package com.smartgrid.billing.dto;

import java.time.Instant;

public class UsageRecordEvent {

    private String  eventType;       // UsageRecordCreated
    private String  userId;
    private double  totalUsageKwh;
    private double  cost;
    private Instant periodStart;
    private Instant periodEnd;

    public UsageRecordEvent() {}

    public UsageRecordEvent(String userId, double totalUsageKwh, double cost,
                            Instant periodStart, Instant periodEnd) {
        this.eventType     = "UsageRecordCreated";
        this.userId        = userId;
        this.totalUsageKwh = totalUsageKwh;
        this.cost          = cost;
        this.periodStart   = periodStart;
        this.periodEnd     = periodEnd;
    }

    public String  getEventType()                        { return eventType; }
    public void    setEventType(String eventType)        { this.eventType = eventType; }

    public String  getUserId()                           { return userId; }
    public void    setUserId(String userId)              { this.userId = userId; }

    public double  getTotalUsageKwh()                            { return totalUsageKwh; }
    public void    setTotalUsageKwh(double totalUsageKwh)        { this.totalUsageKwh = totalUsageKwh; }

    public double  getCost()                     { return cost; }
    public void    setCost(double cost)          { this.cost = cost; }

    public Instant getPeriodStart()                      { return periodStart; }
    public void    setPeriodStart(Instant periodStart)   { this.periodStart = periodStart; }

    public Instant getPeriodEnd()                        { return periodEnd; }
    public void    setPeriodEnd(Instant periodEnd)       { this.periodEnd = periodEnd; }

    @Override
    public String toString() {
        return "UsageRecordEvent{userId='" + userId + "', totalUsageKwh=" + totalUsageKwh +
               ", cost=" + cost + ", periodStart=" + periodStart +
               ", periodEnd=" + periodEnd + '}';
    }
}
