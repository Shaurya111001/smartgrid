package com.smartgrid.account.dto;

import java.time.Instant;

public class UserEvent {

    private String  eventType;   // UserRegistered | UserUpdated | UserDeleted
    private String  userId;
    private String  name;
    private String  email;
    private Instant timestamp;

    public UserEvent() {}

    public UserEvent(String eventType, String userId, String name, String email) {
        this.eventType = eventType;
        this.userId    = userId;
        this.name      = name;
        this.email     = email;
        this.timestamp = Instant.now();
    }

    public String  getEventType()                    { return eventType; }
    public void    setEventType(String eventType)    { this.eventType = eventType; }

    public String  getUserId()                       { return userId; }
    public void    setUserId(String userId)          { this.userId = userId; }

    public String  getName()                         { return name; }
    public void    setName(String name)              { this.name = name; }

    public String  getEmail()                        { return email; }
    public void    setEmail(String email)            { this.email = email; }

    public Instant getTimestamp()                     { return timestamp; }
    public void    setTimestamp(Instant timestamp)    { this.timestamp = timestamp; }

    @Override
    public String toString() {
        return "UserEvent{eventType='" + eventType + "', userId='" + userId +
               "', name='" + name + "', email='" + email +
               "', timestamp=" + timestamp + '}';
    }
}
