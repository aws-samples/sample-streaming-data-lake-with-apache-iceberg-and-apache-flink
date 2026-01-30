package com.aws.samples.iceberg.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * User event representing user actions (login, logout, signup).
 * Includes optional userAgent field for schema evolution testing.
 */
public class UserEvent extends BaseEvent {
    
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("user_id")
    private String userId;
    
    @JsonProperty("action")
    private String action;
    
    @JsonProperty("device_type")
    private String deviceType;
    
    @JsonProperty("ip_address")
    private String ipAddress;
    
    @JsonProperty("user_agent")
    private String userAgent;  // Optional field for schema evolution
    
    public UserEvent() {
        super();
    }
    
    public UserEvent(String eventId, Instant eventTime, String region, LocalDate eventDate,
                     String userId, String action, String deviceType, String ipAddress) {
        super(eventId, eventTime, "USER", region, eventDate);
        this.userId = userId;
        this.action = action;
        this.deviceType = deviceType;
        this.ipAddress = ipAddress;
    }
    
    // Getters and Setters
    
    public String getUserId() {
        return userId;
    }
    
    public void setUserId(String userId) {
        this.userId = userId;
    }
    
    public String getAction() {
        return action;
    }
    
    public void setAction(String action) {
        this.action = action;
    }
    
    public String getDeviceType() {
        return deviceType;
    }
    
    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }
    
    public String getIpAddress() {
        return ipAddress;
    }
    
    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }
    
    public String getUserAgent() {
        return userAgent;
    }
    
    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        UserEvent userEvent = (UserEvent) o;
        return Objects.equals(userId, userEvent.userId) &&
               Objects.equals(action, userEvent.action) &&
               Objects.equals(deviceType, userEvent.deviceType) &&
               Objects.equals(ipAddress, userEvent.ipAddress) &&
               Objects.equals(userAgent, userEvent.userAgent);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), userId, action, deviceType, ipAddress, userAgent);
    }
    
    @Override
    public String toString() {
        return "UserEvent{" +
               "eventId='" + getEventId() + '\'' +
               ", eventTime=" + getEventTime() +
               ", userId='" + userId + '\'' +
               ", action='" + action + '\'' +
               ", deviceType='" + deviceType + '\'' +
               ", ipAddress='" + ipAddress + '\'' +
               ", userAgent='" + userAgent + '\'' +
               ", region='" + getRegion() + '\'' +
               ", eventDate=" + getEventDate() +
               '}';
    }
}
