package com.aws.samples.iceberg.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Base event class with common fields for all event types.
 * Uses Jackson polymorphic type handling for JSON serialization/deserialization.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "eventType"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = OrderEvent.class, name = "ORDER"),
    @JsonSubTypes.Type(value = UserEvent.class, name = "USER"),
    @JsonSubTypes.Type(value = ClickEvent.class, name = "CLICK")
})
public abstract class BaseEvent implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("event_id")
    private String eventId;
    
    @JsonProperty("event_time")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant eventTime;
    
    @JsonProperty("event_type")
    private String eventType;
    
    @JsonProperty("region")
    private String region;
    
    @JsonProperty("event_date")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
    private LocalDate eventDate;
    
    @JsonProperty("metadata")
    private Map<String, String> metadata;
    
    public BaseEvent() {
        this.metadata = new HashMap<>();
    }
    
    public BaseEvent(String eventId, Instant eventTime, String eventType, String region, LocalDate eventDate) {
        this.eventId = eventId;
        this.eventTime = eventTime;
        this.eventType = eventType;
        this.region = region;
        this.eventDate = eventDate;
        this.metadata = new HashMap<>();
    }
    
    // Getters and Setters
    
    public String getEventId() {
        return eventId;
    }
    
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }
    
    public Instant getEventTime() {
        return eventTime;
    }
    
    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }
    
    public String getEventType() {
        return eventType;
    }
    
    public void setEventType(String eventType) {
        this.eventType = eventType;
    }
    
    public String getRegion() {
        return region;
    }
    
    public void setRegion(String region) {
        this.region = region;
    }
    
    public LocalDate getEventDate() {
        return eventDate;
    }
    
    public void setEventDate(LocalDate eventDate) {
        this.eventDate = eventDate;
    }
    
    public Map<String, String> getMetadata() {
        return metadata;
    }
    
    public void setMetadata(Map<String, String> metadata) {
        this.metadata = metadata;
    }
    
    public void addMetadata(String key, String value) {
        this.metadata.put(key, value);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BaseEvent baseEvent = (BaseEvent) o;
        return Objects.equals(eventId, baseEvent.eventId) &&
               Objects.equals(eventTime, baseEvent.eventTime) &&
               Objects.equals(eventType, baseEvent.eventType) &&
               Objects.equals(region, baseEvent.region) &&
               Objects.equals(eventDate, baseEvent.eventDate) &&
               Objects.equals(metadata, baseEvent.metadata);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(eventId, eventTime, eventType, region, eventDate, metadata);
    }
    
    @Override
    public String toString() {
        return "BaseEvent{" +
               "eventId='" + eventId + '\'' +
               ", eventTime=" + eventTime +
               ", eventType='" + eventType + '\'' +
               ", region='" + region + '\'' +
               ", eventDate=" + eventDate +
               ", metadata=" + metadata +
               '}';
    }
}
