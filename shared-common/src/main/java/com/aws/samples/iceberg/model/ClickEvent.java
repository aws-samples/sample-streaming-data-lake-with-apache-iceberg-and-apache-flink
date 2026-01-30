package com.aws.samples.iceberg.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Objects;

/**
 * Click event representing user web interactions.
 * Includes optional scrollDepth field for schema evolution testing.
 */
public class ClickEvent extends BaseEvent {
    
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("session_id")
    private String sessionId;
    
    @JsonProperty("page_url")
    private String pageUrl;
    
    @JsonProperty("referrer")
    private String referrer;
    
    @JsonProperty("scroll_depth")
    private Integer scrollDepth;  // Optional field for schema evolution
    
    @JsonProperty("time_on_page_seconds")
    private Long timeOnPageSeconds;  // Duration stored as seconds
    
    public ClickEvent() {
        super();
    }
    
    public ClickEvent(String eventId, Instant eventTime, String region, LocalDate eventDate,
                      String sessionId, String pageUrl, String referrer) {
        super(eventId, eventTime, "CLICK", region, eventDate);
        this.sessionId = sessionId;
        this.pageUrl = pageUrl;
        this.referrer = referrer;
    }
    
    // Getters and Setters
    
    public String getSessionId() {
        return sessionId;
    }
    
    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
    
    public String getPageUrl() {
        return pageUrl;
    }
    
    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }
    
    public String getReferrer() {
        return referrer;
    }
    
    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }
    
    public Integer getScrollDepth() {
        return scrollDepth;
    }
    
    public void setScrollDepth(Integer scrollDepth) {
        this.scrollDepth = scrollDepth;
    }
    
    public Long getTimeOnPageSeconds() {
        return timeOnPageSeconds;
    }
    
    public void setTimeOnPageSeconds(Long timeOnPageSeconds) {
        this.timeOnPageSeconds = timeOnPageSeconds;
    }
    
    /**
     * Set time on page from Duration object.
     */
    public void setTimeOnPage(Duration duration) {
        this.timeOnPageSeconds = duration != null ? duration.getSeconds() : null;
    }
    
    /**
     * Get time on page as Duration object.
     */
    public Duration getTimeOnPage() {
        return timeOnPageSeconds != null ? Duration.ofSeconds(timeOnPageSeconds) : null;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        ClickEvent that = (ClickEvent) o;
        return Objects.equals(sessionId, that.sessionId) &&
               Objects.equals(pageUrl, that.pageUrl) &&
               Objects.equals(referrer, that.referrer) &&
               Objects.equals(scrollDepth, that.scrollDepth) &&
               Objects.equals(timeOnPageSeconds, that.timeOnPageSeconds);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sessionId, pageUrl, referrer, scrollDepth, timeOnPageSeconds);
    }
    
    @Override
    public String toString() {
        return "ClickEvent{" +
               "eventId='" + getEventId() + '\'' +
               ", eventTime=" + getEventTime() +
               ", sessionId='" + sessionId + '\'' +
               ", pageUrl='" + pageUrl + '\'' +
               ", referrer='" + referrer + '\'' +
               ", scrollDepth=" + scrollDepth +
               ", timeOnPageSeconds=" + timeOnPageSeconds +
               ", region='" + getRegion() + '\'' +
               ", eventDate=" + getEventDate() +
               '}';
    }
}
