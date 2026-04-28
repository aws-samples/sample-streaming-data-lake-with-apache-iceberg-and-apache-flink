package com.aws.samples.iceberg.generator;

import com.aws.samples.iceberg.model.BaseEvent;
import com.aws.samples.iceberg.model.ClickEvent;
import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.model.UserEvent;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Factory for generating test events with configurable distributions.
 * Supports duplicate key generation, late-arriving events, and schema evolution scenarios.
 */
public class EventFactory {
    
    private static final String[] REGIONS = {"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"};
    private static final String[] USER_ACTIONS = {"login", "logout", "signup", "profile_update"};
    private static final String[] DEVICE_TYPES = {"mobile", "desktop", "tablet"};
    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "JPY"};
    private static final String[] ORDER_STATUSES = {"pending", "confirmed", "shipped", "delivered", "cancelled"};
    private static final String[] USER_AGENTS = {
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15"
    };
    
    private final Random random;
    private final Map<String, Double> eventTypeDistribution;
    private final double duplicateKeyProbability;
    private final double lateArrivalProbability;
    private final double schemaEvolutionProbability;
    private final Map<String, Set<String>> reusableEventIdsByType;
    private final int maxReusableIdsPerType;
    
    /**
     * Creates an EventFactory with default configuration.
     */
    public EventFactory() {
        this(new HashMap<>(), 0.1, 0.05, 0.3);
    }
    
    /**
     * Creates an EventFactory with custom configuration.
     * 
     * @param eventTypeDistribution Map of event type to probability (must sum to 1.0)
     * @param duplicateKeyProbability Probability of generating duplicate event IDs (0.0-1.0)
     * @param lateArrivalProbability Probability of generating late-arriving events (0.0-1.0)
     * @param schemaEvolutionProbability Probability of including optional fields (0.0-1.0)
     */
    public EventFactory(Map<String, Double> eventTypeDistribution,
                       double duplicateKeyProbability,
                       double lateArrivalProbability,
                       double schemaEvolutionProbability) {
        this.random = ThreadLocalRandom.current();
        this.eventTypeDistribution = new HashMap<>(eventTypeDistribution);
        
        // Set default distribution if not provided
        if (this.eventTypeDistribution.isEmpty()) {
            this.eventTypeDistribution.put("ORDER", 0.4);
            this.eventTypeDistribution.put("USER", 0.3);
            this.eventTypeDistribution.put("CLICK", 0.3);
        }
        
        this.duplicateKeyProbability = Math.max(0.0, Math.min(1.0, duplicateKeyProbability));
        this.lateArrivalProbability = Math.max(0.0, Math.min(1.0, lateArrivalProbability));
        this.schemaEvolutionProbability = Math.max(0.0, Math.min(1.0, schemaEvolutionProbability));
        this.reusableEventIdsByType = new HashMap<>();
        this.maxReusableIdsPerType = 100;
    }
    
    /**
     * Creates a random event based on configured distribution.
     * 
     * @return A randomly generated BaseEvent (OrderEvent, UserEvent, or ClickEvent)
     */
    public BaseEvent createRandomEvent() {
        String eventType = selectEventType();
        
        switch (eventType) {
            case "ORDER":
                return createOrderEvent();
            case "USER":
                return createUserEvent();
            case "CLICK":
                return createClickEvent();
            default:
                throw new IllegalStateException("Unknown event type: " + eventType);
        }
    }
    
    /**
     * Creates an OrderEvent with random data.
     */
    public OrderEvent createOrderEvent() {
        String eventId = generateEventId("order");
        Instant eventTime = generateEventTime();
        String region = randomElement(REGIONS);
        LocalDate eventDate = eventTime.atZone(ZoneOffset.UTC).toLocalDate();
        
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8);
        String customerId = "CUST-" + random.nextInt(10000);
        BigDecimal amount = BigDecimal.valueOf(10 + random.nextDouble() * 990)
                .setScale(2, java.math.RoundingMode.HALF_UP);
        String currency = randomElement(CURRENCIES);
        String status = randomElement(ORDER_STATUSES);
        
        OrderEvent event = new OrderEvent(eventId, eventTime, region, eventDate,
                                          orderId, customerId, amount, currency, status);
        
        // Add metadata
        event.addMetadata("source", "data-generator");
        event.addMetadata("schema_version", "v1");
        
        return event;
    }
    
    /**
     * Creates a UserEvent with random data.
     */
    public UserEvent createUserEvent() {
        String eventId = generateEventId("user");
        Instant eventTime = generateEventTime();
        String region = randomElement(REGIONS);
        LocalDate eventDate = eventTime.atZone(ZoneOffset.UTC).toLocalDate();
        
        String userId = "USER-" + random.nextInt(5000);
        String action = randomElement(USER_ACTIONS);
        String deviceType = randomElement(DEVICE_TYPES);
        String ipAddress = generateIpAddress();
        
        UserEvent event = new UserEvent(eventId, eventTime, region, eventDate,
                                        userId, action, deviceType, ipAddress);
        
        // Schema evolution: optionally include userAgent field
        if (random.nextDouble() < schemaEvolutionProbability) {
            event.setUserAgent(randomElement(USER_AGENTS));
        }
        
        // Add metadata
        event.addMetadata("source", "data-generator");
        event.addMetadata("schema_version", event.getUserAgent() != null ? "v2" : "v1");
        
        return event;
    }
    
    /**
     * Creates a ClickEvent with random data.
     */
    public ClickEvent createClickEvent() {
        String eventId = generateEventId("click");
        Instant eventTime = generateEventTime();
        String region = randomElement(REGIONS);
        LocalDate eventDate = eventTime.atZone(ZoneOffset.UTC).toLocalDate();
        
        String sessionId = "SESSION-" + UUID.randomUUID().toString().substring(0, 12);
        String pageUrl = "https://example.com/page" + random.nextInt(100);
        String referrer = random.nextBoolean() ? "https://google.com" : "https://example.com";
        
        ClickEvent event = new ClickEvent(eventId, eventTime, region, eventDate,
                                          sessionId, pageUrl, referrer);
        
        // Set time on page
        Duration timeOnPage = Duration.ofSeconds(5 + random.nextInt(300));
        event.setTimeOnPage(timeOnPage);
        
        // Schema evolution: optionally include scrollDepth field
        if (random.nextDouble() < schemaEvolutionProbability) {
            event.setScrollDepth(random.nextInt(100));
        }
        
        // Add metadata
        event.addMetadata("source", "data-generator");
        event.addMetadata("schema_version", event.getScrollDepth() != null ? "v2" : "v1");
        
        return event;
    }
    
    /**
     * Selects an event type based on configured distribution.
     */
    private String selectEventType() {
        double rand = random.nextDouble();
        double cumulative = 0.0;
        
        for (Map.Entry<String, Double> entry : eventTypeDistribution.entrySet()) {
            cumulative += entry.getValue();
            if (rand <= cumulative) {
                return entry.getKey();
            }
        }
        
        // Fallback to last type if rounding errors occur
        return eventTypeDistribution.keySet().iterator().next();
    }
    
    /**
     * Generates an event ID, potentially reusing existing IDs for upsert testing.
     * IDs are only reused within the same event type to maintain consistency.
     */
    private String generateEventId(String prefix) {
        String eventType = prefix.toUpperCase();
        
        // Get or create the ID pool for this event type
        Set<String> typePool = reusableEventIdsByType.computeIfAbsent(eventType, k -> new HashSet<>());
        
        // Decide whether to reuse an existing ID from this type's pool
        if (!typePool.isEmpty() && random.nextDouble() < duplicateKeyProbability) {
            List<String> ids = new ArrayList<>(typePool);
            return ids.get(random.nextInt(ids.size()));
        }
        
        // Generate new ID
        String newId = eventType + "-" + UUID.randomUUID().toString();
        
        // Add to reusable pool if not full
        if (typePool.size() < maxReusableIdsPerType) {
            typePool.add(newId);
        }
        
        return newId;
    }
    
    /**
     * Generates an event timestamp, potentially creating late-arriving events.
     */
    private Instant generateEventTime() {
        Instant now = Instant.now();
        
        // Generate late-arriving event
        if (random.nextDouble() < lateArrivalProbability) {
            // Late by 1-60 minutes
            long lateMinutes = 1 + random.nextInt(60);
            return now.minus(Duration.ofMinutes(lateMinutes));
        }
        
        // Normal event (within last 5 seconds)
        long offsetMillis = random.nextInt(5000);
        return now.minusMillis(offsetMillis);
    }
    
    /**
     * Generates a random IP address.
     */
    private String generateIpAddress() {
        return random.nextInt(256) + "." +
               random.nextInt(256) + "." +
               random.nextInt(256) + "." +
               random.nextInt(256);
    }
    
    /**
     * Returns a random element from an array.
     */
    private <T> T randomElement(T[] array) {
        return array[random.nextInt(array.length)];
    }
    
    /**
     * Gets the configured event type distribution.
     */
    public Map<String, Double> getEventTypeDistribution() {
        return new HashMap<>(eventTypeDistribution);
    }
    
    /**
     * Gets the duplicate key probability.
     */
    public double getDuplicateKeyProbability() {
        return duplicateKeyProbability;
    }
    
    /**
     * Gets the late arrival probability.
     */
    public double getLateArrivalProbability() {
        return lateArrivalProbability;
    }
    
    /**
     * Gets the schema evolution probability.
     */
    public double getSchemaEvolutionProbability() {
        return schemaEvolutionProbability;
    }
}
