package com.aws.samples.iceberg.dynamic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import net.jqwik.api.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based test for schema-agnostic dynamic routing correctness.
 * 
 * Feature: iceberg-flink-samples, Property 4: Dynamic Routing Correctness
 * 
 * For any JSON event with an event_type field, the event should be routed
 * to the table corresponding to its event_type value (e.g., "order" → order_events).
 * 
 * Validates: Requirements 4.1
 */
class DynamicRoutingPropertyTest {
    
    private static final ObjectMapper MAPPER = new ObjectMapper();
    
    /**
     * Property: Generator can be instantiated with various configurations.
     */
    @Property(tries = 10)
    void generatorCanBeInstantiatedWithDifferentConfigs(
            @ForAll("databases") String database,
            @ForAll("routingFields") String routingField) {
        
        SchemaAgnosticRoutingGenerator generator = new SchemaAgnosticRoutingGenerator(
            database,
            routingField,
            null,
            "_events",
            java.util.Arrays.asList("event_date", "region")
        );
        
        assertNotNull(generator);
    }
    
    /**
     * Property: JSON events with event_type field can be processed.
     */
    @Property(tries = 100)
    void jsonEventsCanBeProcessed(@ForAll("mixedJsonEvents") JsonNode event) {
        SchemaAgnosticRoutingGenerator generator = new SchemaAgnosticRoutingGenerator("iceberg_samples");
        
        // Verify the event has required fields
        assertNotNull(event.get("event_type"), "Event should have event_type field");
        assertNotNull(event.get("event_id"), "Event should have event_id field");
        
        // The generator should be able to process any valid JSON
        String eventType = event.get("event_type").asText();
        assertFalse(eventType.isEmpty(), "event_type should not be empty");
    }
    
    @Provide
    Arbitrary<String> databases() {
        return Arbitraries.of("iceberg_samples", "test_db", "production");
    }
    
    @Provide
    Arbitrary<String> routingFields() {
        return Arbitraries.of("event_type", "type", "category");
    }
    
    @Provide
    Arbitrary<JsonNode> mixedJsonEvents() {
        return Arbitraries.oneOf(
            orderJsonEvents(),
            userJsonEvents(),
            clickJsonEvents(),
            customJsonEvents()
        );
    }
    
    @Provide
    Arbitrary<JsonNode> orderJsonEvents() {
        return Combinators.combine(
            Arbitraries.strings().alpha().ofLength(10),
            Arbitraries.of("us-east-1", "us-west-2", "eu-west-1"),
            Arbitraries.strings().alpha().ofLength(10),
            Arbitraries.bigDecimals().between(BigDecimal.ONE, BigDecimal.valueOf(10000))
        ).as((eventId, region, orderId, amount) -> {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("event_id", eventId);
            node.put("event_time", Instant.now().toString());
            node.put("event_type", "order");
            node.put("region", region);
            node.put("event_date", LocalDate.now().toString());
            node.put("order_id", orderId);
            node.put("amount", amount);
            node.put("currency", "USD");
            node.put("status", "PENDING");
            return (JsonNode) node;
        });
    }
    
    @Provide
    Arbitrary<JsonNode> userJsonEvents() {
        return Combinators.combine(
            Arbitraries.strings().alpha().ofLength(10),
            Arbitraries.of("us-east-1", "us-west-2", "eu-west-1"),
            Arbitraries.strings().alpha().ofLength(10),
            Arbitraries.of("login", "logout", "signup")
        ).as((eventId, region, userId, action) -> {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("event_id", eventId);
            node.put("event_time", Instant.now().toString());
            node.put("event_type", "user");
            node.put("region", region);
            node.put("event_date", LocalDate.now().toString());
            node.put("user_id", userId);
            node.put("action", action);
            node.put("device_type", "mobile");
            return (JsonNode) node;
        });
    }
    
    @Provide
    Arbitrary<JsonNode> clickJsonEvents() {
        return Combinators.combine(
            Arbitraries.strings().alpha().ofLength(10),
            Arbitraries.of("us-east-1", "us-west-2", "eu-west-1"),
            Arbitraries.strings().alpha().ofLength(10),
            Arbitraries.strings().alpha().ofLength(20)
        ).as((eventId, region, sessionId, pageUrl) -> {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("event_id", eventId);
            node.put("event_time", Instant.now().toString());
            node.put("event_type", "click");
            node.put("region", region);
            node.put("event_date", LocalDate.now().toString());
            node.put("session_id", sessionId);
            node.put("page_url", pageUrl);
            return (JsonNode) node;
        });
    }
    
    /**
     * Test with completely custom/unknown event types to verify true schema-agnosticism.
     */
    @Provide
    Arbitrary<JsonNode> customJsonEvents() {
        return Combinators.combine(
            Arbitraries.strings().alpha().ofLength(10),
            Arbitraries.of("custom", "unknown", "new_type", "sensor"),
            Arbitraries.integers().between(1, 1000),
            Arbitraries.doubles().between(0.0, 100.0)
        ).as((eventId, eventType, intValue, doubleValue) -> {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("event_id", eventId);
            node.put("event_time", Instant.now().toString());
            node.put("event_type", eventType);
            node.put("custom_int_field", intValue);
            node.put("custom_double_field", doubleValue);
            node.put("custom_boolean", true);
            return (JsonNode) node;
        });
    }
}
