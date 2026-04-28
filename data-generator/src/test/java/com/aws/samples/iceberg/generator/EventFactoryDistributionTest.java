package com.aws.samples.iceberg.generator;

import com.aws.samples.iceberg.model.BaseEvent;
import net.jqwik.api.*;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;

import java.util.HashMap;
import java.util.Map;

/**
 * Property-based tests for EventFactory distribution correctness.
 * 
 * Feature: iceberg-flink-samples, Property 10: Data Generator Distribution Correctness
 * Validates: Requirements 11.6, 13.3
 */
class EventFactoryDistributionTest {
    
    /**
     * Property 10: Data Generator Distribution Correctness
     * 
     * For any run of the data generator with configured event type distribution,
     * the actual distribution of generated events should match the configured
     * distribution within a statistical tolerance (±5%).
     */
    @Property(tries = 100)
    void eventDistributionMatchesConfiguration(
            @ForAll @DoubleRange(min = 0.1, max = 0.8) double orderProbability,
            @ForAll @DoubleRange(min = 0.1, max = 0.8) double userProbability,
            @ForAll @IntRange(min = 1000, max = 5000) int sampleSize) {
        
        // Normalize probabilities to sum to 1.0
        double clickProbability = 1.0 - orderProbability - userProbability;
        
        // Skip if click probability is negative or too small
        Assume.that(clickProbability >= 0.1);
        
        Map<String, Double> distribution = new HashMap<>();
        distribution.put("ORDER", orderProbability);
        distribution.put("USER", userProbability);
        distribution.put("CLICK", clickProbability);
        
        EventFactory factory = new EventFactory(distribution, 0.0, 0.0, 0.0);
        
        // Generate events and count types
        Map<String, Integer> counts = new HashMap<>();
        counts.put("ORDER", 0);
        counts.put("USER", 0);
        counts.put("CLICK", 0);
        
        for (int i = 0; i < sampleSize; i++) {
            BaseEvent event = factory.createRandomEvent();
            String eventType = event.getEventType();
            counts.put(eventType, counts.get(eventType) + 1);
        }
        
        // Calculate actual distribution
        Map<String, Double> actualDistribution = new HashMap<>();
        for (Map.Entry<String, Integer> entry : counts.entrySet()) {
            actualDistribution.put(entry.getKey(), (double) entry.getValue() / sampleSize);
        }
        
        // Verify distribution within tolerance. Tolerance scales with sampleSize since
        // smaller samples have larger expected statistical deviation. For sampleSize=1000
        // with p=0.36, the 3σ deviation is ~4.5%, so we use max(5%, 3σ) to keep the test
        // from becoming flaky while still catching real distribution bugs.
        double tolerance = Math.max(0.05,
                3.0 * Math.sqrt(0.25 / sampleSize));
        for (String eventType : distribution.keySet()) {
            double expected = distribution.get(eventType);
            double actual = actualDistribution.get(eventType);
            double difference = Math.abs(expected - actual);

            if (difference > tolerance) {
                throw new AssertionError(
                    String.format("Distribution mismatch for %s: expected %.3f, got %.3f "
                            + "(diff: %.3f > tolerance %.3f, sampleSize %d)",
                        eventType, expected, actual, difference, tolerance, sampleSize));
            }
        }
    }
    
    /**
     * Verifies that all generated events have required fields populated.
     */
    @Property(tries = 100)
    void allEventsHaveRequiredFields(@ForAll @IntRange(min = 10, max = 100) int count) {
        EventFactory factory = new EventFactory();
        
        for (int i = 0; i < count; i++) {
            BaseEvent event = factory.createRandomEvent();
            
            // Verify required fields are not null
            if (event.getEventId() == null) {
                throw new AssertionError("Event ID is null");
            }
            if (event.getEventTime() == null) {
                throw new AssertionError("Event time is null");
            }
            if (event.getEventType() == null) {
                throw new AssertionError("Event type is null");
            }
            if (event.getRegion() == null) {
                throw new AssertionError("Region is null");
            }
            if (event.getEventDate() == null) {
                throw new AssertionError("Event date is null");
            }
        }
    }
    
    /**
     * Verifies that event types match the configured distribution keys.
     */
    @Property(tries = 100)
    void eventTypesMatchDistributionKeys(@ForAll @IntRange(min = 10, max = 100) int count) {
        Map<String, Double> distribution = new HashMap<>();
        distribution.put("ORDER", 0.5);
        distribution.put("USER", 0.3);
        distribution.put("CLICK", 0.2);
        
        EventFactory factory = new EventFactory(distribution, 0.0, 0.0, 0.0);
        
        for (int i = 0; i < count; i++) {
            BaseEvent event = factory.createRandomEvent();
            String eventType = event.getEventType();
            
            if (!distribution.containsKey(eventType)) {
                throw new AssertionError(
                    String.format("Generated event type '%s' not in distribution keys", eventType));
            }
        }
    }
}
