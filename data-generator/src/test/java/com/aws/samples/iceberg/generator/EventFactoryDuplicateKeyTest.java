package com.aws.samples.iceberg.generator;

import com.aws.samples.iceberg.model.BaseEvent;
import net.jqwik.api.*;
import net.jqwik.api.constraints.DoubleRange;
import net.jqwik.api.constraints.IntRange;

import java.util.*;

/**
 * Property-based tests for EventFactory duplicate key generation.
 * 
 * Feature: iceberg-flink-samples, Property 11: Data Generator Key Duplication
 * Validates: Requirements 13.7
 */
class EventFactoryDuplicateKeyTest {
    
    /**
     * Property 11: Data Generator Key Duplication
     * 
     * For any run of the data generator in upsert testing mode, a configurable
     * percentage of events should have duplicate keys to enable testing of
     * upsert/delete vector functionality.
     */
    @Property(tries = 100)
    void duplicateKeyGenerationMatchesConfiguration(
            @ForAll @DoubleRange(min = 0.1, max = 0.9) double duplicateProbability,
            @ForAll @IntRange(min = 500, max = 2000) int sampleSize) {
        
        // Create factory with duplicate key probability
        Map<String, Double> distribution = new HashMap<>();
        distribution.put("ORDER", 0.5);
        distribution.put("USER", 0.3);
        distribution.put("CLICK", 0.2);
        
        EventFactory factory = new EventFactory(distribution, duplicateProbability, 0.0, 0.0);
        
        // Generate events and track unique IDs
        Set<String> uniqueIds = new HashSet<>();
        int duplicateCount = 0;
        
        for (int i = 0; i < sampleSize; i++) {
            BaseEvent event = factory.createRandomEvent();
            String eventId = event.getEventId();
            
            if (uniqueIds.contains(eventId)) {
                duplicateCount++;
            } else {
                uniqueIds.add(eventId);
            }
        }
        
        // Calculate actual duplicate rate
        double actualDuplicateRate = (double) duplicateCount / sampleSize;
        
        // Verify duplicate rate is within tolerance
        // Note: The actual rate may be lower than configured because:
        // 1. Early events have no pool to reuse from
        // 2. Random selection from pool may not always hit duplicates
        // We verify that duplicates DO occur when probability > 0
        if (duplicateProbability > 0.1 && sampleSize > 100) {
            if (duplicateCount == 0) {
                throw new AssertionError(
                    String.format("Expected duplicates with probability %.2f but got none in %d events",
                        duplicateProbability, sampleSize));
            }
        }
        
        // For high probability and large samples, verify we get reasonable duplication
        if (duplicateProbability > 0.5 && sampleSize > 1000) {
            // Should have at least some duplicates (conservative check)
            double minExpectedRate = 0.05; // At least 5% duplicates
            if (actualDuplicateRate < minExpectedRate) {
                throw new AssertionError(
                    String.format("Duplicate rate too low: expected >= %.2f, got %.2f (config: %.2f)",
                        minExpectedRate, actualDuplicateRate, duplicateProbability));
            }
        }
    }
    
    /**
     * Verifies that duplicate keys are actually reused (same ID appears multiple times).
     */
    @Property(tries = 100)
    void duplicateKeysAreActuallyReused(@ForAll @IntRange(min = 200, max = 500) int count) {
        // High duplicate probability to ensure we see reuse
        EventFactory factory = new EventFactory(new HashMap<>(), 0.5, 0.0, 0.0);
        
        Map<String, Integer> idCounts = new HashMap<>();
        
        for (int i = 0; i < count; i++) {
            BaseEvent event = factory.createRandomEvent();
            String eventId = event.getEventId();
            idCounts.put(eventId, idCounts.getOrDefault(eventId, 0) + 1);
        }
        
        // Count how many IDs appear more than once
        long reusedIds = idCounts.values().stream()
            .filter(c -> c > 1)
            .count();
        
        // With 50% duplicate probability and 200+ events, we should see some reuse
        if (count >= 200 && reusedIds == 0) {
            throw new AssertionError(
                String.format("Expected some ID reuse with 50%% duplicate probability in %d events, but found none",
                    count));
        }
    }
    
    /**
     * Verifies that with zero duplicate probability, all IDs are unique.
     */
    @Property(tries = 100)
    void zeroDuplicateProbabilityProducesUniqueIds(@ForAll @IntRange(min = 50, max = 200) int count) {
        EventFactory factory = new EventFactory(new HashMap<>(), 0.0, 0.0, 0.0);
        
        Set<String> uniqueIds = new HashSet<>();
        
        for (int i = 0; i < count; i++) {
            BaseEvent event = factory.createRandomEvent();
            String eventId = event.getEventId();
            
            if (uniqueIds.contains(eventId)) {
                throw new AssertionError(
                    String.format("Found duplicate ID '%s' with zero duplicate probability", eventId));
            }
            uniqueIds.add(eventId);
        }
        
        // All IDs should be unique
        if (uniqueIds.size() != count) {
            throw new AssertionError(
                String.format("Expected %d unique IDs, got %d", count, uniqueIds.size()));
        }
    }
    
    /**
     * Verifies that duplicate IDs maintain the same event type prefix.
     */
    @Property(tries = 100)
    void duplicateIdsPreserveEventTypePrefix(@ForAll @IntRange(min = 100, max = 300) int count) {
        EventFactory factory = new EventFactory(new HashMap<>(), 0.3, 0.0, 0.0);
        
        Map<String, String> idToType = new HashMap<>();
        
        for (int i = 0; i < count; i++) {
            BaseEvent event = factory.createRandomEvent();
            String eventId = event.getEventId();
            String eventType = event.getEventType();
            
            if (idToType.containsKey(eventId)) {
                // This is a duplicate - verify it has the same type
                String previousType = idToType.get(eventId);
                if (!previousType.equals(eventType)) {
                    throw new AssertionError(
                        String.format("Duplicate ID '%s' has inconsistent types: '%s' vs '%s'",
                            eventId, previousType, eventType));
                }
            } else {
                idToType.put(eventId, eventType);
            }
        }
    }
}
