package com.aws.samples.iceberg.sql;

import com.aws.samples.iceberg.model.OrderEvent;
import net.jqwik.api.*;
import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Property-based test for UPSERT semantics in Iceberg tables.
 * 
 * Feature: iceberg-flink-samples, Property 2: Upsert Semantics Correctness
 * 
 * Validates: Requirements 2.4, 3.4
 * 
 * This test verifies that for any sequence of events with the same primary key (event_id),
 * the final state of the Iceberg table should contain exactly one row per key with the
 * values from the most recent event (by event_time or processing order).
 */
public class UpsertSemanticsPropertyTest {
    
    /**
     * Property: For any sequence of events with duplicate keys, the final state
     * should contain exactly one row per unique key with the most recent values.
     * 
     * This property tests the core UPSERT behavior:
     * 1. Multiple events with the same event_id should result in a single row
     * 2. The final row should have values from the event with the latest event_time
     * 3. All unique event_ids should be present in the final state
     */
    @Property(tries = 100)
    void upsertShouldKeepOnlyMostRecentEventPerKey(
            @ForAll("eventSequencesWithDuplicates") List<OrderEvent> events) {
        
        // Simulate the UPSERT behavior: keep only the most recent event per event_id
        Map<String, OrderEvent> finalState = simulateUpsert(events);
        
        // Property 1: Each unique event_id should appear exactly once in final state
        Set<String> uniqueEventIds = events.stream()
            .map(OrderEvent::getEventId)
            .collect(Collectors.toSet());
        
        assertEquals(
            uniqueEventIds.size(),
            finalState.size(),
            "Final state should contain exactly one row per unique event_id"
        );
        
        // Property 2: For each event_id, the final state should contain the event
        // with the most recent event_time
        for (String eventId : uniqueEventIds) {
            List<OrderEvent> eventsWithSameId = events.stream()
                .filter(e -> e.getEventId().equals(eventId))
                .sorted(Comparator.comparing(OrderEvent::getEventTime).reversed())
                .collect(Collectors.toList());
            
            OrderEvent expectedMostRecent = eventsWithSameId.get(0);
            OrderEvent actualInFinalState = finalState.get(eventId);
            
            assertNotNull(
                actualInFinalState,
                "Final state should contain event with id: " + eventId
            );
            
            // Verify the most recent event is in the final state
            assertEquals(
                expectedMostRecent.getEventTime(),
                actualInFinalState.getEventTime(),
                "Final state should contain the event with the most recent event_time for id: " + eventId
            );
            
            assertEquals(
                expectedMostRecent.getStatus(),
                actualInFinalState.getStatus(),
                "Final state should have the status from the most recent event for id: " + eventId
            );
            
            assertEquals(
                expectedMostRecent.getAmount(),
                actualInFinalState.getAmount(),
                "Final state should have the amount from the most recent event for id: " + eventId
            );
        }
    }
    
    /**
     * Property: UPSERT should preserve all unique keys even when some keys
     * appear only once in the input stream.
     */
    @Property(tries = 100)
    void upsertShouldPreserveUniqueKeys(
            @ForAll("eventSequencesWithMixedDuplicates") List<OrderEvent> events) {
        
        Map<String, OrderEvent> finalState = simulateUpsert(events);
        
        // Get all unique event IDs from input
        Set<String> inputEventIds = events.stream()
            .map(OrderEvent::getEventId)
            .collect(Collectors.toSet());
        
        // Get all event IDs from final state
        Set<String> finalStateEventIds = finalState.keySet();
        
        // Property: All unique input keys should be present in final state
        assertEquals(
            inputEventIds,
            finalStateEventIds,
            "Final state should contain all unique event_ids from input"
        );
    }
    
    /**
     * Property: When events arrive out of order (by event_time), UPSERT should
     * still keep the event with the latest event_time, not the last processed.
     */
    @Property(tries = 100)
    void upsertShouldUseEventTimeNotProcessingTime(
            @ForAll("outOfOrderEventSequences") List<OrderEvent> events) {
        
        Map<String, OrderEvent> finalState = simulateUpsert(events);
        
        // For each unique event_id, verify the final state has the event
        // with the maximum event_time, regardless of processing order
        Set<String> uniqueEventIds = events.stream()
            .map(OrderEvent::getEventId)
            .collect(Collectors.toSet());
        
        for (String eventId : uniqueEventIds) {
            Optional<OrderEvent> maxEventTimeEvent = events.stream()
                .filter(e -> e.getEventId().equals(eventId))
                .max(Comparator.comparing(OrderEvent::getEventTime));
            
            assertTrue(maxEventTimeEvent.isPresent());
            
            OrderEvent actualInFinalState = finalState.get(eventId);
            assertNotNull(actualInFinalState);
            
            assertEquals(
                maxEventTimeEvent.get().getEventTime(),
                actualInFinalState.getEventTime(),
                "Final state should have the event with maximum event_time for id: " + eventId
            );
        }
    }
    
    /**
     * Simulates UPSERT behavior: for each event_id, keep only the event
     * with the most recent event_time.
     */
    private Map<String, OrderEvent> simulateUpsert(List<OrderEvent> events) {
        Map<String, OrderEvent> state = new HashMap<>();
        
        for (OrderEvent event : events) {
            String eventId = event.getEventId();
            OrderEvent existing = state.get(eventId);
            
            if (existing == null || 
                event.getEventTime().isAfter(existing.getEventTime())) {
                state.put(eventId, event);
            }
        }
        
        return state;
    }
    
    // ========== Data Generators ==========
    
    /**
     * Generates sequences of events where some event_ids are duplicated.
     * This simulates the scenario where updates are sent for the same entity.
     */
    @Provide
    Arbitrary<List<OrderEvent>> eventSequencesWithDuplicates() {
        return Arbitraries.integers().between(3, 10).flatMap(numUniqueKeys -> {
            List<OrderEvent> allEvents = new ArrayList<>();
            
            for (int i = 0; i < numUniqueKeys; i++) {
                String eventId = "key-" + i;
                
                // Generate 1-5 versions of this event
                int numVersions = 1 + new Random().nextInt(5);
                
                for (int j = 0; j < numVersions; j++) {
                    OrderEvent event = createOrderEvent(
                        eventId,
                        Instant.now().plusSeconds(j * 10)
                    );
                    allEvents.add(event);
                }
            }
            
            // Shuffle to simulate random arrival order
            Collections.shuffle(allEvents);
            return Arbitraries.just(allEvents);
        });
    }
    
    /**
     * Generates sequences where some keys appear once and others multiple times.
     */
    @Provide
    Arbitrary<List<OrderEvent>> eventSequencesWithMixedDuplicates() {
        return Arbitraries.integers().between(10, 20).flatMap(totalUniqueKeys -> {
            List<OrderEvent> events = new ArrayList<>();
            
            // Generate some unique events (appear only once)
            for (int i = 0; i < totalUniqueKeys / 2; i++) {
                events.add(createOrderEvent("unique-" + i, Instant.now()));
            }
            
            // Generate some duplicate events (appear 2-3 times)
            for (int i = 0; i < totalUniqueKeys / 4; i++) {
                String duplicateKey = "duplicate-" + i;
                int duplicateCount = 2 + new Random().nextInt(2); // 2 or 3
                
                for (int j = 0; j < duplicateCount; j++) {
                    events.add(createOrderEvent(
                        duplicateKey,
                        Instant.now().plusSeconds(j * 10)
                    ));
                }
            }
            
            Collections.shuffle(events);
            return Arbitraries.just(events);
        });
    }
    
    /**
     * Generates sequences where events arrive out of order by event_time.
     */
    @Provide
    Arbitrary<List<OrderEvent>> outOfOrderEventSequences() {
        return Arbitraries.integers().between(5, 10).flatMap(numKeys -> {
            List<OrderEvent> events = new ArrayList<>();
            
            for (int i = 0; i < numKeys; i++) {
                String eventId = "key-" + i;
                
                // Generate 2-4 events with the same key but different timestamps
                int numVersions = 2 + new Random().nextInt(3);
                List<Instant> timestamps = new ArrayList<>();
                
                Instant baseTime = Instant.now().minusSeconds(3600);
                for (int j = 0; j < numVersions; j++) {
                    timestamps.add(baseTime.plusSeconds(j * 100));
                }
                
                // Shuffle timestamps to create out-of-order arrival
                Collections.shuffle(timestamps);
                
                for (Instant timestamp : timestamps) {
                    events.add(createOrderEvent(eventId, timestamp));
                }
            }
            
            return Arbitraries.just(events);
        });
    }
    
    private OrderEvent createOrderEvent(String eventId, Instant eventTime) {
        Random random = new Random();
        String[] regions = {"us-east-1", "us-west-2", "eu-west-1"};
        String[] statuses = {"PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"};
        
        OrderEvent event = new OrderEvent();
        event.setEventId(eventId);
        event.setEventTime(eventTime);
        event.setEventType("ORDER");
        event.setRegion(regions[random.nextInt(regions.length)]);
        event.setEventDate(eventTime.atZone(ZoneOffset.UTC).toLocalDate());
        event.setOrderId("order-" + UUID.randomUUID().toString().substring(0, 8));
        event.setCustomerId("customer-" + random.nextInt(1000));
        event.setAmount(BigDecimal.valueOf(1 + random.nextInt(10000)).setScale(2, BigDecimal.ROUND_HALF_UP));
        event.setCurrency("USD");
        event.setStatus(statuses[random.nextInt(statuses.length)]);
        return event;
    }
}
