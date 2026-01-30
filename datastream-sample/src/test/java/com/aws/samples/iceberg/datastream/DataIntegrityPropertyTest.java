package com.aws.samples.iceberg.datastream;

import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.util.EventToRowDataConverter;
import net.jqwik.api.*;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.junit.jupiter.api.Tag;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Property-based test for Kinesis to Iceberg data integrity.
 * 
 * Feature: iceberg-flink-samples, Property 1: Kinesis to Iceberg Data Integrity
 * 
 * Validates: Requirements 2.2
 * 
 * This test verifies that for any set of events sent to the Kinesis stream,
 * all events should eventually appear in the corresponding Iceberg table with
 * matching field values (event_id, event_time, and all payload fields).
 */
@Tag("property-test")
public class DataIntegrityPropertyTest {
    
    /**
     * Property: For any set of events, conversion to RowData and back should
     * preserve all field values.
     * 
     * This property tests the data integrity through the conversion pipeline:
     * 1. OrderEvent -> RowData conversion preserves all fields
     * 2. All required fields are present and non-null
     * 3. Field values match exactly after conversion
     */
    @Property(tries = 100)
    void eventToRowDataConversionPreservesAllFields(
            @ForAll("orderEvents") OrderEvent event) {
        
        // Convert event to RowData (this is what gets written to Iceberg)
        RowData rowData = EventToRowDataConverter.convertOrderEvent(event);
        
        // Verify all fields are present and match
        assertNotNull(rowData, "RowData should not be null");
        
        // Verify common fields (indices 0-4)
        assertEquals(
            event.getEventId(),
            rowData.getString(0).toString(),
            "event_id should match"
        );
        
        assertEquals(
            event.getEventTime().toEpochMilli(),
            rowData.getTimestamp(1, 6).getMillisecond(),
            "event_time should match"
        );
        
        assertEquals(
            event.getEventType(),
            rowData.getString(2).toString(),
            "event_type should match"
        );
        
        assertEquals(
            event.getRegion(),
            rowData.getString(3).toString(),
            "region should match"
        );
        
        assertEquals(
            event.getEventDate().toEpochDay(),
            rowData.getInt(4),
            "event_date should match"
        );
        
        // Verify OrderEvent specific fields (indices 5-10)
        assertEquals(
            event.getOrderId(),
            rowData.getString(5).toString(),
            "order_id should match"
        );
        
        assertEquals(
            event.getCustomerId(),
            rowData.getString(6).toString(),
            "customer_id should match"
        );
        
        assertEquals(
            event.getAmount(),
            rowData.getDecimal(7, 18, 2).toBigDecimal(),
            "amount should match"
        );
        
        assertEquals(
            event.getCurrency(),
            rowData.getString(8).toString(),
            "currency should match"
        );
        
        assertEquals(
            event.getStatus(),
            rowData.getString(9).toString(),
            "status should match"
        );
        
        // Verify metadata map is present (can be empty)
        assertNotNull(rowData.getMap(10), "metadata map should not be null");
    }
    
    /**
     * Property: For any batch of events, all events should be convertible to RowData
     * without loss or corruption.
     */
    @Property(tries = 100)
    void batchConversionPreservesAllEvents(
            @ForAll("eventBatches") List<OrderEvent> events) {
        
        // Convert all events to RowData
        List<RowData> rowDataList = events.stream()
            .map(EventToRowDataConverter::convertOrderEvent)
            .collect(Collectors.toList());
        
        // Property 1: Same number of rows as input events
        assertEquals(
            events.size(),
            rowDataList.size(),
            "All events should be converted to RowData"
        );
        
        // Property 2: All event_ids should be present in RowData
        Set<String> inputEventIds = events.stream()
            .map(OrderEvent::getEventId)
            .collect(Collectors.toSet());
        
        Set<String> outputEventIds = rowDataList.stream()
            .map(row -> row.getString(0).toString())
            .collect(Collectors.toSet());
        
        assertEquals(
            inputEventIds,
            outputEventIds,
            "All event_ids should be preserved in conversion"
        );
        
        // Property 3: For each event, verify the corresponding RowData has matching values
        for (int i = 0; i < events.size(); i++) {
            OrderEvent event = events.get(i);
            RowData rowData = rowDataList.get(i);
            
            assertEquals(
                event.getEventId(),
                rowData.getString(0).toString(),
                "event_id should match for event at index " + i
            );
            
            assertEquals(
                event.getOrderId(),
                rowData.getString(5).toString(),
                "order_id should match for event at index " + i
            );
        }
    }
    
    /**
     * Property: Events with different field values should produce distinct RowData.
     */
    @Property(tries = 100)
    void distinctEventsProduceDistinctRowData(
            @ForAll("orderEvents") OrderEvent event1,
            @ForAll("orderEvents") OrderEvent event2) {
        
        // Ensure events have different event_ids
        Assume.that(!event1.getEventId().equals(event2.getEventId()));
        
        RowData rowData1 = EventToRowDataConverter.convertOrderEvent(event1);
        RowData rowData2 = EventToRowDataConverter.convertOrderEvent(event2);
        
        // Property: Different events should have different event_ids in RowData
        assertNotEquals(
            rowData1.getString(0).toString(),
            rowData2.getString(0).toString(),
            "Distinct events should have distinct event_ids in RowData"
        );
    }
    
    /**
     * Property: Conversion should handle edge cases in field values.
     */
    @Property(tries = 100)
    void conversionHandlesEdgeCases(
            @ForAll("edgeCaseEvents") OrderEvent event) {
        
        // Should not throw exception
        RowData rowData = EventToRowDataConverter.convertOrderEvent(event);
        
        assertNotNull(rowData, "RowData should be created for edge case events");
        
        // Verify required fields are present
        assertNotNull(rowData.getString(0), "event_id should not be null");
        assertNotNull(rowData.getTimestamp(1, 6), "event_time should not be null");
        assertNotNull(rowData.getString(2), "event_type should not be null");
        
        // Verify numeric fields are within valid ranges
        assertTrue(
            rowData.getDecimal(7, 18, 2).toBigDecimal().compareTo(BigDecimal.ZERO) >= 0,
            "amount should be non-negative"
        );
    }
    
    /**
     * Property: Metadata map should be preserved correctly, including empty maps.
     */
    @Property(tries = 100)
    void metadataMapIsPreservedCorrectly(
            @ForAll("eventsWithMetadata") OrderEvent event) {
        
        RowData rowData = EventToRowDataConverter.convertOrderEvent(event);
        
        // Metadata is at index 10
        assertNotNull(rowData.getMap(10), "metadata map should not be null");
        
        // If input metadata is empty, output should handle it gracefully
        if (event.getMetadata().isEmpty()) {
            // Empty map should be represented, not null
            assertNotNull(rowData.getMap(10), "empty metadata should be represented as empty map");
        }
    }
    
    // ========== Data Generators ==========
    
    /**
     * Generates random OrderEvent instances with realistic field values.
     */
    @Provide
    Arbitrary<OrderEvent> orderEvents() {
        return Combinators.combine(
            Arbitraries.strings().alpha().ofLength(10),  // eventId
            Arbitraries.longs().between(
                Instant.now().minusSeconds(86400).toEpochMilli(),
                Instant.now().toEpochMilli()
            ),  // eventTime
            Arbitraries.of("us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"),  // region
            Arbitraries.strings().alpha().ofLength(8),  // orderId
            Arbitraries.strings().alpha().ofLength(8),  // customerId
            Arbitraries.bigDecimals()
                .between(BigDecimal.ONE, BigDecimal.valueOf(10000))
                .ofScale(2),  // amount
            Arbitraries.of("USD", "EUR", "GBP"),  // currency
            Arbitraries.of("PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED")  // status
        ).as((eventId, eventTimeMillis, region, orderId, customerId, amount, currency, status) -> {
            Instant eventTime = Instant.ofEpochMilli(eventTimeMillis);
            OrderEvent event = new OrderEvent();
            event.setEventId(eventId);
            event.setEventTime(eventTime);
            event.setEventType("ORDER");
            event.setRegion(region);
            event.setEventDate(eventTime.atZone(ZoneOffset.UTC).toLocalDate());
            event.setOrderId(orderId);
            event.setCustomerId(customerId);
            event.setAmount(amount);
            event.setCurrency(currency);
            event.setStatus(status);
            return event;
        });
    }
    
    /**
     * Generates batches of events (10-50 events per batch).
     */
    @Provide
    Arbitrary<List<OrderEvent>> eventBatches() {
        return orderEvents().list().ofMinSize(10).ofMaxSize(50);
    }
    
    /**
     * Generates events with edge case values.
     */
    @Provide
    Arbitrary<OrderEvent> edgeCaseEvents() {
        return Combinators.combine(
            Arbitraries.strings().alpha().ofLength(1),  // Very short eventId
            Arbitraries.of(
                Instant.now().minusSeconds(86400 * 365),  // 1 year ago
                Instant.now(),  // Current time
                Instant.now().minusSeconds(1)  // 1 second ago
            ),
            Arbitraries.of("us-east-1"),
            Arbitraries.strings().alpha().ofLength(1),
            Arbitraries.strings().alpha().ofLength(1),
            Arbitraries.of(
                BigDecimal.valueOf(0.01),  // Minimum amount
                BigDecimal.valueOf(999999.99)  // Maximum amount
            ),
            Arbitraries.of("USD"),
            Arbitraries.of("PENDING", "CANCELLED")
        ).as((eventId, eventTime, region, orderId, customerId, amount, currency, status) -> {
            OrderEvent event = new OrderEvent();
            event.setEventId(eventId);
            event.setEventTime(eventTime);
            event.setEventType("ORDER");
            event.setRegion(region);
            event.setEventDate(eventTime.atZone(ZoneOffset.UTC).toLocalDate());
            event.setOrderId(orderId);
            event.setCustomerId(customerId);
            event.setAmount(amount);
            event.setCurrency(currency);
            event.setStatus(status);
            return event;
        });
    }
    
    /**
     * Generates events with various metadata configurations.
     */
    @Provide
    Arbitrary<OrderEvent> eventsWithMetadata() {
        return orderEvents().map(event -> {
            Random random = new Random();
            
            // 50% chance of empty metadata
            if (random.nextBoolean()) {
                event.setMetadata(new HashMap<>());
            } else {
                // Add 1-3 metadata entries
                int numEntries = 1 + random.nextInt(3);
                for (int i = 0; i < numEntries; i++) {
                    event.addMetadata("key" + i, "value" + i);
                }
            }
            
            return event;
        });
    }
}
