package com.aws.samples.iceberg.util;

import com.aws.samples.iceberg.model.BaseEvent;
import com.aws.samples.iceberg.model.ClickEvent;
import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.model.UserEvent;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for converting event POJOs to Flink RowData.
 * RowData is the internal data format used by Flink's Table API and Iceberg sink.
 */
public class EventToRowDataConverter {
    
    /**
     * Convert a BaseEvent to RowData based on its concrete type.
     */
    public static RowData convert(BaseEvent event) {
        if (event instanceof OrderEvent) {
            return convertOrderEvent((OrderEvent) event);
        } else if (event instanceof UserEvent) {
            return convertUserEvent((UserEvent) event);
        } else if (event instanceof ClickEvent) {
            return convertClickEvent((ClickEvent) event);
        } else {
            throw new IllegalArgumentException("Unknown event type: " + event.getClass().getName());
        }
    }
    
    /**
     * Convert OrderEvent to RowData.
     * Schema: event_id, event_time, event_type, region, event_date, order_id, customer_id, amount, currency, status, metadata
     */
    public static RowData convertOrderEvent(OrderEvent event) {
        GenericRowData rowData = new GenericRowData(11);
        
        // Common fields
        rowData.setField(0, StringData.fromString(event.getEventId()));
        rowData.setField(1, TimestampData.fromInstant(event.getEventTime()));
        rowData.setField(2, StringData.fromString(event.getEventType()));
        rowData.setField(3, StringData.fromString(event.getRegion()));
        rowData.setField(4, (int) event.getEventDate().toEpochDay());
        
        // OrderEvent specific fields
        rowData.setField(5, StringData.fromString(event.getOrderId()));
        rowData.setField(6, StringData.fromString(event.getCustomerId()));
        rowData.setField(7, org.apache.flink.table.data.DecimalData.fromBigDecimal(event.getAmount(), 18, 2));
        rowData.setField(8, StringData.fromString(event.getCurrency()));
        rowData.setField(9, StringData.fromString(event.getStatus()));
        
        // Metadata map
        rowData.setField(10, convertMetadataToMapData(event.getMetadata()));
        
        return rowData;
    }
    
    /**
     * Convert UserEvent to RowData.
     * Schema: event_id, event_time, event_type, region, event_date, user_id, action, device_type, ip_address, user_agent, metadata
     */
    public static RowData convertUserEvent(UserEvent event) {
        GenericRowData rowData = new GenericRowData(11);
        
        // Common fields
        rowData.setField(0, StringData.fromString(event.getEventId()));
        rowData.setField(1, TimestampData.fromInstant(event.getEventTime()));
        rowData.setField(2, StringData.fromString(event.getEventType()));
        rowData.setField(3, StringData.fromString(event.getRegion()));
        rowData.setField(4, (int) event.getEventDate().toEpochDay());
        
        // UserEvent specific fields
        rowData.setField(5, StringData.fromString(event.getUserId()));
        rowData.setField(6, StringData.fromString(event.getAction()));
        rowData.setField(7, StringData.fromString(event.getDeviceType()));
        rowData.setField(8, StringData.fromString(event.getIpAddress()));
        rowData.setField(9, event.getUserAgent() != null ? StringData.fromString(event.getUserAgent()) : null);
        
        // Metadata map
        rowData.setField(10, convertMetadataToMapData(event.getMetadata()));
        
        return rowData;
    }
    
    /**
     * Convert ClickEvent to RowData.
     * Schema: event_id, event_time, event_type, region, event_date, session_id, page_url, referrer, scroll_depth, time_on_page_seconds, metadata
     */
    public static RowData convertClickEvent(ClickEvent event) {
        GenericRowData rowData = new GenericRowData(11);
        
        // Common fields
        rowData.setField(0, StringData.fromString(event.getEventId()));
        rowData.setField(1, TimestampData.fromInstant(event.getEventTime()));
        rowData.setField(2, StringData.fromString(event.getEventType()));
        rowData.setField(3, StringData.fromString(event.getRegion()));
        rowData.setField(4, (int) event.getEventDate().toEpochDay());
        
        // ClickEvent specific fields
        rowData.setField(5, StringData.fromString(event.getSessionId()));
        rowData.setField(6, StringData.fromString(event.getPageUrl()));
        rowData.setField(7, event.getReferrer() != null ? StringData.fromString(event.getReferrer()) : null);
        rowData.setField(8, event.getScrollDepth());
        rowData.setField(9, event.getTimeOnPageSeconds());
        
        // Metadata map
        rowData.setField(10, convertMetadataToMapData(event.getMetadata()));
        
        return rowData;
    }
    
    /**
     * Convert metadata Map<String, String> to Flink MapData.
     */
    private static GenericMapData convertMetadataToMapData(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return new GenericMapData(new HashMap<>());
        }
        
        Map<StringData, StringData> mapData = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            mapData.put(
                StringData.fromString(entry.getKey()),
                StringData.fromString(entry.getValue())
            );
        }
        
        return new GenericMapData(mapData);
    }
    
    /**
     * Helper method to convert LocalDate to epoch day (int).
     */
    public static int localDateToEpochDay(LocalDate date) {
        return (int) date.toEpochDay();
    }
    
    /**
     * Helper method to convert epoch day (int) to LocalDate.
     */
    public static LocalDate epochDayToLocalDate(int epochDay) {
        return LocalDate.ofEpochDay(epochDay);
    }
}
