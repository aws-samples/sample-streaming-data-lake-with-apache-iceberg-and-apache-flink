package com.aws.samples.iceberg.dynamic;

import com.aws.samples.iceberg.model.BaseEvent;
import com.aws.samples.iceberg.model.ClickEvent;
import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.model.UserEvent;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dynamic record generator with TRUE dynamic schema inference.
 * 
 * Key design principles:
 * 1. STABLE FIELD IDS: Same field name always gets same ID (critical for schema comparison)
 * 2. SCHEMA CACHING: Reuse Schema instances for same field combinations (performance)
 * 3. DYNAMIC ROWDATA: Build RowData to match the dynamically inferred schema
 * 
 * Requirements: 4.2, 4.3
 */
public class EventRoutingGenerator implements DynamicRecordGenerator<BaseEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(EventRoutingGenerator.class);
    
    private final String database;
    
    // Schema cache: reuse Schema instances for performance
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    
    // Field ID registry: ensures stable field IDs across schema variations
    private final Map<String, Integer> fieldIdRegistry = new ConcurrentHashMap<>();
    private int nextFieldId = 1;
    
    // Partition spec cache
    private final Map<String, PartitionSpec> partitionSpecCache = new ConcurrentHashMap<>();
    
    public EventRoutingGenerator(String database) {
        this.database = database;
        // Pre-register common field IDs for stability
        registerCommonFields();
    }
    
    /**
     * Pre-register common fields to ensure stable IDs.
     */
    private void registerCommonFields() {
        getOrAssignFieldId("event_id");
        getOrAssignFieldId("event_time");
        getOrAssignFieldId("event_type");
        getOrAssignFieldId("region");
        getOrAssignFieldId("event_date");
    }
    
    @Override
    public void generate(BaseEvent event, Collector<DynamicRecord> out) {
        try {
            String tableName = getTableNameForEvent(event);
            TableIdentifier tableId = TableIdentifier.of(database, tableName);
            
            // Get schema signature
            String schemaSignature = getSchemaSignature(event);
            
            // Get or create schema
            Schema schema = schemaCache.computeIfAbsent(schemaSignature, sig -> {
                Schema newSchema = deriveSchemaFromActualEvent(event);
                LOG.info("Created new schema for signature '{}' with {} fields", sig, newSchema.columns().size());
                return newSchema;
            });
            
            // Build RowData dynamically to match the schema
            RowData rowData = convertEventToRowData(event, schema);
            
            // Get partition spec
            PartitionSpec partitionSpec = partitionSpecCache.computeIfAbsent(
                schemaSignature,
                sig -> PartitionSpec.builderFor(schema)
                    .day("event_date")
                    .identity("region")
                    .build()
            );
            
            // Create dynamic record
            DynamicRecord dynamicRecord = new DynamicRecord(
                tableId,
                "main",
                schema,
                rowData,
                partitionSpec,
                DistributionMode.NONE,
                4
            );
            
            out.collect(dynamicRecord);
            
        } catch (Exception e) {
            LOG.error("Failed to generate dynamic record for event: {}", event.getEventId(), e);
        }
    }
    
    /**
     * Get or assign a stable field ID for a field name.
     */
    private synchronized int getOrAssignFieldId(String fieldName) {
        return fieldIdRegistry.computeIfAbsent(fieldName, name -> nextFieldId++);
    }
    
    /**
     * Get schema signature for caching.
     */
    private String getSchemaSignature(BaseEvent event) {
        StringBuilder sig = new StringBuilder(event.getEventType());
        
        if (event instanceof UserEvent) {
            if (((UserEvent) event).getUserAgent() != null) sig.append(":userAgent");
        } else if (event instanceof ClickEvent) {
            ClickEvent ce = (ClickEvent) event;
            if (ce.getReferrer() != null) sig.append(":referrer");
            if (ce.getScrollDepth() != null) sig.append(":scrollDepth");
            if (ce.getTimeOnPageSeconds() != null) sig.append(":timeOnPage");
        }
        
        return sig.toString();
    }
    
    /**
     * Get table name based on event type.
     */
    private String getTableNameForEvent(BaseEvent event) {
        switch (event.getEventType()) {
            case "ORDER": return "orders_dynamic";
            case "USER": return "users_dynamic";
            case "CLICK": return "clicks_dynamic";
            default: throw new IllegalArgumentException("Unknown event type: " + event.getEventType());
        }
    }
    
    /**
     * Derive schema from actual event data with STABLE field IDs.
     */
    private Schema deriveSchemaFromActualEvent(BaseEvent event) {
        List<Types.NestedField> fields = new ArrayList<>();
        
        // Common fields with stable IDs
        fields.add(Types.NestedField.required(getOrAssignFieldId("event_id"), "event_id", Types.StringType.get()));
        fields.add(Types.NestedField.required(getOrAssignFieldId("event_time"), "event_time", Types.TimestampType.withZone()));
        fields.add(Types.NestedField.required(getOrAssignFieldId("event_type"), "event_type", Types.StringType.get()));
        fields.add(Types.NestedField.required(getOrAssignFieldId("region"), "region", Types.StringType.get()));
        fields.add(Types.NestedField.required(getOrAssignFieldId("event_date"), "event_date", Types.DateType.get()));
        
        // Type-specific fields
        if (event instanceof OrderEvent) {
            fields.add(Types.NestedField.required(getOrAssignFieldId("order_id"), "order_id", Types.StringType.get()));
            fields.add(Types.NestedField.required(getOrAssignFieldId("customer_id"), "customer_id", Types.StringType.get()));
            fields.add(Types.NestedField.required(getOrAssignFieldId("amount"), "amount", Types.DecimalType.of(18, 2)));
            fields.add(Types.NestedField.required(getOrAssignFieldId("currency"), "currency", Types.StringType.get()));
            fields.add(Types.NestedField.required(getOrAssignFieldId("status"), "status", Types.StringType.get()));
            fields.add(Types.NestedField.optional(getOrAssignFieldId("metadata"), "metadata", Types.MapType.ofOptional(
                getOrAssignFieldId("metadata_key"), getOrAssignFieldId("metadata_value"),
                Types.StringType.get(), Types.StringType.get())));
            
        } else if (event instanceof UserEvent) {
            UserEvent ue = (UserEvent) event;
            fields.add(Types.NestedField.required(getOrAssignFieldId("user_id"), "user_id", Types.StringType.get()));
            fields.add(Types.NestedField.required(getOrAssignFieldId("action"), "action", Types.StringType.get()));
            fields.add(Types.NestedField.required(getOrAssignFieldId("device_type"), "device_type", Types.StringType.get()));
            fields.add(Types.NestedField.required(getOrAssignFieldId("ip_address"), "ip_address", Types.StringType.get()));
            
            // Only add if present
            if (ue.getUserAgent() != null) {
                fields.add(Types.NestedField.optional(getOrAssignFieldId("user_agent"), "user_agent", Types.StringType.get()));
            }
            
            fields.add(Types.NestedField.optional(getOrAssignFieldId("metadata"), "metadata", Types.MapType.ofOptional(
                getOrAssignFieldId("metadata_key"), getOrAssignFieldId("metadata_value"),
                Types.StringType.get(), Types.StringType.get())));
            
        } else if (event instanceof ClickEvent) {
            ClickEvent ce = (ClickEvent) event;
            fields.add(Types.NestedField.required(getOrAssignFieldId("session_id"), "session_id", Types.StringType.get()));
            fields.add(Types.NestedField.required(getOrAssignFieldId("page_url"), "page_url", Types.StringType.get()));
            
            if (ce.getReferrer() != null) {
                fields.add(Types.NestedField.optional(getOrAssignFieldId("referrer"), "referrer", Types.StringType.get()));
            }
            if (ce.getScrollDepth() != null) {
                fields.add(Types.NestedField.optional(getOrAssignFieldId("scroll_depth"), "scroll_depth", Types.IntegerType.get()));
            }
            if (ce.getTimeOnPageSeconds() != null) {
                fields.add(Types.NestedField.optional(getOrAssignFieldId("time_on_page_seconds"), "time_on_page_seconds", Types.LongType.get()));
            }
            
            fields.add(Types.NestedField.optional(getOrAssignFieldId("metadata"), "metadata", Types.MapType.ofOptional(
                getOrAssignFieldId("metadata_key"), getOrAssignFieldId("metadata_value"),
                Types.StringType.get(), Types.StringType.get())));
        }
        
        return new Schema(fields);
    }
    
    /**
     * Convert event to RowData dynamically based on the schema.
     * This builds RowData with fields in the same order as the schema.
     */
    private RowData convertEventToRowData(BaseEvent event, Schema schema) {
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, schema.columns().size());
        
        int fieldIndex = 0;
        for (Types.NestedField field : schema.columns()) {
            Object value = getFieldValue(event, field.name());
            rowData.setField(fieldIndex++, value);
        }
        
        return rowData;
    }
    
    /**
     * Get field value from event by field name.
     */
    private Object getFieldValue(BaseEvent event, String fieldName) {
        switch (fieldName) {
            case "event_id": return StringData.fromString(event.getEventId());
            case "event_time": return TimestampData.fromInstant(event.getEventTime());
            case "event_type": return StringData.fromString(event.getEventType());
            case "region": return StringData.fromString(event.getRegion());
            case "event_date": return (int) event.getEventDate().toEpochDay();
            case "metadata": return convertMetadata(event.getMetadata());
        }
        
        if (event instanceof OrderEvent) {
            OrderEvent oe = (OrderEvent) event;
            switch (fieldName) {
                case "order_id": return StringData.fromString(oe.getOrderId());
                case "customer_id": return StringData.fromString(oe.getCustomerId());
                case "amount": return org.apache.flink.table.data.DecimalData.fromBigDecimal(oe.getAmount(), 18, 2);
                case "currency": return StringData.fromString(oe.getCurrency());
                case "status": return StringData.fromString(oe.getStatus());
            }
        } else if (event instanceof UserEvent) {
            UserEvent ue = (UserEvent) event;
            switch (fieldName) {
                case "user_id": return StringData.fromString(ue.getUserId());
                case "action": return StringData.fromString(ue.getAction());
                case "device_type": return StringData.fromString(ue.getDeviceType());
                case "ip_address": return StringData.fromString(ue.getIpAddress());
                case "user_agent": return ue.getUserAgent() != null ? StringData.fromString(ue.getUserAgent()) : null;
            }
        } else if (event instanceof ClickEvent) {
            ClickEvent ce = (ClickEvent) event;
            switch (fieldName) {
                case "session_id": return StringData.fromString(ce.getSessionId());
                case "page_url": return StringData.fromString(ce.getPageUrl());
                case "referrer": return ce.getReferrer() != null ? StringData.fromString(ce.getReferrer()) : null;
                case "scroll_depth": return ce.getScrollDepth();
                case "time_on_page_seconds": return ce.getTimeOnPageSeconds();
            }
        }
        
        return null;
    }
    
    /**
     * Convert metadata map to Flink MapData.
     */
    private GenericMapData convertMetadata(Map<String, String> metadata) {
        if (metadata == null || metadata.isEmpty()) {
            return new GenericMapData(new HashMap<>());
        }
        
        Map<StringData, StringData> mapData = new HashMap<>();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            mapData.put(StringData.fromString(entry.getKey()), StringData.fromString(entry.getValue()));
        }
        return new GenericMapData(mapData);
    }
}
