package com.aws.samples.iceberg.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Maps Iceberg RowData to JSON strings for Kinesis output.
 * 
 * Handles all Iceberg primitive types:
 * - STRING, INT, LONG, FLOAT, DOUBLE, BOOLEAN
 * - DECIMAL
 * - DATE, TIME, TIMESTAMP, TIMESTAMPTZ
 * - BINARY, FIXED, UUID
 */
public class RowDataToJsonMapper extends RichMapFunction<RowData, String> {
    
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    
    private final Schema schema;
    private transient ObjectMapper objectMapper;
    private transient List<Types.NestedField> columns;
    
    public RowDataToJsonMapper(Schema schema) {
        this.schema = schema;
    }
    
    @Override
    public void open(OpenContext openContext) {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.columns = schema.columns();
    }
    
    @Override
    public String map(RowData rowData) throws Exception {
        ObjectNode jsonNode = objectMapper.createObjectNode();
        
        for (int i = 0; i < columns.size(); i++) {
            Types.NestedField field = columns.get(i);
            String fieldName = field.name();
            Type fieldType = field.type();
            
            if (rowData.isNullAt(i)) {
                jsonNode.putNull(fieldName);
                continue;
            }
            
            addFieldToJson(jsonNode, fieldName, fieldType, rowData, i);
        }
        
        return objectMapper.writeValueAsString(jsonNode);
    }
    
    private void addFieldToJson(ObjectNode jsonNode, String fieldName, Type type, RowData rowData, int pos) {
        switch (type.typeId()) {
            case BOOLEAN:
                jsonNode.put(fieldName, rowData.getBoolean(pos));
                break;
                
            case INTEGER:
                jsonNode.put(fieldName, rowData.getInt(pos));
                break;
                
            case LONG:
                jsonNode.put(fieldName, rowData.getLong(pos));
                break;
                
            case FLOAT:
                jsonNode.put(fieldName, rowData.getFloat(pos));
                break;
                
            case DOUBLE:
                jsonNode.put(fieldName, rowData.getDouble(pos));
                break;
                
            case STRING:
                StringData stringData = rowData.getString(pos);
                jsonNode.put(fieldName, stringData != null ? stringData.toString() : null);
                break;
                
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) type;
                DecimalData decimalData = rowData.getDecimal(pos, decimalType.precision(), decimalType.scale());
                if (decimalData != null) {
                    BigDecimal decimal = decimalData.toBigDecimal();
                    jsonNode.put(fieldName, decimal);
                } else {
                    jsonNode.putNull(fieldName);
                }
                break;
                
            case DATE:
                int daysSinceEpoch = rowData.getInt(pos);
                LocalDate date = LocalDate.ofEpochDay(daysSinceEpoch);
                jsonNode.put(fieldName, date.format(DATE_FORMATTER));
                break;
                
            case TIME:
                // Time is stored as microseconds since midnight
                long timeMicros = rowData.getLong(pos);
                jsonNode.put(fieldName, formatTime(timeMicros));
                break;
                
            case TIMESTAMP:
                TimestampData timestampData = rowData.getTimestamp(pos, 6);
                if (timestampData != null) {
                    Instant instant = timestampData.toInstant();
                    jsonNode.put(fieldName, TIMESTAMP_FORMATTER.format(instant));
                } else {
                    jsonNode.putNull(fieldName);
                }
                break;
                
            case BINARY:
            case FIXED:
                byte[] bytes = rowData.getBinary(pos);
                if (bytes != null) {
                    jsonNode.put(fieldName, java.util.Base64.getEncoder().encodeToString(bytes));
                } else {
                    jsonNode.putNull(fieldName);
                }
                break;
                
            case UUID:
                // UUID is stored as 16 bytes
                byte[] uuidBytes = rowData.getBinary(pos);
                if (uuidBytes != null && uuidBytes.length == 16) {
                    java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(uuidBytes);
                    long high = bb.getLong();
                    long low = bb.getLong();
                    java.util.UUID uuid = new java.util.UUID(high, low);
                    jsonNode.put(fieldName, uuid.toString());
                } else {
                    jsonNode.putNull(fieldName);
                }
                break;
                
            case LIST:
                // Handle LIST type - convert to JSON array
                org.apache.flink.table.data.ArrayData arrayData = rowData.getArray(pos);
                if (arrayData != null) {
                    Types.ListType listType = (Types.ListType) type;
                    com.fasterxml.jackson.databind.node.ArrayNode arrayNode = objectMapper.createArrayNode();
                    for (int j = 0; j < arrayData.size(); j++) {
                        addArrayElementToJson(arrayNode, listType.elementType(), arrayData, j);
                    }
                    jsonNode.set(fieldName, arrayNode);
                } else {
                    jsonNode.putNull(fieldName);
                }
                break;
                
            case MAP:
                // Handle MAP type - convert to JSON object
                org.apache.flink.table.data.MapData mapData = rowData.getMap(pos);
                if (mapData != null) {
                    Types.MapType mapType = (Types.MapType) type;
                    ObjectNode mapNode = objectMapper.createObjectNode();
                    org.apache.flink.table.data.ArrayData keyArray = mapData.keyArray();
                    org.apache.flink.table.data.ArrayData valueArray = mapData.valueArray();
                    for (int j = 0; j < mapData.size(); j++) {
                        String key = keyArray.getString(j).toString();
                        addMapValueToJson(mapNode, key, mapType.valueType(), valueArray, j);
                    }
                    jsonNode.set(fieldName, mapNode);
                } else {
                    jsonNode.putNull(fieldName);
                }
                break;
                
            case STRUCT:
                // For struct types, skip for now (would need recursive handling)
                jsonNode.putNull(fieldName);
                break;
                
            default:
                jsonNode.put(fieldName, rowData.toString());
        }
    }
    
    private String formatTime(long microseconds) {
        long hours = microseconds / 3_600_000_000L;
        long minutes = (microseconds % 3_600_000_000L) / 60_000_000L;
        long seconds = (microseconds % 60_000_000L) / 1_000_000L;
        long micros = microseconds % 1_000_000L;
        
        return String.format("%02d:%02d:%02d.%06d", hours, minutes, seconds, micros);
    }
    
    private void addArrayElementToJson(com.fasterxml.jackson.databind.node.ArrayNode arrayNode, Type elementType, 
                                        org.apache.flink.table.data.ArrayData arrayData, int pos) {
        if (arrayData.isNullAt(pos)) {
            arrayNode.addNull();
            return;
        }
        
        switch (elementType.typeId()) {
            case BOOLEAN:
                arrayNode.add(arrayData.getBoolean(pos));
                break;
            case INTEGER:
                arrayNode.add(arrayData.getInt(pos));
                break;
            case LONG:
                arrayNode.add(arrayData.getLong(pos));
                break;
            case FLOAT:
                arrayNode.add(arrayData.getFloat(pos));
                break;
            case DOUBLE:
                arrayNode.add(arrayData.getDouble(pos));
                break;
            case STRING:
                arrayNode.add(arrayData.getString(pos).toString());
                break;
            default:
                arrayNode.add(arrayData.getString(pos).toString());
        }
    }
    
    private void addMapValueToJson(ObjectNode mapNode, String key, Type valueType,
                                   org.apache.flink.table.data.ArrayData valueArray, int pos) {
        if (valueArray.isNullAt(pos)) {
            mapNode.putNull(key);
            return;
        }
        
        switch (valueType.typeId()) {
            case BOOLEAN:
                mapNode.put(key, valueArray.getBoolean(pos));
                break;
            case INTEGER:
                mapNode.put(key, valueArray.getInt(pos));
                break;
            case LONG:
                mapNode.put(key, valueArray.getLong(pos));
                break;
            case FLOAT:
                mapNode.put(key, valueArray.getFloat(pos));
                break;
            case DOUBLE:
                mapNode.put(key, valueArray.getDouble(pos));
                break;
            case STRING:
                mapNode.put(key, valueArray.getString(pos).toString());
                break;
            default:
                mapNode.put(key, valueArray.getString(pos).toString());
        }
    }
}
