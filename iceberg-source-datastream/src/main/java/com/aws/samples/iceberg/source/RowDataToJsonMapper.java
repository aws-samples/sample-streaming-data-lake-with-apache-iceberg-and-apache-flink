package com.aws.samples.iceberg.source;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
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
    
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ISO_INSTANT;
    
    private final Schema schema;
    private transient ObjectMapper objectMapper;
    private transient List<Types.NestedField> columns;
    
    public RowDataToJsonMapper(Schema schema) {
        this.schema = schema;
    }
    
    @Override
    public void open(Configuration parameters) {
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
            case MAP:
            case STRUCT:
                // For complex types, convert to string representation
                // In production, you might want more sophisticated handling
                jsonNode.put(fieldName, rowData.toString());
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
}
