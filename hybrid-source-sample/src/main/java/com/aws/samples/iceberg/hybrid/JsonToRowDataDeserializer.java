package com.aws.samples.iceberg.hybrid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Deserializes JSON messages from Kinesis into Flink RowData format.
 * 
 * This deserializer converts JSON to RowData using an Iceberg schema,
 * enabling the HybridSource to produce a unified type from both
 * Iceberg (RowData) and Kinesis (JSON -> RowData) sources.
 * 
 * Supports Iceberg types:
 * - STRING, BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE
 * - DECIMAL
 * - DATE, TIMESTAMP, TIMESTAMPTZ
 * - MAP (string keys only)
 */
public class JsonToRowDataDeserializer implements DeserializationSchema<RowData> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonToRowDataDeserializer.class);
    
    private final Schema schema;
    private transient ObjectMapper objectMapper;
    private transient List<Types.NestedField> columns;
    
    public JsonToRowDataDeserializer(Schema schema) {
        this.schema = schema;
    }
    
    @Override
    public void open(InitializationContext context) {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.columns = schema.columns();
        LOG.info("Initialized JsonToRowDataDeserializer with {} columns", columns.size());
    }
    
    @Override
    public RowData deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        
        JsonNode jsonNode = objectMapper.readTree(message);
        GenericRowData rowData = new GenericRowData(columns.size());
        
        for (int i = 0; i < columns.size(); i++) {
            Types.NestedField field = columns.get(i);
            String fieldName = field.name();
            Type fieldType = field.type();
            
            JsonNode fieldValue = jsonNode.get(fieldName);
            
            if (fieldValue == null || fieldValue.isNull()) {
                rowData.setField(i, null);
            } else {
                rowData.setField(i, convertJsonToFlink(fieldValue, fieldType));
            }
        }
        
        return rowData;
    }
    
    private Object convertJsonToFlink(JsonNode jsonNode, Type type) {
        if (jsonNode == null || jsonNode.isNull()) {
            return null;
        }
        
        switch (type.typeId()) {
            case BOOLEAN:
                return jsonNode.asBoolean();
                
            case INTEGER:
                return jsonNode.asInt();
                
            case LONG:
                return jsonNode.asLong();
                
            case FLOAT:
                return (float) jsonNode.asDouble();
                
            case DOUBLE:
                return jsonNode.asDouble();
                
            case STRING:
                return StringData.fromString(jsonNode.asText());
                
            case DECIMAL:
                Types.DecimalType decimalType = (Types.DecimalType) type;
                return org.apache.flink.table.data.DecimalData.fromBigDecimal(
                        jsonNode.decimalValue(),
                        decimalType.precision(),
                        decimalType.scale()
                );
                
            case DATE:
                // Parse ISO date string to days since epoch
                String dateStr = jsonNode.asText();
                LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
                return (int) date.toEpochDay();
                
            case TIMESTAMP:
                // Parse ISO timestamp string
                String timestampStr = jsonNode.asText();
                Instant instant = Instant.parse(timestampStr);
                return TimestampData.fromInstant(instant);
                
            case MAP:
                Types.MapType mapType = (Types.MapType) type;
                return convertJsonToMap(jsonNode, mapType);
                
            case LIST:
                // For simplicity, convert list to string representation
                return StringData.fromString(jsonNode.toString());
                
            case STRUCT:
                // For simplicity, convert struct to string representation
                return StringData.fromString(jsonNode.toString());
                
            default:
                return StringData.fromString(jsonNode.asText());
        }
    }
    
    private GenericMapData convertJsonToMap(JsonNode jsonNode, Types.MapType mapType) {
        Map<StringData, Object> map = new HashMap<>();
        Type valueType = mapType.valueType();
        
        Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();
            StringData key = StringData.fromString(entry.getKey());
            Object value = convertJsonToFlink(entry.getValue(), valueType);
            map.put(key, value);
        }
        
        return new GenericMapData(map);
    }
    
    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }
    
    @Override
    public TypeInformation<RowData> getProducedType() {
        return TypeInformation.of(RowData.class);
    }
}
