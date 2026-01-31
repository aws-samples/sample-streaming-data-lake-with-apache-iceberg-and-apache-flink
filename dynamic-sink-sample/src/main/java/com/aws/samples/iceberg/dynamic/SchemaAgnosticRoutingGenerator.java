package com.aws.samples.iceberg.dynamic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
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
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Truly schema-agnostic dynamic record generator.
 * 
 * This generator:
 * 1. Accepts raw JSON (JsonNode) - no typed POJOs required
 * 2. Infers Iceberg schema from JSON structure dynamically
 * 3. Routes to tables based on a configurable routing field (default: event_type)
 * 4. Handles ANY JSON structure - completely schema-agnostic
 * 5. Supports schema evolution as new fields appear
 * 
 * Table naming: {routing_field_value}_events (e.g., order_events, user_events)
 * Or custom table name field can be specified.
 */
public class SchemaAgnosticRoutingGenerator implements DynamicRecordGenerator<JsonNode> {
    
    private static final Logger LOG = LoggerFactory.getLogger(SchemaAgnosticRoutingGenerator.class);
    
    private final String database;
    private final String routingField;      // Field used for table routing (e.g., "event_type")
    private final String tableNameField;    // Optional: explicit table name field
    private final String tableSuffix;       // Suffix for auto-generated table names
    
    // Field ID registry: ensures stable field IDs across schema variations
    private final Map<String, Integer> fieldIdRegistry = new ConcurrentHashMap<>();
    private int nextFieldId = 1;
    
    // Schema cache: key = sorted field signature, value = Schema
    private final Map<String, Schema> schemaCache = new ConcurrentHashMap<>();
    
    // Partition spec cache
    private final Map<String, PartitionSpec> partitionSpecCache = new ConcurrentHashMap<>();
    
    // Partition fields (if present in data, will be used for partitioning)
    private final List<String> partitionCandidates;

    /**
     * Create with default settings.
     * Routes by "event_type" field, creates tables like "order_events".
     */
    public SchemaAgnosticRoutingGenerator(String database) {
        this(database, "event_type", null, "_events", Arrays.asList("event_date", "region", "date"));
    }
    
    /**
     * Create with custom routing configuration.
     * 
     * @param database Target database/namespace
     * @param routingField Field to determine table routing (e.g., "event_type")
     * @param tableNameField Optional field containing explicit table name (overrides routing)
     * @param tableSuffix Suffix for auto-generated table names
     * @param partitionCandidates Fields to use for partitioning if present
     */
    public SchemaAgnosticRoutingGenerator(
            String database,
            String routingField,
            String tableNameField,
            String tableSuffix,
            List<String> partitionCandidates) {
        this.database = database;
        this.routingField = routingField;
        this.tableNameField = tableNameField;
        this.tableSuffix = tableSuffix;
        this.partitionCandidates = partitionCandidates != null ? partitionCandidates : Collections.emptyList();
    }
    
    @Override
    public void generate(JsonNode json, Collector<DynamicRecord> out) {
        try {
            if (json == null || !json.isObject()) {
                LOG.warn("Skipping non-object JSON: {}", json);
                return;
            }
            
            // Determine target table
            String tableName = determineTableName(json);
            if (tableName == null) {
                LOG.warn("Could not determine table name for record, skipping");
                return;
            }
            
            TableIdentifier tableId = TableIdentifier.of(database, tableName);
            
            // Generate schema signature for caching
            String schemaSignature = generateSchemaSignature(json, tableName);
            
            // Get or create schema
            Schema schema = schemaCache.computeIfAbsent(schemaSignature, sig -> {
                Schema newSchema = inferSchemaFromJson(json);
                LOG.info("Created new schema for table '{}' with {} fields: {}", 
                    tableName, newSchema.columns().size(), getFieldNames(newSchema));
                return newSchema;
            });
            
            // Build RowData from JSON
            RowData rowData = convertJsonToRowData(json, schema);
            
            // Get or create partition spec
            PartitionSpec partitionSpec = partitionSpecCache.computeIfAbsent(
                schemaSignature,
                sig -> buildPartitionSpec(schema)
            );
            
            // Create dynamic record
            DynamicRecord record = new DynamicRecord(
                tableId,
                "main",           // branch
                schema,
                rowData,
                partitionSpec,
                DistributionMode.NONE,
                4                 // write parallelism hint
            );
            
            out.collect(record);
            
        } catch (Exception e) {
            LOG.error("Failed to generate dynamic record: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Determine table name from JSON.
     */
    private String determineTableName(JsonNode json) {
        // First check explicit table name field
        if (tableNameField != null && json.has(tableNameField)) {
            JsonNode tableNode = json.get(tableNameField);
            if (tableNode.isTextual()) {
                return tableNode.asText().toLowerCase();
            }
        }
        
        // Use routing field to generate table name
        if (json.has(routingField)) {
            JsonNode routingNode = json.get(routingField);
            if (routingNode.isTextual()) {
                String routingValue = routingNode.asText().toLowerCase();
                return routingValue + tableSuffix;
            }
        }
        
        // Fallback to generic table
        return "unknown_events";
    }
    
    /**
     * Generate schema signature for caching.
     * Includes table name + sorted field names + types.
     */
    private String generateSchemaSignature(JsonNode json, String tableName) {
        StringBuilder sig = new StringBuilder(tableName).append(":");
        
        List<String> fieldSigs = new ArrayList<>();
        Iterator<String> fieldNames = json.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode value = json.get(fieldName);
            String typeCode = getTypeCode(value);
            fieldSigs.add(fieldName + "=" + typeCode);
        }
        
        Collections.sort(fieldSigs);
        sig.append(String.join(",", fieldSigs));
        
        return sig.toString();
    }
    
    /**
     * Get type code for schema signature.
     */
    private String getTypeCode(JsonNode value) {
        if (value == null || value.isNull()) return "null";
        if (value.isTextual()) {
            String text = value.asText();
            // Check for timestamp patterns
            if (isTimestamp(text)) return "timestamp";
            if (isDate(text)) return "date";
            return "string";
        }
        if (value.isInt()) return "int";
        if (value.isLong()) return "long";
        if (value.isDouble() || value.isFloat()) return "double";
        if (value.isBigDecimal()) return "decimal";
        if (value.isBoolean()) return "boolean";
        if (value.isArray()) return "array";
        if (value.isObject()) return "struct";
        return "binary";
    }
    
    /**
     * Check if string looks like a timestamp.
     */
    private boolean isTimestamp(String text) {
        if (text == null || text.length() < 19) return false;
        try {
            // Try ISO-8601 formats
            if (text.contains("T")) {
                Instant.parse(text);
                return true;
            }
        } catch (DateTimeParseException e) {
            // Not a timestamp
        }
        return false;
    }
    
    /**
     * Check if string looks like a date.
     */
    private boolean isDate(String text) {
        if (text == null || text.length() != 10) return false;
        try {
            LocalDate.parse(text);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }
    
    /**
     * Infer Iceberg schema from JSON structure.
     */
    private Schema inferSchemaFromJson(JsonNode json) {
        List<Types.NestedField> fields = new ArrayList<>();
        
        Iterator<String> fieldNames = json.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode value = json.get(fieldName);
            
            int fieldId = getOrAssignFieldId(fieldName);
            Type icebergType = inferIcebergType(value, fieldName);
            
            // Make fields optional by default (safer for schema evolution)
            boolean isRequired = isRequiredField(fieldName);
            
            if (isRequired) {
                fields.add(Types.NestedField.required(fieldId, fieldName, icebergType));
            } else {
                fields.add(Types.NestedField.optional(fieldId, fieldName, icebergType));
            }
        }
        
        // Sort fields by ID for consistency
        fields.sort(Comparator.comparingInt(Types.NestedField::fieldId));
        
        return new Schema(fields);
    }
    
    /**
     * Determine if a field should be required.
     */
    private boolean isRequiredField(String fieldName) {
        // Only routing field and common identifiers are required
        return fieldName.equals(routingField) || 
               fieldName.equals("event_id") || 
               fieldName.equals("id");
    }
    
    /**
     * Infer Iceberg type from JSON value.
     */
    private Type inferIcebergType(JsonNode value, String fieldName) {
        if (value == null || value.isNull()) {
            return Types.StringType.get(); // Default to string for null
        }
        
        if (value.isTextual()) {
            String text = value.asText();
            
            // Check for timestamp
            if (isTimestamp(text)) {
                return Types.TimestampType.withZone();
            }
            
            // Check for date
            if (isDate(text)) {
                return Types.DateType.get();
            }
            
            return Types.StringType.get();
        }
        
        if (value.isInt()) {
            return Types.IntegerType.get();
        }
        
        if (value.isLong()) {
            return Types.LongType.get();
        }
        
        if (value.isDouble() || value.isFloat()) {
            return Types.DoubleType.get();
        }
        
        if (value.isBigDecimal()) {
            return Types.DecimalType.of(38, 10);
        }
        
        if (value.isBoolean()) {
            return Types.BooleanType.get();
        }
        
        if (value.isArray()) {
            return inferArrayType((ArrayNode) value, fieldName);
        }
        
        if (value.isObject()) {
            return inferStructType((ObjectNode) value, fieldName);
        }
        
        // Default to string for unknown types
        return Types.StringType.get();
    }
    
    /**
     * Infer array type from JSON array.
     */
    private Type inferArrayType(ArrayNode array, String fieldName) {
        if (array.isEmpty()) {
            // Default to string array for empty arrays
            return Types.ListType.ofOptional(
                getOrAssignFieldId(fieldName + "_element"),
                Types.StringType.get()
            );
        }
        
        // Infer element type from first non-null element
        Type elementType = Types.StringType.get();
        for (JsonNode element : array) {
            if (!element.isNull()) {
                elementType = inferIcebergType(element, fieldName + "_element");
                break;
            }
        }
        
        return Types.ListType.ofOptional(
            getOrAssignFieldId(fieldName + "_element"),
            elementType
        );
    }
    
    /**
     * Infer struct type from JSON object.
     */
    private Type inferStructType(ObjectNode obj, String parentFieldName) {
        List<Types.NestedField> nestedFields = new ArrayList<>();
        
        Iterator<String> fieldNames = obj.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode value = obj.get(fieldName);
            
            String fullFieldName = parentFieldName + "." + fieldName;
            int fieldId = getOrAssignFieldId(fullFieldName);
            Type fieldType = inferIcebergType(value, fullFieldName);
            
            nestedFields.add(Types.NestedField.optional(fieldId, fieldName, fieldType));
        }
        
        return Types.StructType.of(nestedFields);
    }
    
    /**
     * Get or assign a stable field ID.
     */
    private synchronized int getOrAssignFieldId(String fieldName) {
        return fieldIdRegistry.computeIfAbsent(fieldName, name -> nextFieldId++);
    }
    
    /**
     * Convert JSON to Flink RowData based on schema.
     */
    private RowData convertJsonToRowData(JsonNode json, Schema schema) {
        GenericRowData rowData = new GenericRowData(RowKind.INSERT, schema.columns().size());
        
        int fieldIndex = 0;
        for (Types.NestedField field : schema.columns()) {
            JsonNode value = json.get(field.name());
            Object converted = convertJsonValue(value, field.type());
            rowData.setField(fieldIndex++, converted);
        }
        
        return rowData;
    }
    
    /**
     * Convert JSON value to Flink internal format.
     */
    private Object convertJsonValue(JsonNode value, Type type) {
        if (value == null || value.isNull()) {
            return null;
        }
        
        if (type instanceof Types.StringType) {
            return StringData.fromString(value.asText());
        }
        
        if (type instanceof Types.IntegerType) {
            return value.asInt();
        }
        
        if (type instanceof Types.LongType) {
            return value.asLong();
        }
        
        if (type instanceof Types.DoubleType) {
            return value.asDouble();
        }
        
        if (type instanceof Types.FloatType) {
            return (float) value.asDouble();
        }
        
        if (type instanceof Types.BooleanType) {
            return value.asBoolean();
        }
        
        if (type instanceof Types.DecimalType) {
            Types.DecimalType decimalType = (Types.DecimalType) type;
            BigDecimal decimal = value.isBigDecimal() ? 
                value.decimalValue() : new BigDecimal(value.asText());
            return DecimalData.fromBigDecimal(decimal, decimalType.precision(), decimalType.scale());
        }
        
        if (type instanceof Types.TimestampType) {
            if (value.isTextual()) {
                Instant instant = Instant.parse(value.asText());
                return TimestampData.fromInstant(instant);
            }
            if (value.isLong()) {
                return TimestampData.fromEpochMillis(value.asLong());
            }
        }
        
        if (type instanceof Types.DateType) {
            if (value.isTextual()) {
                LocalDate date = LocalDate.parse(value.asText());
                return (int) date.toEpochDay();
            }
            if (value.isInt()) {
                return value.asInt();
            }
        }
        
        if (type instanceof Types.ListType) {
            return convertArrayValue((ArrayNode) value, (Types.ListType) type);
        }
        
        if (type instanceof Types.MapType) {
            return convertMapValue((ObjectNode) value, (Types.MapType) type);
        }
        
        if (type instanceof Types.StructType) {
            return convertStructValue((ObjectNode) value, (Types.StructType) type);
        }
        
        // Fallback: convert to string
        return StringData.fromString(value.toString());
    }
    
    /**
     * Convert JSON array to Flink ArrayData.
     */
    private ArrayData convertArrayValue(ArrayNode array, Types.ListType listType) {
        if (array == null || array.isEmpty()) {
            return new GenericArrayData(new Object[0]);
        }
        
        Type elementType = listType.elementType();
        Object[] elements = new Object[array.size()];
        
        for (int i = 0; i < array.size(); i++) {
            elements[i] = convertJsonValue(array.get(i), elementType);
        }
        
        return new GenericArrayData(elements);
    }
    
    /**
     * Convert JSON object to Flink MapData.
     */
    private MapData convertMapValue(ObjectNode obj, Types.MapType mapType) {
        if (obj == null || obj.isEmpty()) {
            return new GenericMapData(new HashMap<>());
        }
        
        Map<Object, Object> map = new HashMap<>();
        Iterator<String> fieldNames = obj.fieldNames();
        
        while (fieldNames.hasNext()) {
            String key = fieldNames.next();
            JsonNode value = obj.get(key);
            
            Object convertedKey = convertJsonValue(
                obj.textNode(key), mapType.keyType());
            Object convertedValue = convertJsonValue(value, mapType.valueType());
            
            map.put(convertedKey, convertedValue);
        }
        
        return new GenericMapData(map);
    }
    
    /**
     * Convert JSON object to Flink RowData (for nested structs).
     */
    private RowData convertStructValue(ObjectNode obj, Types.StructType structType) {
        if (obj == null) {
            return null;
        }
        
        List<Types.NestedField> fields = structType.fields();
        GenericRowData rowData = new GenericRowData(fields.size());
        
        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            JsonNode value = obj.get(field.name());
            rowData.setField(i, convertJsonValue(value, field.type()));
        }
        
        return rowData;
    }
    
    /**
     * Build partition spec based on available fields.
     */
    private PartitionSpec buildPartitionSpec(Schema schema) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        
        for (String candidate : partitionCandidates) {
            Types.NestedField field = schema.findField(candidate);
            if (field != null) {
                Type fieldType = field.type();
                
                if (fieldType instanceof Types.DateType) {
                    // Use identity partitioning for date fields
                    builder.identity(candidate);
                } else if (fieldType instanceof Types.TimestampType) {
                    // Use day partitioning for timestamp fields
                    builder.day(candidate);
                } else if (fieldType instanceof Types.StringType) {
                    // Use identity partitioning for string fields (like region)
                    builder.identity(candidate);
                }
            }
        }
        
        return builder.build();
    }
    
    /**
     * Get field names from schema for logging.
     */
    private List<String> getFieldNames(Schema schema) {
        List<String> names = new ArrayList<>();
        for (Types.NestedField field : schema.columns()) {
            names.add(field.name() + ":" + field.type());
        }
        return names;
    }
}