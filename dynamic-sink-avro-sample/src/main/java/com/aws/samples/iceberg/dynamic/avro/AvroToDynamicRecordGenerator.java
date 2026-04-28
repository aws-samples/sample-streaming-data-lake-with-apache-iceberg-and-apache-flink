package com.aws.samples.iceberg.dynamic.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts an Avro {@link GenericRecord} (with its schema name from GSR) into Iceberg's
 * {@link DynamicRecord}.
 *
 * Routing policy: the schema name becomes the Iceberg table name (lowercased, with
 * hyphens replaced by underscores for Iceberg identifier compatibility). The database
 * and partitioning candidates come from configuration.
 *
 * The Avro writer schema is converted to an Iceberg schema once per schema name via
 * {@link AvroSchemaUtil#toIceberg} and cached. This avoids re-parsing schemas on every
 * record and lets the downstream DynamicIcebergSink reuse its own schema cache.
 */
public class AvroToDynamicRecordGenerator
        implements DynamicRecordGenerator<Tuple2<String, GenericRecord>> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToDynamicRecordGenerator.class);
    private static final long serialVersionUID = 1L;

    private final String database;
    private final List<String> partitionCandidates;
    private final String branch;

    // Caches keyed by schema full name (schema identity is stable within a version)
    private transient Map<String, Schema> icebergSchemaCache;
    private transient Map<String, TableIdentifier> tableIdCache;

    public AvroToDynamicRecordGenerator(String database,
                                        List<String> partitionCandidates,
                                        String branch) {
        this.database = database;
        this.partitionCandidates = partitionCandidates;
        this.branch = branch;
    }

    @Override
    public void generate(Tuple2<String, GenericRecord> input,
                         Collector<DynamicRecord> out) {
        if (icebergSchemaCache == null) {
            icebergSchemaCache = new HashMap<>();
            tableIdCache = new HashMap<>();
        }

        String schemaName = input.f0;
        GenericRecord record = input.f1;
        org.apache.avro.Schema avroSchema = record.getSchema();
        String cacheKey = avroSchema.getFullName() + "#" + avroSchema.hashCode();

        Schema icebergSchema = icebergSchemaCache.computeIfAbsent(cacheKey,
                k -> convertAvroSchema(avroSchema));
        TableIdentifier tableId = tableIdCache.computeIfAbsent(schemaName,
                k -> TableIdentifier.of(database, normalizeTableName(schemaName)));

        RowData rowData = convertToRowData(record, icebergSchema);

        PartitionSpec spec = buildPartitionSpec(icebergSchema);

        DynamicRecord dyn = new DynamicRecord(
                tableId,
                branch,
                icebergSchema,
                rowData,
                spec,
                DistributionMode.HASH,
                2);
        out.collect(dyn);
    }

    private Schema convertAvroSchema(org.apache.avro.Schema avroSchema) {
        // Iceberg's AvroSchemaUtil uses a shaded Avro; round-trip the schema through
        // JSON so we work with whichever Avro version Iceberg expects internally.
        String json = avroSchema.toString();
        org.apache.iceberg.shaded.org.apache.avro.Schema shadedSchema =
                new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(json);
        Schema schema = AvroSchemaUtil.toIceberg(shadedSchema);
        LOG.info("Converted Avro schema '{}' to Iceberg schema with {} fields",
                avroSchema.getFullName(), schema.columns().size());
        return schema;
    }

    private String normalizeTableName(String schemaName) {
        return schemaName.toLowerCase().replace('-', '_').replace('.', '_');
    }

    private PartitionSpec buildPartitionSpec(Schema schema) {
        PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
        boolean hasPartition = false;
        for (String candidate : partitionCandidates) {
            Types.NestedField field = schema.findField(candidate.trim());
            if (field == null) continue;
            Type.TypeID typeId = field.type().typeId();
            try {
                if (typeId == Type.TypeID.DATE) {
                    builder.identity(field.name());
                    hasPartition = true;
                } else if (typeId == Type.TypeID.TIMESTAMP) {
                    builder.day(field.name());
                    hasPartition = true;
                } else if (typeId == Type.TypeID.STRING) {
                    builder.identity(field.name());
                    hasPartition = true;
                }
            } catch (Exception e) {
                LOG.warn("Could not partition by {}: {}", field.name(), e.getMessage());
            }
        }
        return hasPartition ? builder.build() : PartitionSpec.unpartitioned();
    }

    private RowData convertToRowData(GenericRecord record, Schema icebergSchema) {
        List<Types.NestedField> fields = icebergSchema.columns();
        GenericRowData row = new GenericRowData(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Types.NestedField field = fields.get(i);
            Object value = record.get(field.name());
            row.setField(i, convertValue(value, field.type()));
        }
        return row;
    }

    private Object convertValue(Object value, Type type) {
        if (value == null) return null;
        switch (type.typeId()) {
            case STRING:
                return StringData.fromString(value.toString());
            case INTEGER:
                return ((Number) value).intValue();
            case LONG:
                return ((Number) value).longValue();
            case FLOAT:
                return ((Number) value).floatValue();
            case DOUBLE:
                return ((Number) value).doubleValue();
            case BOOLEAN:
                return value;
            case DATE:
                if (value instanceof Integer) return (Integer) value;
                if (value instanceof LocalDate) return (int) ((LocalDate) value).toEpochDay();
                return Integer.parseInt(value.toString());
            case TIMESTAMP:
                if (value instanceof Long) {
                    long micros = (Long) value;
                    return TimestampData.fromEpochMillis(micros / 1000L, (int) (micros % 1000) * 1000);
                }
                if (value instanceof Instant) return TimestampData.fromInstant((Instant) value);
                return TimestampData.fromEpochMillis(Long.parseLong(value.toString()));
            case DECIMAL:
                Types.DecimalType decType = (Types.DecimalType) type;
                BigDecimal bd;
                if (value instanceof ByteBuffer) {
                    byte[] bytes = new byte[((ByteBuffer) value).remaining()];
                    ((ByteBuffer) value).duplicate().get(bytes);
                    bd = new BigDecimal(new java.math.BigInteger(bytes), decType.scale());
                } else if (value instanceof BigDecimal) {
                    bd = (BigDecimal) value;
                } else {
                    bd = new BigDecimal(value.toString());
                }
                return DecimalData.fromBigDecimal(bd, decType.precision(), decType.scale());
            case BINARY:
            case FIXED:
                if (value instanceof ByteBuffer) {
                    ByteBuffer bb = ((ByteBuffer) value).duplicate();
                    byte[] bytes = new byte[bb.remaining()];
                    bb.get(bytes);
                    return bytes;
                }
                return value;
            case STRUCT: {
                GenericRecord nested = (GenericRecord) value;
                Types.StructType st = type.asStructType();
                GenericRowData nestedRow = new GenericRowData(st.fields().size());
                for (int i = 0; i < st.fields().size(); i++) {
                    Types.NestedField f = st.fields().get(i);
                    nestedRow.setField(i, convertValue(nested.get(f.name()), f.type()));
                }
                return nestedRow;
            }
            default:
                return StringData.fromString(value.toString());
        }
    }
}
