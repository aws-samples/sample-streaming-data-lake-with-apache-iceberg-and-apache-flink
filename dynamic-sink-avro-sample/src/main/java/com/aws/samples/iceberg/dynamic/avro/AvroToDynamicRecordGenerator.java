package com.aws.samples.iceberg.dynamic.avro;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
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
 * Receives raw GSR-wrapped Avro bytes and emits Iceberg DynamicRecords.
 *
 * Responsibilities:
 *  - Resolve the writer schema from GSR (facade caches by UUID).
 *  - Decode the Avro payload into a GenericRecord.
 *  - Convert the Avro schema to an Iceberg schema (cached by Avro schema identity).
 *  - Build a RowData and route to an Iceberg table named after the GSR schema.
 *
 * Keeping the GSR facade + schema caches in this operator (rather than transmitting
 * decoded records between operators) avoids the need to serialize GenericRecord with
 * Kryo, which is slow and sometimes impossible due to unmodifiable collections in
 * Avro's Schema model.
 */
public class AvroToDynamicRecordGenerator
        implements DynamicRecordGenerator<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(AvroToDynamicRecordGenerator.class);
    private static final long serialVersionUID = 1L;

    private final String awsRegion;
    private final String registryName;
    private final String database;
    private final List<String> partitionCandidates;
    private final String branch;

    // Lazy-initialized per task
    private transient GlueSchemaRegistryDeserializationFacade facade;
    private transient Map<String, org.apache.avro.Schema> writerSchemaCache;
    private transient Map<org.apache.avro.Schema, Schema> icebergSchemaCache;
    private transient Map<String, TableIdentifier> tableIdCache;

    public AvroToDynamicRecordGenerator(String awsRegion,
                                        String registryName,
                                        String database,
                                        List<String> partitionCandidates,
                                        String branch) {
        this.awsRegion = awsRegion;
        this.registryName = registryName;
        this.database = database;
        this.partitionCandidates = partitionCandidates;
        this.branch = branch;
    }

    private void ensureInitialized() {
        if (facade != null) return;

        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, awsRegion);
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        if (registryName != null && !registryName.isEmpty()) {
            configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registryName);
        }
        facade = GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.create())
                .configs(configs)
                .build();
        writerSchemaCache = new HashMap<>();
        icebergSchemaCache = new HashMap<>();
        tableIdCache = new HashMap<>();
        LOG.info("Initialized GSR generator: region={}, registry={}, database={}",
                awsRegion, registryName, database);
    }

    @Override
    public void generate(byte[] bytes, Collector<DynamicRecord> out) throws Exception {
        if (bytes == null || bytes.length == 0) return;
        ensureInitialized();

        // Resolve schema (cached inside the facade and in our writerSchemaCache)
        com.amazonaws.services.schemaregistry.common.Schema gsrSchema = facade.getSchema(bytes);
        String schemaName = gsrSchema.getSchemaName();

        org.apache.avro.Schema writerSchema = writerSchemaCache.computeIfAbsent(
                gsrSchema.getSchemaDefinition(),
                def -> new org.apache.avro.Schema.Parser().parse(def));

        Schema icebergSchema = icebergSchemaCache.computeIfAbsent(
                writerSchema, this::convertAvroSchema);

        TableIdentifier tableId = tableIdCache.computeIfAbsent(
                schemaName, n -> TableIdentifier.of(database, normalizeTableName(n)));
        String writeBranch = (branch == null || branch.isEmpty()) ? "main" : branch;

        // Decode the Avro payload
        byte[] avroPayload = facade.getActualData(bytes);
        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroPayload, null);
        GenericRecord record = reader.read(null, decoder);

        RowData rowData = convertToRowData(record, icebergSchema);
        PartitionSpec spec = buildPartitionSpec(icebergSchema);

        out.collect(new DynamicRecord(
                tableId,
                writeBranch,
                icebergSchema,
                rowData,
                spec,
                DistributionMode.HASH,
                2));
    }

    private Schema convertAvroSchema(org.apache.avro.Schema avroSchema) {
        // Iceberg uses shaded Avro, so round-trip via JSON
        String json = avroSchema.toString();
        org.apache.iceberg.shaded.org.apache.avro.Schema shadedSchema =
                new org.apache.iceberg.shaded.org.apache.avro.Schema.Parser().parse(json);
        Schema schema = AvroSchemaUtil.toIceberg(shadedSchema);
        LOG.info("Converted Avro schema '{}' to Iceberg schema ({} fields)",
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
                if (typeId == Type.TypeID.DATE || typeId == Type.TypeID.STRING) {
                    builder.identity(field.name());
                    hasPartition = true;
                } else if (typeId == Type.TypeID.TIMESTAMP) {
                    builder.day(field.name());
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
