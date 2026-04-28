package com.aws.samples.iceberg.dynamic.avro;

import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.DataFormat;

import java.util.HashMap;
import java.util.Map;

/**
 * Flink DeserializationSchema that reads AVRO-encoded records produced against AWS
 * Glue Schema Registry, without requiring a reader schema upfront.
 *
 * Wire format (produced by the GSR serializer):
 *   [header byte][version byte][compression byte][16-byte schema version UUID][avro payload]
 *
 * This deserializer extracts the schema UUID from the payload, fetches the writer
 * schema from GSR (cached locally by the facade), and decodes the Avro payload using
 * that schema. It returns a tuple of (schemaName, GenericRecord) so downstream
 * operators can route records by schema name without parsing the payload again.
 */
public class GsrMultiSchemaDeserializer implements DeserializationSchema<Tuple2<String, GenericRecord>> {

    private static final Logger LOG = LoggerFactory.getLogger(GsrMultiSchemaDeserializer.class);
    private static final long serialVersionUID = 1L;

    private final String awsRegion;
    private final String registryName;

    // Not serializable — lazy-initialized per task manager
    private transient GlueSchemaRegistryDeserializationFacade facade;

    public GsrMultiSchemaDeserializer(String awsRegion, String registryName) {
        this.awsRegion = awsRegion;
        this.registryName = registryName;
    }

    @Override
    public void open(InitializationContext context) {
        ensureInitialized();
    }

    private void ensureInitialized() {
        if (facade != null) {
            return;
        }
        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, awsRegion);
        configs.put(AWSSchemaRegistryConstants.AVRO_RECORD_TYPE, AvroRecordType.GENERIC_RECORD.getName());
        if (registryName != null && !registryName.isEmpty()) {
            configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registryName);
        }
        this.facade = GlueSchemaRegistryDeserializationFacade.builder()
                .credentialProvider(software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider.create())
                .configs(configs)
                .build();
        LOG.info("GSR deserialization facade initialized: region={}, registry={}",
                awsRegion, registryName == null ? "default" : registryName);
    }

    @Override
    public Tuple2<String, GenericRecord> deserialize(byte[] message) throws java.io.IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        ensureInitialized();

        // Fetch the writer schema from GSR based on the schema ID embedded in the payload
        com.amazonaws.services.schemaregistry.common.Schema gsrSchema = facade.getSchema(message);
        if (gsrSchema == null || !DataFormat.AVRO.name().equals(gsrSchema.getDataFormat())) {
            throw new java.io.IOException("Expected AVRO data format but got: "
                    + (gsrSchema == null ? "null" : gsrSchema.getDataFormat()));
        }

        Schema writerSchema = new Schema.Parser().parse(gsrSchema.getSchemaDefinition());

        // Strip GSR header bytes to get the raw Avro payload
        byte[] avroPayload = facade.getActualData(message);

        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(writerSchema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(avroPayload, null);
        GenericRecord record = reader.read(null, decoder);

        String schemaName = gsrSchema.getSchemaName();
        return new Tuple2<>(schemaName, record);
    }

    @Override
    public boolean isEndOfStream(Tuple2<String, GenericRecord> nextElement) {
        return false;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeInformation<Tuple2<String, GenericRecord>> getProducedType() {
        return TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<Tuple2<String, GenericRecord>>() {});
    }
}
