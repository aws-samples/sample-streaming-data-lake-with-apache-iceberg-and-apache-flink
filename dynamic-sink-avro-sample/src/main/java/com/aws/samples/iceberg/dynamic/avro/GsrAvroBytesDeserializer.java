package com.aws.samples.iceberg.dynamic.avro;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pass-through DeserializationSchema that forwards the GSR-wrapped Avro bytes as-is
 * to downstream operators. The schema UUID is embedded in the wire format bytes;
 * the downstream generator decodes and resolves the schema from GSR just-in-time
 * (with its own local cache).
 *
 * Keeping bytes small on the shuffle is important — we avoid re-emitting the full
 * schema JSON alongside every record.
 */
public class GsrAvroBytesDeserializer implements DeserializationSchema<byte[]> {

    private static final Logger LOG = LoggerFactory.getLogger(GsrAvroBytesDeserializer.class);
    private static final long serialVersionUID = 1L;

    @Override
    public void open(InitializationContext context) {
        // no-op
    }

    @Override
    public byte[] deserialize(byte[] message) {
        if (message == null || message.length == 0) {
            return null;
        }
        // Sanity check: verify the magic byte so we fail fast on non-GSR records
        if (message[0] != AWSSchemaRegistryConstants.HEADER_VERSION_BYTE) {
            LOG.warn("Record does not start with GSR header version byte (0x{}), got 0x{}",
                    String.format("%02X", AWSSchemaRegistryConstants.HEADER_VERSION_BYTE),
                    String.format("%02X", message[0]));
        }
        return message;
    }

    @Override
    public boolean isEndOfStream(byte[] nextElement) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}
