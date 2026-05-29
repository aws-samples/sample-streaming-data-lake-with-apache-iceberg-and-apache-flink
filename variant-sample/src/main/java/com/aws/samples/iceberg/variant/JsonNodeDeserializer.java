package com.aws.samples.iceberg.variant;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.nio.charset.StandardCharsets;

/** Deserializes raw Kinesis JSON bytes into Jackson {@link JsonNode}; skips malformed records. */
public class JsonNodeDeserializer implements KinesisDeserializationSchema<JsonNode> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonNodeDeserializer.class);

    private transient ObjectMapper objectMapper;

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<JsonNode> out) {
        try {
            JsonNode node = objectMapper.readTree(new String(record.data().asByteArray(), StandardCharsets.UTF_8));
            if (node != null && !node.isNull()) {
                out.collect(node);
            }
        } catch (Exception e) {
            LOG.warn("Skipping malformed JSON record on shard {}: {}", shardId, e.toString());
        }
    }

    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
}
