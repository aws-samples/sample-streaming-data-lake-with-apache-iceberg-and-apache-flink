package com.aws.samples.iceberg.dynamic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kinesis.model.Record;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Schema-agnostic JSON deserializer for Kinesis.
 * 
 * Deserializes raw JSON bytes to Jackson JsonNode without requiring
 * any knowledge of the schema. This enables truly dynamic processing
 * where the schema is inferred at runtime.
 */
public class JsonNodeDeserializer implements KinesisDeserializationSchema<JsonNode> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(JsonNodeDeserializer.class);
    
    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<JsonNode> out) throws IOException {
        try {
            byte[] data = record.data().asByteArray();
            String json = new String(data, StandardCharsets.UTF_8);
            
            JsonNode jsonNode = objectMapper.readTree(json);
            
            if (jsonNode != null && !jsonNode.isNull()) {
                out.collect(jsonNode);
            } else {
                LOG.warn("Received null or empty JSON from stream {} shard {} at sequence {}", 
                    stream, shardId, record.sequenceNumber());
            }
        } catch (Exception e) {
            LOG.error("Failed to deserialize JSON from stream {} shard {} at sequence {}: {}", 
                stream, shardId, record.sequenceNumber(), e.getMessage());
            // Skip malformed records to avoid blocking the stream
        }
    }
    
    @Override
    public TypeInformation<JsonNode> getProducedType() {
        return TypeInformation.of(JsonNode.class);
    }
}
