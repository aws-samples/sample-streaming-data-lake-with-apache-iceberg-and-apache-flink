package com.aws.samples.iceberg.util;

import com.aws.samples.iceberg.model.BaseEvent;
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
 * Kinesis deserializer for BaseEvent and its subtypes.
 * Uses Jackson polymorphic deserialization based on eventType field.
 */
public class BaseEventDeserializer implements KinesisDeserializationSchema<BaseEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(BaseEventDeserializer.class);
    private static final long serialVersionUID = 1L;
    
    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<BaseEvent> out) throws IOException {
        try {
            byte[] data = record.data().asByteArray();
            String json = new String(data, StandardCharsets.UTF_8);
            
            BaseEvent event = objectMapper.readValue(json, BaseEvent.class);
            out.collect(event);
            
        } catch (Exception e) {
            LOG.error("Failed to deserialize Kinesis record from stream {} shard {}: {}", 
                     stream, shardId, record.sequenceNumber(), e);
            // Skip malformed records - could implement dead letter queue here
        }
    }
    
    @Override
    public TypeInformation<BaseEvent> getProducedType() {
        return TypeInformation.of(BaseEvent.class);
    }
}
