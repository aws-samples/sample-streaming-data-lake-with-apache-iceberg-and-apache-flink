package com.aws.samples.iceberg.util;

import com.aws.samples.iceberg.model.ClickEvent;
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
 * Kinesis deserializer specifically for ClickEvent.
 * Use this when the stream contains only click events.
 */
public class ClickEventDeserializer implements KinesisDeserializationSchema<ClickEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(ClickEventDeserializer.class);
    private static final long serialVersionUID = 1L;
    
    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<ClickEvent> out) throws IOException {
        try {
            byte[] data = record.data().asByteArray();
            String json = new String(data, StandardCharsets.UTF_8);
            
            ClickEvent event = objectMapper.readValue(json, ClickEvent.class);
            out.collect(event);
            
        } catch (Exception e) {
            LOG.error("Failed to deserialize ClickEvent from stream {} shard {}: {}", 
                     stream, shardId, record.sequenceNumber(), e);
            // Skip malformed records
        }
    }
    
    @Override
    public TypeInformation<ClickEvent> getProducedType() {
        return TypeInformation.of(ClickEvent.class);
    }
}
