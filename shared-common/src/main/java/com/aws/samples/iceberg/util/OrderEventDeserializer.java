package com.aws.samples.iceberg.util;

import com.aws.samples.iceberg.model.OrderEvent;
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
 * Kinesis deserializer specifically for OrderEvent.
 * Use this when the stream contains only order events.
 */
public class OrderEventDeserializer implements KinesisDeserializationSchema<OrderEvent> {
    
    private static final Logger LOG = LoggerFactory.getLogger(OrderEventDeserializer.class);
    private static final long serialVersionUID = 1L;
    
    private transient ObjectMapper objectMapper;
    
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }
    
    @Override
    public void deserialize(Record record, String stream, String shardId, Collector<OrderEvent> out) throws IOException {
        try {
            byte[] data = record.data().asByteArray();
            String json = new String(data, StandardCharsets.UTF_8);
            
            OrderEvent event = objectMapper.readValue(json, OrderEvent.class);
            out.collect(event);
            
        } catch (Exception e) {
            LOG.error("Failed to deserialize OrderEvent from stream {} shard {}: {}", 
                     stream, shardId, record.sequenceNumber(), e);
            // Skip malformed records
        }
    }
    
    @Override
    public TypeInformation<OrderEvent> getProducedType() {
        return TypeInformation.of(OrderEvent.class);
    }
}
