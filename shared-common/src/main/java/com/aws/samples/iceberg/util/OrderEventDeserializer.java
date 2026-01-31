package com.aws.samples.iceberg.util;

import com.aws.samples.iceberg.model.OrderEvent;
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
 * Kinesis deserializer specifically for OrderEvent.
 * Filters out non-ORDER events from the stream silently.
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
            
            // First parse as JsonNode to check event type
            JsonNode node = objectMapper.readTree(json);
            String eventType = node.has("event_type") ? node.get("event_type").asText() : null;
            
            // Filter: only process ORDER events
            if (!"ORDER".equals(eventType)) {
                // Silently skip non-ORDER events - this is expected behavior
                return;
            }
            
            // Now deserialize as OrderEvent
            OrderEvent event = objectMapper.treeToValue(node, OrderEvent.class);
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
