package com.aws.samples.iceberg.generator;

import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.aws.samples.iceberg.model.BaseEvent;
import com.aws.samples.iceberg.model.ClickEvent;
import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.model.UserEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Data generator that publishes Avro-encoded events to Kinesis using AWS Glue
 * Schema Registry. Each event type has its own Avro schema and maps to a unique
 * GSR schema name. Consumers can route records by schema name without inspecting
 * payloads.
 *
 * Usage:
 *   java -jar data-generator.jar avro <stream-name> <region> <registry-name> [rate] [duration-seconds]
 */
public class AvroDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(AvroDataGenerator.class);
    private static final int BATCH_SIZE = 500;

    private final String streamName;
    private final Region region;
    private final String registryName;
    private final int eventsPerSecond;
    private final int durationSeconds;
    private final KinesisAsyncClient kinesisClient;
    private final EventFactory eventFactory;
    private final GlueSchemaRegistrySerializationFacade gsrFacade;
    private final Map<String, UUID> schemaVersionIds = new HashMap<>();
    private final Schema orderSchema;
    private final Schema userSchema;
    private final Schema clickSchema;
    private final AtomicLong totalEventsSent = new AtomicLong(0);
    private final AtomicLong totalBytesSent = new AtomicLong(0);

    public AvroDataGenerator(String streamName, String region, String registryName,
                             int eventsPerSecond, int durationSeconds) {
        this.streamName = streamName;
        this.region = Region.of(region);
        this.registryName = registryName;
        this.eventsPerSecond = eventsPerSecond;
        this.durationSeconds = durationSeconds;

        this.kinesisClient = KinesisAsyncClient.builder().region(this.region).build();
        this.eventFactory = new EventFactory(new HashMap<>(), 0.1, 0.05, 0.3);

        this.orderSchema = loadSchema("/avro/OrderEvent.avsc");
        this.userSchema = loadSchema("/avro/UserEvent.avsc");
        this.clickSchema = loadSchema("/avro/ClickEvent.avsc");

        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, region);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "BACKWARD");
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registryName);

        this.gsrFacade = GlueSchemaRegistrySerializationFacade.builder()
                .credentialProvider(DefaultCredentialsProvider.builder().build())
                .configs(configs)
                .build();
    }

    private Schema loadSchema(String resourcePath) {
        try (java.io.InputStream in = AvroDataGenerator.class.getResourceAsStream(resourcePath)) {
            if (in == null) {
                throw new IllegalStateException("Avro schema not found on classpath: " + resourcePath);
            }
            return new Schema.Parser().parse(in);
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed to load schema: " + resourcePath, e);
        }
    }

    public void run() {
        LOG.info("Starting Avro data generator");
        LOG.info("  Stream:   {}", streamName);
        LOG.info("  Region:   {}", region);
        LOG.info("  Registry: {}", registryName);
        LOG.info("  Rate:     {} events/s", eventsPerSecond);
        LOG.info("  Duration: {} (-1 = continuous)", durationSeconds);

        long startTime = System.currentTimeMillis();
        long lastLog = startTime;
        long lastCount = 0;

        try {
            while (shouldContinue(startTime)) {
                long batchStart = System.currentTimeMillis();
                List<PutRecordsRequestEntry> batch = generateBatch();
                sendBatch(batch);

                long now = System.currentTimeMillis();
                if (now - lastLog >= 5000) {
                    long cur = totalEventsSent.get();
                    double rate = (cur - lastCount) / ((now - lastLog) / 1000.0);
                    LOG.info("Sent {} events so far ({} MB), recent rate: {}/s",
                            cur, totalBytesSent.get() / (1024 * 1024), String.format("%.1f", rate));
                    lastLog = now;
                    lastCount = cur;
                }

                long elapsed = System.currentTimeMillis() - batchStart;
                if (elapsed < 1000) Thread.sleep(1000 - elapsed);
            }
            LOG.info("Completed. Total events: {}", totalEventsSent.get());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            kinesisClient.close();
        }
    }

    private boolean shouldContinue(long startTime) {
        if (durationSeconds < 0) return true;
        return (System.currentTimeMillis() - startTime) / 1000 < durationSeconds;
    }

    private List<PutRecordsRequestEntry> generateBatch() {
        List<PutRecordsRequestEntry> records = new ArrayList<>();
        for (int i = 0; i < eventsPerSecond && records.size() < BATCH_SIZE; i++) {
            try {
                BaseEvent event = eventFactory.createRandomEvent();
                SchemaNamedRecord named = toAvro(event);
                byte[] payload = encodeWithGsr(named.schemaName, named.avroSchema, named.record);

                records.add(PutRecordsRequestEntry.builder()
                        .partitionKey(event.getEventId())
                        .data(SdkBytes.fromByteArray(payload))
                        .build());
            } catch (Exception e) {
                LOG.error("Error generating event", e);
            }
        }
        return records;
    }

    private SchemaNamedRecord toAvro(BaseEvent event) {
        if (event instanceof OrderEvent) {
            OrderEvent o = (OrderEvent) event;
            GenericRecord r = new GenericData.Record(orderSchema);
            r.put("event_id", o.getEventId());
            r.put("event_time", o.getEventTime().toEpochMilli() * 1000L);
            r.put("event_type", o.getEventType());
            r.put("region", o.getRegion());
            r.put("event_date", (int) o.getEventDate().toEpochDay());
            r.put("order_id", o.getOrderId());
            r.put("customer_id", o.getCustomerId());
            r.put("amount", o.getAmount().doubleValue());
            r.put("currency", o.getCurrency());
            r.put("status", o.getStatus());
            return new SchemaNamedRecord("OrderEvent", orderSchema, r);
        } else if (event instanceof UserEvent) {
            UserEvent u = (UserEvent) event;
            GenericRecord r = new GenericData.Record(userSchema);
            r.put("event_id", u.getEventId());
            r.put("event_time", u.getEventTime().toEpochMilli() * 1000L);
            r.put("event_type", u.getEventType());
            r.put("region", u.getRegion());
            r.put("event_date", (int) u.getEventDate().toEpochDay());
            r.put("user_id", u.getUserId());
            r.put("action", u.getAction());
            r.put("device_type", u.getDeviceType());
            r.put("ip_address", u.getIpAddress());
            if (u.getUserAgent() != null) r.put("user_agent", u.getUserAgent());
            return new SchemaNamedRecord("UserEvent", userSchema, r);
        } else if (event instanceof ClickEvent) {
            ClickEvent c = (ClickEvent) event;
            GenericRecord r = new GenericData.Record(clickSchema);
            r.put("event_id", c.getEventId());
            r.put("event_time", c.getEventTime().toEpochMilli() * 1000L);
            r.put("event_type", c.getEventType());
            r.put("region", c.getRegion());
            r.put("event_date", (int) c.getEventDate().toEpochDay());
            r.put("session_id", c.getSessionId());
            r.put("page_url", c.getPageUrl());
            r.put("referrer", c.getReferrer());
            if (c.getScrollDepth() != null) r.put("scroll_depth", c.getScrollDepth());
            if (c.getTimeOnPageSeconds() != null) r.put("time_on_page_seconds", c.getTimeOnPageSeconds());
            return new SchemaNamedRecord("ClickEvent", clickSchema, r);
        }
        throw new IllegalArgumentException("Unknown event type: " + event.getClass());
    }

    private byte[] encodeWithGsr(String schemaName, Schema avroSchema, GenericRecord record) {
        // Register (or look up) schema version; facade caches after first call per unique schema
        UUID schemaVersionId = schemaVersionIds.computeIfAbsent(schemaName, name -> {
            AWSSerializerInput input = AWSSerializerInput.builder()
                    .schemaDefinition(avroSchema.toString())
                    .schemaName(name)
                    .dataFormat(DataFormat.AVRO.name())
                    .transportName(streamName)
                    .build();
            UUID id = gsrFacade.getOrRegisterSchemaVersion(input);
            LOG.info("Schema {} registered/resolved: {}", name, id);
            return id;
        });

        // serialize() takes the object, serializes per dataFormat + wraps with GSR header
        return gsrFacade.serialize(DataFormat.AVRO, record, schemaVersionId);
    }

    private void sendBatch(List<PutRecordsRequestEntry> records) {
        if (records.isEmpty()) return;
        try {
            PutRecordsRequest req = PutRecordsRequest.builder()
                    .streamName(streamName).records(records).build();
            CompletableFuture<PutRecordsResponse> future = kinesisClient.putRecords(req);
            PutRecordsResponse resp = future.join();
            int ok = records.size() - resp.failedRecordCount();
            totalEventsSent.addAndGet(ok);
            long bytes = records.stream().mapToLong(r -> r.data().asByteArray().length).sum();
            totalBytesSent.addAndGet(bytes);
            if (resp.failedRecordCount() > 0) LOG.warn("Failed: {}", resp.failedRecordCount());
        } catch (Exception e) {
            LOG.error("Error sending batch", e);
        }
    }

    private static class SchemaNamedRecord {
        final String schemaName;
        final Schema avroSchema;
        final GenericRecord record;
        SchemaNamedRecord(String n, Schema s, GenericRecord r) {
            this.schemaName = n; this.avroSchema = s; this.record = r;
        }
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: AvroDataGenerator <stream-name> <region> <registry-name> [rate] [duration]");
            System.exit(1);
        }
        String stream = args[0];
        String region = args[1];
        String registry = args[2];
        int rate = args.length > 3 ? Integer.parseInt(args[3]) : 100;
        int duration = args.length > 4 ? Integer.parseInt(args[4]) : -1;
        new AvroDataGenerator(stream, region, registry, rate, duration).run();
    }
}
