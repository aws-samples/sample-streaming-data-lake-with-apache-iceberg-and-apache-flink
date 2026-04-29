package com.aws.samples.iceberg.generator;

import com.aws.samples.iceberg.model.BaseEvent;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Standalone data generator application that produces test events to a Kinesis stream.
 * 
 * Supports:
 * - Configurable event rate and duration
 * - Continuous and batch modes
 * - Multiple event types with configurable distribution
 * - Duplicate keys for upsert testing
 * - Late-arriving events for watermark testing
 * - Schema evolution scenarios (v1 vs v2)
 * 
 * Usage:
 *   java -jar data-generator.jar <stream-name> <region> <events-per-second> [duration-seconds] [schema-version]
 * 
 * Examples:
 *   # V1 schema (no optional fields)
 *   java -jar data-generator.jar iceberg-events us-east-1 100 60 v1
 * 
 *   # V2 schema (with optional fields)
 *   java -jar data-generator.jar iceberg-events us-east-1 100 60 v2
 */
public class DataGenerator {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataGenerator.class);
    private static final int BATCH_SIZE = 500; // Max records per PutRecords call
    
    private final String streamName;
    private final Region region;
    private final int eventsPerSecond;
    private final int durationSeconds;
    private final KinesisAsyncClient kinesisClient;
    private final EventFactory eventFactory;
    private final ObjectMapper objectMapper;
    private final AtomicLong totalEventsSent;
    private final AtomicLong totalBytesSent;
    
    public DataGenerator(String streamName, String region, int eventsPerSecond, int durationSeconds) {
        this(streamName, region, eventsPerSecond, durationSeconds, new HashMap<>(), 0.1, 0.05, 0.3);
    }
    
    public DataGenerator(String streamName, String region, int eventsPerSecond, int durationSeconds,
                        Map<String, Double> eventTypeDistribution,
                        double duplicateKeyProbability,
                        double lateArrivalProbability,
                        double schemaEvolutionProbability) {
        this.streamName = streamName;
        this.region = Region.of(region);
        this.eventsPerSecond = eventsPerSecond;
        this.durationSeconds = durationSeconds;
        
        // Initialize Kinesis client
        this.kinesisClient = KinesisAsyncClient.builder()
            .region(this.region)
            .build();
        
        // Initialize event factory
        this.eventFactory = new EventFactory(
            eventTypeDistribution,
            duplicateKeyProbability,
            lateArrivalProbability,
            schemaEvolutionProbability
        );
        
        // Initialize JSON mapper
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        
        // Initialize metrics
        this.totalEventsSent = new AtomicLong(0);
        this.totalBytesSent = new AtomicLong(0);
    }
    
    /**
     * Runs the data generator.
     */
    public void run() {
        LOG.info("Starting data generator");
        LOG.info("  Stream: {}", streamName);
        LOG.info("  Region: {}", region);
        LOG.info("  Rate: {} events/second", eventsPerSecond);
        LOG.info("  Mode: {}", durationSeconds < 0 ? "continuous" : "batch (" + durationSeconds + "s)");
        LOG.info("  Event distribution: {}", eventFactory.getEventTypeDistribution());
        LOG.info("  Duplicate key probability: {}", eventFactory.getDuplicateKeyProbability());
        LOG.info("  Late arrival probability: {}", eventFactory.getLateArrivalProbability());
        LOG.info("  Schema evolution probability: {}", eventFactory.getSchemaEvolutionProbability());
        
        long startTime = System.currentTimeMillis();
        long lastLogTime = startTime;
        long lastEventCount = 0;
        
        try {
            while (shouldContinue(startTime)) {
                long batchStartTime = System.currentTimeMillis();
                
                // Generate and send batch of events
                List<PutRecordsRequestEntry> records = generateBatch();
                sendBatch(records);
                
                // Log progress every 5 seconds
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastLogTime >= 5000) {
                    long currentCount = totalEventsSent.get();
                    long eventsSinceLastLog = currentCount - lastEventCount;
                    double throughput = eventsSinceLastLog / ((currentTime - lastLogTime) / 1000.0);
                    double avgThroughput = currentCount / ((currentTime - startTime) / 1000.0);
                    
                    LOG.info("Progress: {} events sent, {} MB, current: {:.1f} events/s, avg: {:.1f} events/s",
                        currentCount,
                        totalBytesSent.get() / (1024 * 1024),
                        throughput,
                        avgThroughput);
                    
                    lastLogTime = currentTime;
                    lastEventCount = currentCount;
                }
                
                // Sleep to maintain target rate
                long batchDuration = System.currentTimeMillis() - batchStartTime;
                long targetDuration = 1000; // 1 second per batch
                if (batchDuration < targetDuration) {
                    Thread.sleep(targetDuration - batchDuration);
                }
            }
            
            // Final summary
            long totalTime = System.currentTimeMillis() - startTime;
            double avgThroughput = totalEventsSent.get() / (totalTime / 1000.0);
            
            LOG.info("Data generator completed");
            LOG.info("  Total events: {}", totalEventsSent.get());
            LOG.info("  Total data: {} MB", totalBytesSent.get() / (1024 * 1024));
            LOG.info("  Duration: {} seconds", totalTime / 1000);
            LOG.info("  Average throughput: {:.1f} events/s", avgThroughput);
            
        } catch (InterruptedException e) {
            LOG.warn("Data generator interrupted", e);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOG.error("Error in data generator", e);
            throw new RuntimeException("Data generator failed", e);
        } finally {
            kinesisClient.close();
        }
    }
    
    /**
     * Checks if the generator should continue running.
     */
    private boolean shouldContinue(long startTime) {
        if (durationSeconds < 0) {
            return true; // Continuous mode
        }
        long elapsed = (System.currentTimeMillis() - startTime) / 1000;
        return elapsed < durationSeconds;
    }
    
    /**
     * Generates a batch of events.
     */
    private List<PutRecordsRequestEntry> generateBatch() {
        List<PutRecordsRequestEntry> records = new ArrayList<>();
        
        for (int i = 0; i < eventsPerSecond && records.size() < BATCH_SIZE; i++) {
            try {
                BaseEvent event = eventFactory.createRandomEvent();
                String json = objectMapper.writeValueAsString(event);
                
                PutRecordsRequestEntry record = PutRecordsRequestEntry.builder()
                    .partitionKey(event.getEventId())
                    .data(SdkBytes.fromUtf8String(json))
                    .build();
                
                records.add(record);
                
            } catch (Exception e) {
                LOG.error("Error generating event", e);
            }
        }
        
        return records;
    }
    
    /**
     * Sends a batch of records to Kinesis.
     */
    private void sendBatch(List<PutRecordsRequestEntry> records) {
        if (records.isEmpty()) {
            return;
        }
        
        try {
            PutRecordsRequest request = PutRecordsRequest.builder()
                .streamName(streamName)
                .records(records)
                .build();
            
            CompletableFuture<PutRecordsResponse> future = kinesisClient.putRecords(request);
            PutRecordsResponse response = future.join();
            
            // Update metrics
            int successCount = records.size() - response.failedRecordCount();
            totalEventsSent.addAndGet(successCount);
            
            long batchBytes = records.stream()
                .mapToLong(r -> r.data().asByteArray().length)
                .sum();
            totalBytesSent.addAndGet(batchBytes);
            
            // Log failures
            if (response.failedRecordCount() > 0) {
                LOG.warn("Failed to send {} records", response.failedRecordCount());
            }
            
        } catch (Exception e) {
            LOG.error("Error sending batch to Kinesis", e);
        }
    }
    
    /**
     * Main entry point.
     */
    public static void main(String[] args) {
        // Route to Avro generator if first arg is 'avro'
        if (args.length > 0 && "avro".equalsIgnoreCase(args[0])) {
            String[] rest = new String[args.length - 1];
            System.arraycopy(args, 1, rest, 0, rest.length);
            AvroDataGenerator.main(rest);
            return;
        }

        if (args.length < 3) {
            System.err.println("Usage: DataGenerator <stream-name> <region> <events-per-second> [duration-seconds] [schema-version]");
            System.err.println("   OR: DataGenerator avro <stream-name> <region> <registry-name> [events-per-second] [duration-seconds]");
            System.err.println();
            System.err.println("Arguments:");
            System.err.println("  stream-name        : Name of the Kinesis stream");
            System.err.println("  region             : AWS region (e.g., us-east-1)");
            System.err.println("  events-per-second  : Target event generation rate");
            System.err.println("  duration-seconds   : Duration in seconds (optional, -1 for continuous)");
            System.err.println("  schema-version     : v1 or v2 (optional, default: v1)");
            System.err.println("                       v1 = no optional fields (userAgent, scrollDepth)");
            System.err.println("                       v2 = include optional fields");
            System.err.println();
            System.err.println("Examples:");
            System.err.println("  # V1 schema - no optional fields");
            System.err.println("  java -jar data-generator.jar iceberg-events us-east-1 100 60 v1");
            System.err.println();
            System.err.println("  # V2 schema - with optional fields");
            System.err.println("  java -jar data-generator.jar iceberg-events us-east-1 100 60 v2");
            System.exit(1);
        }
        
        String streamName = args[0];
        String region = args[1];
        int eventsPerSecond = Integer.parseInt(args[2]);
        int durationSeconds = args.length > 3 ? Integer.parseInt(args[3]) : -1;
        String schemaVersion = args.length > 4 ? args[4].toLowerCase() : "v1";
        
        // Validate arguments
        if (eventsPerSecond <= 0) {
            System.err.println("Error: events-per-second must be positive");
            System.exit(1);
        }
        
        // Determine schema evolution probability based on version
        double schemaEvolutionProbability;
        if ("v2".equals(schemaVersion)) {
            schemaEvolutionProbability = 1.0;  // Always include optional fields
            System.out.println("Using V2 schema: ALL events will include optional fields (userAgent, scrollDepth)");
        } else {
            schemaEvolutionProbability = 0.0;  // Never include optional fields
            System.out.println("Using V1 schema: NO events will include optional fields");
        }
        
        DataGenerator generator = new DataGenerator(
            streamName, region, eventsPerSecond, durationSeconds,
            new HashMap<>(),  // default event type distribution
            0.1,              // 10% duplicate keys
            0.05,             // 5% late arrivals
            schemaEvolutionProbability
        );
        generator.run();
    }
}
