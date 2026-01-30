package com.aws.samples.iceberg.dynamic;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.aws.samples.iceberg.model.BaseEvent;
import com.aws.samples.iceberg.util.BaseEventDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Dynamic Iceberg Sink sample demonstrating automatic table routing and schema evolution.
 * 
 * This job reads mixed event types from Kinesis and dynamically routes them to different
 * Iceberg tables based on event_type field:
 * - ORDER events → orders table
 * - USER events → users table
 * - CLICK events → clicks table
 * 
 * Features demonstrated:
 * - Dynamic table routing based on record content
 * - Automatic schema evolution when new fields are added
 * - Auto table creation with inferred schema
 * - Table metadata caching for performance
 * 
 * Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6
 */
public class DynamicSinkJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(DynamicSinkJob.class);
    
    // Configuration keys
    private static final String KINESIS_STREAM_ARN = "kinesis.stream.arn";
    private static final String KINESIS_REGION = "kinesis.region";
    private static final String ICEBERG_CATALOG_NAME = "iceberg.catalog.name";
    private static final String ICEBERG_DATABASE = "iceberg.database";
    private static final String ICEBERG_WAREHOUSE = "iceberg.warehouse";
    private static final String AWS_REGION = "aws.region";
    private static final String CHECKPOINT_INTERVAL = "checkpoint.interval.ms";
    private static final String CACHE_MAX_SIZE = "cache.max.size";
    private static final String CACHE_REFRESH_MS = "cache.refresh.ms";
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";
    
    /**
     * Check if running in local execution mode.
     */
    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }
    
    /**
     * Create execution environment with Web UI for local development.
     */
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            if (isLocal(env)) {
                org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
                config.setString("rest.port", "8083");
                config.setString("rest.bind-address", "localhost");
                env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
                LOG.info("Local execution detected - Flink Web UI available at http://localhost:8083");
            }
            return env;
        } catch (Exception e) {
            LOG.warn("Could not create environment with Web UI, falling back to standard environment", e);
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }
    
    /**
     * Load application properties from runtime or local file.
     */
    private static Map<String, String> loadApplicationProperties(StreamExecutionEnvironment env) throws Exception {
        Map<String, String> config = new HashMap<>();
        
        if (isLocal(env)) {
            LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            Map<String, Properties> props = KinesisAnalyticsRuntime.getApplicationProperties(
                DynamicSinkJob.class.getClassLoader().getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath()
            );
            Properties flinkProps = props.getOrDefault("FlinkApplicationProperties", new Properties());
            
            flinkProps.forEach((key, value) -> config.put(key.toString(), value.toString()));
        } else {
            LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
            Map<String, Properties> props = KinesisAnalyticsRuntime.getApplicationProperties();
            Properties flinkProps = props.getOrDefault("FlinkApplicationProperties", new Properties());
            
            flinkProps.forEach((key, value) -> config.put(key.toString(), value.toString()));
        }
        
        return config;
    }
    
    public static void main(String[] args) throws Exception {
        LOG.info("Starting Dynamic Iceberg Sink Job");
        
        // Set up Flink execution environment
        StreamExecutionEnvironment env = createExecutionEnvironment();
        
        // Load configuration from runtime properties
        Map<String, String> config = loadApplicationProperties(env);
        
        // Configure checkpointing for local development only
        // AWS Managed Flink configures checkpointing automatically
        if (isLocal(env)) {
            configureCheckpointing(env, config);
        } else {
            LOG.info("Running on AWS Managed Flink - checkpointing configured by the service");
        }
        
        // Create Kinesis source for mixed event types
        KinesisStreamsSource<BaseEvent> kinesisSource = createKinesisSource(config);
        
        // Read from Kinesis with watermark strategy
        // Explicitly provide TypeInformation to avoid type erasure issues on Managed Flink
        DataStream<BaseEvent> events = env.fromSource(
            kinesisSource,
            createWatermarkStrategy(),
            "Kinesis Source",
            org.apache.flink.api.common.typeinfo.TypeInformation.of(BaseEvent.class)
        )
        .uid("kinesis-source-dynamic")
        .name("Read from Kinesis (Mixed Events)");
        
        // Create Iceberg catalog loader
        CatalogLoader catalogLoader = createCatalogLoader(config);
        
        // Configure Dynamic Iceberg Sink
        configureDynamicSink(events, catalogLoader, config);
        
        LOG.info("Dynamic Iceberg Sink Job configured successfully");
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received, cleaning up resources...");
        }));
        
        // Execute the job
        try {
            env.execute("Dynamic Iceberg Sink Job - Multi-Table Routing");
        } catch (Exception e) {
            LOG.error("Job execution failed", e);
            throw e;
        }
    }
    
    /**
     * Configure checkpointing for local development only.
     * AWS Managed Flink configures checkpointing automatically.
     */
    private static void configureCheckpointing(StreamExecutionEnvironment env, Map<String, String> config) {
        long checkpointInterval = Long.parseLong(
            config.getOrDefault(CHECKPOINT_INTERVAL, "60000")
        );
        
        LOG.info("Configuring checkpointing for local development");
        
        env.enableCheckpointing(checkpointInterval);
        
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000);
        checkpointConfig.setCheckpointTimeout(600000);  // 10 minutes
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        
        // Enable unaligned checkpoints for better performance under backpressure
        checkpointConfig.enableUnalignedCheckpoints(true);
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
        
        LOG.info("Checkpointing configured: interval={}ms, mode=EXACTLY_ONCE, unaligned=true", checkpointInterval);
    }
    
    /**
     * Create Kinesis source with BaseEvent deserializer for mixed event types.
     */
    private static KinesisStreamsSource<BaseEvent> createKinesisSource(Map<String, String> config) {
        String streamArn = config.get(KINESIS_STREAM_ARN);
        String region = config.get(KINESIS_REGION);
        
        if (streamArn == null || streamArn.isEmpty()) {
            throw new IllegalArgumentException("Kinesis stream ARN is required");
        }
        
        LOG.info("Creating Kinesis source for stream: {} in region: {}", streamArn, region);
        
        Configuration sourceConfig = new Configuration();
        sourceConfig.setString("aws.region", region);
        sourceConfig.setString("flink.stream.initpos", "LATEST");
        
        return KinesisStreamsSource.<BaseEvent>builder()
            .setStreamArn(streamArn)
            .setDeserializationSchema(new BaseEventDeserializer())
            .setSourceConfig(sourceConfig)
            .build();
    }
    
    /**
     * Create watermark strategy for handling out-of-order events.
     */
    private static WatermarkStrategy<BaseEvent> createWatermarkStrategy() {
        return WatermarkStrategy
            .<BaseEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
            .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
            .withIdleness(Duration.ofMinutes(5));
    }
    
    /**
     * Create Iceberg catalog loader with Glue Catalog configuration.
     */
    private static CatalogLoader createCatalogLoader(Map<String, String> config) {
        String catalogName = config.getOrDefault(ICEBERG_CATALOG_NAME, "glue_catalog");
        String warehouse = config.get(ICEBERG_WAREHOUSE);
        String awsRegion = config.get(AWS_REGION);
        
        if (warehouse == null || warehouse.isEmpty()) {
            throw new IllegalArgumentException("Iceberg warehouse path is required");
        }
        
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
        catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProperties.put("warehouse", warehouse);
        catalogProperties.put("client.region", awsRegion != null ? awsRegion : "us-east-1");
        
        LOG.info("Creating catalog loader: {} with warehouse: {}", catalogName, warehouse);
        
        return CatalogLoader.custom(
            catalogName,
            catalogProperties,
            new org.apache.hadoop.conf.Configuration(),
            "org.apache.iceberg.aws.glue.GlueCatalog"
        );
    }
    
    /**
     * Configure Dynamic Iceberg Sink with automatic routing and schema evolution.
     * Requirements: 4.1, 4.2, 4.4, 4.5, 4.6
     */
    private static void configureDynamicSink(
            DataStream<BaseEvent> events,
            CatalogLoader catalogLoader,
            Map<String, String> config) {
        
        String database = config.getOrDefault(ICEBERG_DATABASE, "iceberg_samples");
        int cacheMaxSize = Integer.parseInt(config.getOrDefault(CACHE_MAX_SIZE, "100"));
        long cacheRefreshMs = Long.parseLong(config.getOrDefault(CACHE_REFRESH_MS, "60000"));
        
        LOG.info("Configuring Dynamic Iceberg Sink:");
        LOG.info("  Database: {}", database);
        LOG.info("  Cache Max Size: {}", cacheMaxSize);
        LOG.info("  Cache Refresh: {} ms", cacheRefreshMs);
        
        // Create event routing generator
        EventRoutingGenerator generator = new EventRoutingGenerator(database);
        
        // Configure Dynamic Iceberg Sink
        // Note: writeParallelism not set - uses environment default (controlled by Managed Flink)
        DynamicIcebergSink.forInput(events)
            .generator(generator)
            .catalogLoader(catalogLoader)
            .immediateTableUpdate(true)  // Enable immediate schema evolution
            .cacheMaxSize(cacheMaxSize)  // Cache table metadata
            .cacheRefreshMs(cacheRefreshMs)  // Refresh cache periodically
            .set("write.format.default", "parquet")
            .set("format-version", "3")
            .set("write.delete.mode", "merge-on-read")
            .set("write.update.mode", "merge-on-read")
            .set("write.merge.mode", "merge-on-read")            .set("write.target-file-size-bytes", "134217728")
            .set("write.parquet.compression-codec", "snappy")
            .append();
        
        LOG.info("Dynamic Iceberg Sink configured successfully with automatic routing and schema evolution");
    }
    
    /**
     * Parse configuration from environment variables or command-line arguments.
     */
    private static Map<String, String> parseConfiguration(String[] args) {
        Map<String, String> config = new HashMap<>();
        
        // Read from environment variables
        config.put(KINESIS_STREAM_ARN, getEnvOrDefault("KINESIS_STREAM_ARN", ""));
        config.put(KINESIS_REGION, getEnvOrDefault("KINESIS_REGION", "us-east-1"));
        config.put(ICEBERG_CATALOG_NAME, getEnvOrDefault("ICEBERG_CATALOG_NAME", "glue_catalog"));
        config.put(ICEBERG_DATABASE, getEnvOrDefault("ICEBERG_DATABASE", "iceberg_samples"));
        config.put(ICEBERG_WAREHOUSE, getEnvOrDefault("ICEBERG_WAREHOUSE", ""));
        config.put(AWS_REGION, getEnvOrDefault("AWS_REGION", "us-east-1"));
        config.put(CHECKPOINT_INTERVAL, getEnvOrDefault("CHECKPOINT_INTERVAL_MS", "60000"));
        config.put(CACHE_MAX_SIZE, getEnvOrDefault("CACHE_MAX_SIZE", "100"));
        config.put(CACHE_REFRESH_MS, getEnvOrDefault("CACHE_REFRESH_MS", "60000"));
        
        // Override with command-line arguments if provided
        for (int i = 0; i < args.length - 1; i += 2) {
            String key = args[i].replaceFirst("^--", "");
            String value = args[i + 1];
            config.put(key, value);
        }
        
        LOG.info("Configuration loaded: {}", config);
        
        return config;
    }
    
    private static String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }
}
