package com.aws.samples.iceberg.dynamic;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.JsonNode;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Schema-Agnostic Dynamic Iceberg Sink Job.
 * 
 * This job demonstrates truly schema-agnostic event processing:
 * - Reads raw JSON from Kinesis without typed POJOs
 * - Infers Iceberg schema dynamically from JSON structure
 * - Routes to tables based on a configurable field (default: event_type)
 * - Handles ANY JSON structure without code changes
 * - Supports schema evolution as new fields appear
 * 
 * Table naming: {routing_field_value}_events (e.g., order_events, user_events)
 * 
 * Configuration:
 * - routing.field: Field used for table routing (default: event_type)
 * - routing.table.suffix: Suffix for auto-generated table names (default: _events)
 * - partition.candidates: Comma-separated fields to use for partitioning
 */
public class DynamicSinkJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(DynamicSinkJob.class);
    
    // Configuration keys
    private static final String KINESIS_STREAM_ARN = "kinesis.stream.arn";
    private static final String KINESIS_REGION = "kinesis.region";
    private static final String ICEBERG_CATALOG_NAME = "iceberg.catalog.name";
    private static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    private static final String ICEBERG_DATABASE = "iceberg.database";
    private static final String ICEBERG_WAREHOUSE = "iceberg.warehouse";
    private static final String S3TABLES_BUCKET_ARN = "s3tables.bucket.arn";
    private static final String AWS_REGION = "aws.region";
    private static final String CHECKPOINT_INTERVAL = "checkpoint.interval.ms";
    private static final String CACHE_MAX_SIZE = "cache.max.size";
    private static final String CACHE_REFRESH_MS = "cache.refresh.ms";
    
    // Schema-agnostic routing configuration
    private static final String ROUTING_FIELD = "routing.field";
    private static final String ROUTING_TABLE_SUFFIX = "routing.table.suffix";
    private static final String PARTITION_CANDIDATES = "partition.candidates";
    
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";

    
    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }
    
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            if (isLocal(env)) {
                Configuration config = new Configuration();
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
        LOG.info("Starting Schema-Agnostic Dynamic Iceberg Sink Job");
        
        StreamExecutionEnvironment env = createExecutionEnvironment();
        Map<String, String> config = loadApplicationProperties(env);
        
        if (isLocal(env)) {
            configureCheckpointing(env, config);
        } else {
            LOG.info("Running on AWS Managed Flink - checkpointing configured by the service");
        }
        
        // Create Kinesis source for raw JSON (schema-agnostic)
        KinesisStreamsSource<JsonNode> kinesisSource = createKinesisSource(config);
        
        DataStream<JsonNode> events = env.fromSource(
            kinesisSource,
            createWatermarkStrategy(),
            "Kinesis Source (Schema-Agnostic)",
            org.apache.flink.api.common.typeinfo.TypeInformation.of(JsonNode.class)
        )
        .uid("kinesis-source-dynamic")
        .name("Read from Kinesis (Raw JSON)");
        
        CatalogLoader catalogLoader = createCatalogLoader(config);
        configureDynamicSink(events, catalogLoader, config);
        
        LOG.info("Schema-Agnostic Dynamic Iceberg Sink Job configured successfully");
        
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received, cleaning up resources...");
        }));
        
        try {
            env.execute("Dynamic Iceberg Sink Job - Schema Agnostic");
        } catch (Exception e) {
            LOG.error("Job execution failed", e);
            throw e;
        }
    }

    
    private static void configureCheckpointing(StreamExecutionEnvironment env, Map<String, String> config) {
        long checkpointInterval = Long.parseLong(config.getOrDefault(CHECKPOINT_INTERVAL, "60000"));
        
        LOG.info("Configuring checkpointing for local development");
        
        env.enableCheckpointing(checkpointInterval);
        
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000);
        checkpointConfig.setCheckpointTimeout(600000);
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        checkpointConfig.setTolerableCheckpointFailureNumber(3);
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        checkpointConfig.enableUnalignedCheckpoints(true);
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
        
        LOG.info("Checkpointing configured: interval={}ms, mode=EXACTLY_ONCE", checkpointInterval);
    }
    
    private static KinesisStreamsSource<JsonNode> createKinesisSource(Map<String, String> config) {
        String streamArn = config.get(KINESIS_STREAM_ARN);
        String region = config.get(KINESIS_REGION);
        
        if (streamArn == null || streamArn.isEmpty()) {
            throw new IllegalArgumentException("Kinesis stream ARN is required");
        }
        
        LOG.info("Creating Kinesis source for stream: {} in region: {}", streamArn, region);
        
        Configuration sourceConfig = new Configuration();
        sourceConfig.setString("aws.region", region);
        sourceConfig.setString("flink.stream.initpos", "LATEST");
        
        return KinesisStreamsSource.<JsonNode>builder()
            .setStreamArn(streamArn)
            .setDeserializationSchema(new JsonNodeDeserializer())
            .setSourceConfig(sourceConfig)
            .build();
    }
    
    private static WatermarkStrategy<JsonNode> createWatermarkStrategy() {
        return WatermarkStrategy
            .<JsonNode>forBoundedOutOfOrderness(Duration.ofMinutes(1))
            .withTimestampAssigner((json, timestamp) -> {
                // Try to extract event_time from JSON
                if (json.has("event_time")) {
                    try {
                        String eventTime = json.get("event_time").asText();
                        return java.time.Instant.parse(eventTime).toEpochMilli();
                    } catch (Exception e) {
                        // Fall back to processing time
                    }
                }
                return System.currentTimeMillis();
            })
            .withIdleness(Duration.ofMinutes(5));
    }
    
    private static CatalogLoader createCatalogLoader(Map<String, String> config) {
        String catalogName = config.getOrDefault(ICEBERG_CATALOG_NAME, "glue_catalog");
        String catalogType = config.getOrDefault(ICEBERG_CATALOG_TYPE, "glue");
        String warehouse = config.get(ICEBERG_WAREHOUSE);
        String awsRegion = config.get(AWS_REGION);
        
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        
        if ("s3tables".equals(catalogType)) {
            String s3TableBucketArn = config.get(S3TABLES_BUCKET_ARN);
            if (s3TableBucketArn == null || s3TableBucketArn.isEmpty()) {
                throw new IllegalArgumentException("S3 Tables bucket ARN is required when using S3 Tables catalog");
            }
            
            catalogProperties.put("catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog");
            catalogProperties.put("warehouse", s3TableBucketArn);
            catalogProperties.put("client.region", awsRegion != null ? awsRegion : "us-east-1");
            
            LOG.info("Creating S3 Tables catalog: {} with bucket: {}", catalogName, s3TableBucketArn);
            
            return CatalogLoader.custom(
                catalogName,
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "software.amazon.s3tables.iceberg.S3TablesCatalog"
            );
        } else {
            if (warehouse == null || warehouse.isEmpty()) {
                throw new IllegalArgumentException("Iceberg warehouse path is required");
            }
            
            catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            catalogProperties.put("warehouse", warehouse);
            catalogProperties.put("client.region", awsRegion != null ? awsRegion : "us-east-1");
            
            LOG.info("Creating Glue catalog: {} with warehouse: {}", catalogName, warehouse);
            
            return CatalogLoader.custom(
                catalogName,
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
            );
        }
    }

    
    private static void configureDynamicSink(
            DataStream<JsonNode> events,
            CatalogLoader catalogLoader,
            Map<String, String> config) {
        
        String database = config.getOrDefault(ICEBERG_DATABASE, "iceberg_samples");
        int cacheMaxSize = Integer.parseInt(config.getOrDefault(CACHE_MAX_SIZE, "100"));
        long cacheRefreshMs = Long.parseLong(config.getOrDefault(CACHE_REFRESH_MS, "60000"));
        
        // Schema-agnostic routing configuration
        String routingField = config.getOrDefault(ROUTING_FIELD, "event_type");
        String tableSuffix = config.getOrDefault(ROUTING_TABLE_SUFFIX, "_events");
        String partitionCandidatesStr = config.getOrDefault(PARTITION_CANDIDATES, "event_date,region,date");
        List<String> partitionCandidates = Arrays.asList(partitionCandidatesStr.split(","));
        
        LOG.info("Configuring Schema-Agnostic Dynamic Iceberg Sink:");
        LOG.info("  Database: {}", database);
        LOG.info("  Routing Field: {}", routingField);
        LOG.info("  Table Suffix: {}", tableSuffix);
        LOG.info("  Partition Candidates: {}", partitionCandidates);
        LOG.info("  Cache Max Size: {}", cacheMaxSize);
        LOG.info("  Cache Refresh: {} ms", cacheRefreshMs);
        
        // Create schema-agnostic routing generator
        SchemaAgnosticRoutingGenerator generator = new SchemaAgnosticRoutingGenerator(
            database,
            routingField,
            null,  // No explicit table name field
            tableSuffix,
            partitionCandidates
        );
        
        DynamicIcebergSink.forInput(events)
            .generator(generator)
            .catalogLoader(catalogLoader)
            .immediateTableUpdate(true)
            .cacheMaxSize(cacheMaxSize)
            .cacheRefreshMs(cacheRefreshMs)
            .set("write.format.default", "parquet")
            .set("format-version", "3")
            .set("write.delete.mode", "merge-on-read")
            .set("write.update.mode", "merge-on-read")
            .set("write.merge.mode", "merge-on-read")
            .set("write.target-file-size-bytes", "134217728")
            .set("write.parquet.compression-codec", "snappy")
            .append();
        
        LOG.info("Schema-Agnostic Dynamic Iceberg Sink configured successfully");
    }
}
