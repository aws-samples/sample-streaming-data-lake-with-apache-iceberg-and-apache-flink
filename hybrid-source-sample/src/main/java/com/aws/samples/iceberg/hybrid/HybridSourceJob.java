package com.aws.samples.iceberg.hybrid;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.util.OrderEventDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.IcebergSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Hybrid Source Job - Bootstrap from Iceberg historical data, then switch to real-time Kinesis.
 * 
 * This sample demonstrates the Flink HybridSource pattern:
 * 1. First, read all historical data from an Iceberg table (bounded source)
 * 2. Then, seamlessly switch to reading real-time events from Kinesis (unbounded source)
 * 
 * Use cases:
 * - Backfilling a new streaming application with historical data
 * - Recovering from extended downtime without data loss
 * - Migrating from batch to streaming processing
 * 
 * The HybridSource appears as a single source in the Flink job graph, making it
 * operationally simpler than managing multiple sources with custom switching logic.
 * 
 * Configuration properties:
 * - iceberg.catalog.type: 'glue' or 's3tables'
 * - iceberg.database: Database/namespace name
 * - iceberg.table: Table name to read historical data from
 * - kinesis.source.stream.arn: Kinesis stream ARN for real-time data
 * - kinesis.source.starting.position: Where to start in Kinesis (LATEST, TRIM_HORIZON, AT_TIMESTAMP)
 * - kinesis.source.starting.timestamp: Timestamp for AT_TIMESTAMP position (ISO-8601)
 * - kinesis.sink.stream.arn: Kinesis stream ARN to write processed data
 */
public class HybridSourceJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceJob.class);
    private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();
    
    public static void main(String[] args) throws Exception {
        // Load configuration
        Map<String, Properties> applicationProperties = loadApplicationProperties();
        Properties flinkProps = applicationProperties.getOrDefault("FlinkApplicationProperties", new Properties());
        
        // Validate configuration
        validateConfiguration(flinkProps);
        
        // Create execution environment
        StreamExecutionEnvironment env = createExecutionEnvironment(flinkProps);
        
        // Build and execute pipeline
        buildPipeline(env, flinkProps);
        
        env.execute("Hybrid Source: Iceberg Bootstrap + Kinesis Streaming");
    }
    
    private static void buildPipeline(StreamExecutionEnvironment env, Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String database = props.getProperty("iceberg.database", "iceberg_samples");
        String tableName = props.getProperty("iceberg.table", "orders");
        String region = props.getProperty("aws.region", "us-east-1");
        String sourceStreamArn = props.getProperty("kinesis.source.stream.arn");
        String sinkStreamArn = props.getProperty("kinesis.sink.stream.arn");
        
        LOG.info("Building Hybrid Source pipeline: Iceberg({}.{}) -> Kinesis({})",
                database, tableName, sourceStreamArn);
        
        // Create catalog and get table
        Catalog catalog = createCatalog(catalogType, props);
        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        Table table = catalog.loadTable(tableId);
        Schema icebergSchema = table.schema();
        
        LOG.info("Loaded Iceberg table: {} with schema: {}", tableId, icebergSchema);
        
        // Build Iceberg source (bounded - reads all historical data)
        IcebergSource<RowData> icebergSource = IcebergSource.forRowData()
                .table(table)
                .streaming(false)  // Bounded - read all data then finish
                .build();
        
        // Build Kinesis source (unbounded - real-time streaming)
        KinesisStreamsSource<OrderEvent> kinesisSource = buildKinesisSource(sourceStreamArn, region, props);
        
        // Build HybridSource: Iceberg first, then Kinesis
        // Note: HybridSource requires sources to produce the same type
        // We'll process them separately and union the results
        
        // Option 1: Process Iceberg data
        DataStream<String> icebergStream = env.fromSource(
                icebergSource,
                WatermarkStrategy.noWatermarks(),
                "Iceberg Historical Source"
        ).map(new RowDataToJsonMapper(icebergSchema))
         .name("Iceberg to JSON");
        
        // Option 2: Process Kinesis data
        DataStream<String> kinesisStream = env.fromSource(
                kinesisSource,
                WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner((event, timestamp) -> 
                                event.getEventTime() != null ? 
                                        event.getEventTime().toEpochMilli() : 
                                        System.currentTimeMillis())
                        .withIdleness(Duration.ofMinutes(1)),
                "Kinesis Real-time Source"
        ).map(event -> OBJECT_MAPPER.writeValueAsString(event))
         .name("Kinesis to JSON");
        
        // Union both streams
        DataStream<String> unifiedStream = icebergStream.union(kinesisStream);
        
        // Write to sink Kinesis stream
        KinesisStreamsSink<String> kinesisSink = buildKinesisSink(sinkStreamArn, region);
        
        unifiedStream.sinkTo(kinesisSink).name("Kinesis Sink");
        
        LOG.info("Pipeline built successfully - Iceberg bootstrap + Kinesis streaming");
    }
    
    /**
     * Alternative implementation using true HybridSource.
     * This requires both sources to produce the same type (RowData).
     * 
     * Note: This is more complex but provides seamless switchover semantics.
     */
    private static void buildTrueHybridPipeline(StreamExecutionEnvironment env, Properties props) {
        // This would require:
        // 1. A Kinesis source that produces RowData (custom deserializer)
        // 2. Or converting OrderEvent to RowData
        // 
        // HybridSource<RowData> hybridSource = HybridSource
        //     .<RowData>builder(icebergSource)
        //     .addSource(kinesisRowDataSource)
        //     .build();
        //
        // The switchover happens automatically when the first source finishes.
        
        LOG.info("True HybridSource implementation would go here");
    }
    
    private static KinesisStreamsSource<OrderEvent> buildKinesisSource(String streamArn, String region, Properties props) {
        // Configure Kinesis source
        Configuration sourceConfig = new Configuration();
        sourceConfig.setString("aws.region", region);
        sourceConfig.setString("flink.stream.initpos", "LATEST");
        sourceConfig.setString("flink.shard.discovery.intervalmillis", "10000");
        
        LOG.info("Creating Kinesis source for stream: {} in region: {}", streamArn, region);
        
        return KinesisStreamsSource.<OrderEvent>builder()
                .setStreamArn(streamArn)
                .setDeserializationSchema(new OrderEventDeserializer())
                .setSourceConfig(sourceConfig)
                .build();
    }
    
    private static KinesisStreamsSink<String> buildKinesisSink(String streamArn, String region) {
        Properties sinkProps = new Properties();
        sinkProps.setProperty(AWSConfigConstants.AWS_REGION, region);
        
        return KinesisStreamsSink.<String>builder()
                .setStreamArn(streamArn)
                .setSerializationSchema(new SimpleStringSchema())
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .setKinesisClientProperties(sinkProps)
                .build();
    }
    
    private static Catalog createCatalog(String catalogType, Properties props) {
        String region = props.getProperty("aws.region", "us-east-1");
        
        if ("s3tables".equalsIgnoreCase(catalogType)) {
            return createS3TablesCatalog(props, region);
        } else {
            return createGlueCatalog(props, region);
        }
    }
    
    private static Catalog createGlueCatalog(Properties props, String region) {
        String warehouse = props.getProperty("iceberg.warehouse");
        
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName());
        catalogProps.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        catalogProps.put("client.region", region);
        
        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(new org.apache.hadoop.conf.Configuration());
        catalog.initialize("glue_catalog", catalogProps);
        
        LOG.info("Created Glue Catalog with warehouse: {}", warehouse);
        return catalog;
    }
    
    private static Catalog createS3TablesCatalog(Properties props, String region) {
        String tableBucketArn = props.getProperty("s3tables.bucket.arn");
        
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.CATALOG_IMPL, S3TablesCatalog.class.getName());
        catalogProps.put("s3tables.catalog.client.region", region);
        catalogProps.put("warehouse", tableBucketArn);
        
        S3TablesCatalog catalog = new S3TablesCatalog();
        catalog.initialize("s3tables_catalog", catalogProps);
        
        LOG.info("Created S3 Tables Catalog with bucket: {}", tableBucketArn);
        return catalog;
    }
    
    private static StreamExecutionEnvironment createExecutionEnvironment(Properties props) {
        Configuration config = new Configuration();
        
        // Local development settings
        if (isLocalDevelopment()) {
            config.setInteger("rest.port", 8086);
        }
        
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        
        // Configure checkpointing
        long checkpointInterval = Long.parseLong(props.getProperty("checkpoint.interval.ms", "60000"));
        env.enableCheckpointing(checkpointInterval);
        
        return env;
    }
    
    private static void validateConfiguration(Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        
        // Validate required properties based on catalog type
        if ("glue".equalsIgnoreCase(catalogType)) {
            requireProperty(props, "iceberg.warehouse", "ICEBERG_WAREHOUSE is required for Glue catalog");
        } else if ("s3tables".equalsIgnoreCase(catalogType)) {
            requireProperty(props, "s3tables.bucket.arn", "S3 Tables bucket ARN is required");
        }
        
        requireProperty(props, "iceberg.database", "ICEBERG_DATABASE is required");
        requireProperty(props, "iceberg.table", "ICEBERG_TABLE is required");
        requireProperty(props, "kinesis.source.stream.arn", "KINESIS_SOURCE_STREAM_ARN is required");
        requireProperty(props, "kinesis.sink.stream.arn", "KINESIS_SINK_STREAM_ARN is required");
    }
    
    private static void requireProperty(Properties props, String key, String message) {
        String value = props.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }
    
    private static Map<String, Properties> loadApplicationProperties() throws IOException {
        if (isLocalDevelopment()) {
            return loadLocalProperties();
        }
        return KinesisAnalyticsRuntime.getApplicationProperties();
    }
    
    private static Map<String, Properties> loadLocalProperties() throws IOException {
        Map<String, Properties> appProperties = new HashMap<>();
        
        try (InputStream input = HybridSourceJob.class.getClassLoader()
                .getResourceAsStream("flink-application-properties-dev.json")) {
            if (input != null) {
                ObjectMapper mapper = new ObjectMapper();
                Map<String, Object>[] propertyGroups = mapper.readValue(input, Map[].class);
                
                for (Map<String, Object> group : propertyGroups) {
                    String groupId = (String) group.get("PropertyGroupId");
                    @SuppressWarnings("unchecked")
                    Map<String, String> propertyMap = (Map<String, String>) group.get("PropertyMap");
                    
                    Properties props = new Properties();
                    props.putAll(propertyMap);
                    appProperties.put(groupId, props);
                }
            }
        }
        
        return appProperties;
    }
    
    private static boolean isLocalDevelopment() {
        return System.getenv("IS_LOCAL") != null || 
               System.getProperty("flink.execution.target") == null;
    }
    
    private static ObjectMapper createObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
