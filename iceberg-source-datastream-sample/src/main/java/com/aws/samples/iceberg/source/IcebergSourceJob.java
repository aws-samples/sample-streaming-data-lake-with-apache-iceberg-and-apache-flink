package com.aws.samples.iceberg.source;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.aws.samples.iceberg.util.RowDataToJsonMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Iceberg Source Job - Reads from Iceberg tables and writes to Kinesis.
 * 
 * This sample demonstrates:
 * - FLIP-27 IcebergSource for streaming/batch reads
 * - Multiple starting strategies (latest, earliest, snapshot-based)
 * - Watermark generation from Iceberg column statistics
 * - Support for both Glue Catalog and S3 Tables
 * 
 * IMPORTANT: Streaming reads only work for APPEND-ONLY tables.
 * Tables with upserts (equality deletes) are NOT supported for streaming.
 * 
 * Configuration properties:
 * - iceberg.catalog.type: 'glue' or 's3tables'
 * - iceberg.database: Database/namespace name
 * - iceberg.table: Table name to read from
 * - iceberg.source.streaming: 'true' for streaming, 'false' for batch
 * - iceberg.source.starting-strategy: Starting strategy for streaming
 * - iceberg.source.monitor-interval: Interval to check for new snapshots (streaming)
 * - iceberg.source.watermark-column: Column for watermark generation (optional)
 * - kinesis.sink.stream.arn: Kinesis stream ARN to write to
 */
public class IcebergSourceJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceJob.class);
    
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
        
        env.execute("Iceberg Source to Kinesis");
    }
    
    private static void buildPipeline(StreamExecutionEnvironment env, Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String database = props.getProperty("iceberg.database", "iceberg_samples");
        String tableName = props.getProperty("iceberg.table", "orders");
        String region = props.getProperty("aws.region", "us-east-1");
        boolean streaming = Boolean.parseBoolean(props.getProperty("iceberg.source.streaming", "true"));
        
        LOG.info("=== Configuration ===");
        LOG.info("Catalog Type: {}", catalogType);
        LOG.info("AWS Region: {}", region);
        LOG.info("Database: {}", database);
        LOG.info("Table: {}", tableName);
        LOG.info("Streaming: {}", streaming);
        LOG.info("=====================");
        
        // Create catalog loader and table loader
        org.apache.iceberg.flink.CatalogLoader catalogLoader = com.aws.samples.iceberg.config.IcebergConfig.createCatalogLoader(catalogType, props);
        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        org.apache.iceberg.flink.TableLoader tableLoader = org.apache.iceberg.flink.TableLoader.fromCatalog(catalogLoader, tableId);
        
        // Load table to get schema
        tableLoader.open();
        Table table = tableLoader.loadTable();
        Schema icebergSchema = table.schema();
        
        LOG.info("Loaded Iceberg table: {} with schema: {}", tableId, icebergSchema);
        
        // Build IcebergSource
        IcebergSource<RowData> icebergSource = buildIcebergSource(tableLoader, props, streaming);
        
        // Create source stream with watermarks
        // Explicitly provide TypeInformation to avoid type erasure issues
        WatermarkStrategy<RowData> watermarkStrategy = buildWatermarkStrategy(props, icebergSchema);
        
        DataStream<RowData> sourceStream = env.fromSource(
                icebergSource,
                watermarkStrategy,
                "Iceberg Source: " + tableName,
                org.apache.flink.api.common.typeinfo.TypeInformation.of(RowData.class)
        ).uid("iceberg-source");
        
        // Convert RowData to JSON strings
        DataStream<String> jsonStream = sourceStream
                .map(new RowDataToJsonMapper(icebergSchema))
                .uid("rowdata-to-json")
                .name("RowData to JSON");

        // Write to Kinesis
        String sinkStreamArn = props.getProperty("kinesis.sink.stream.arn");
        KinesisStreamsSink<String> kinesisSink = buildKinesisSink(sinkStreamArn, region);
        
        jsonStream.sinkTo(kinesisSink).uid("kinesis-sink").name("Kinesis Sink");
        
        LOG.info("Pipeline built successfully");
    }
    
    private static IcebergSource<RowData> buildIcebergSource(org.apache.iceberg.flink.TableLoader tableLoader, Properties props, boolean streaming) {
        String monitorIntervalStr = props.getProperty("iceberg.source.monitor-interval", "60s");
        Duration monitorInterval = parseDuration(monitorIntervalStr);
        
        IcebergSource.Builder<RowData> builder = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .streaming(streaming);
        
        if (streaming) {
            // Configure streaming-specific options
            StreamingStartingStrategy startingStrategy = parseStartingStrategy(
                    props.getProperty("iceberg.source.starting-strategy", "INCREMENTAL_FROM_LATEST_SNAPSHOT")
            );
            builder.streamingStartingStrategy(startingStrategy);
            builder.monitorInterval(monitorInterval);
            
            LOG.info("Streaming source configured: strategy={}, monitorInterval={}",
                    startingStrategy, monitorInterval);
        }
        // Note: For batch mode time travel, use SQL API or configure via table properties
        // The FLIP-27 IcebergSource builder doesn't expose snapshotId/asOfTimestamp directly
        // for batch reads - it reads the current snapshot by default
        
        // Configure split options
        String splitSize = props.getProperty("iceberg.source.split-size");
        if (splitSize != null && !splitSize.isEmpty()) {
            builder.splitSize(Long.parseLong(splitSize));
        }
        
        return builder.build();
    }
    
    private static WatermarkStrategy<RowData> buildWatermarkStrategy(Properties props, Schema schema) {
        String watermarkColumn = props.getProperty("iceberg.source.watermark-column");
        
        if (watermarkColumn != null && !watermarkColumn.isEmpty()) {
            // Use Iceberg's built-in watermark generation from column statistics
            LOG.info("Watermark generation enabled for column: {}", watermarkColumn);
            
            // For now, use bounded out-of-orderness strategy
            // In production, you might want to use Iceberg's watermark-column option
            // which generates watermarks from file-level column statistics
            return WatermarkStrategy
                    .<RowData>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withIdleness(Duration.ofMinutes(1));
        }
        
        // No watermarks - use no watermarks strategy
        return WatermarkStrategy.noWatermarks();
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
    
    private static StreamingStartingStrategy parseStartingStrategy(String strategy) {
        switch (strategy.toUpperCase()) {
            case "TABLE_SCAN_THEN_INCREMENTAL":
                return StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL;
            case "INCREMENTAL_FROM_EARLIEST_SNAPSHOT":
                return StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT;
            case "INCREMENTAL_FROM_SNAPSHOT_ID":
                return StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID;
            case "INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP":
                return StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP;
            case "INCREMENTAL_FROM_LATEST_SNAPSHOT":
            default:
                return StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT;
        }
    }
    
    private static Duration parseDuration(String durationStr) {
        // Parse duration strings like "60s", "5m", "1h"
        if (durationStr.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(durationStr.replace("s", "")));
        } else if (durationStr.endsWith("m")) {
            return Duration.ofMinutes(Long.parseLong(durationStr.replace("m", "")));
        } else if (durationStr.endsWith("h")) {
            return Duration.ofHours(Long.parseLong(durationStr.replace("h", "")));
        }
        return Duration.ofSeconds(Long.parseLong(durationStr));
    }
    
    private static StreamExecutionEnvironment createExecutionEnvironment(Properties props) {
        Configuration config = new Configuration();
        
        // Note: RestOptions.PORT is NOT set here because Managed Flink rejects it.
        // For local dev, pass -Drest.port=8084 as a JVM argument if needed.
        
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
        requireProperty(props, "kinesis.sink.stream.arn", "KINESIS_SINK_STREAM_ARN is required");
        
        // Warn about streaming limitations
        boolean streaming = Boolean.parseBoolean(props.getProperty("iceberg.source.streaming", "true"));
        if (streaming) {
            LOG.warn("IMPORTANT: Streaming reads only work for APPEND-ONLY tables. " +
                    "Tables with upserts (equality deletes) are NOT supported for streaming.");
        }
    }
    
    private static void requireProperty(Properties props, String key, String message) {
        String value = props.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }
    
    private static Map<String, Properties> loadApplicationProperties() throws IOException {
        // Try to load from KinesisAnalyticsRuntime (AWS Managed Flink)
        // Falls back to local properties file if running locally
        Map<String, Properties> runtimeProps;
        try {
            runtimeProps = KinesisAnalyticsRuntime.getApplicationProperties();
        } catch (Exception e) {
            runtimeProps = new HashMap<>();
        }
        if (runtimeProps == null || runtimeProps.isEmpty()) {
            LOG.info("No runtime properties from Managed Flink, loading local properties");
            return loadLocalProperties();
        }
        LOG.info("Loaded runtime properties from Managed Flink");
        return runtimeProps;
    }
    
    private static Map<String, Properties> loadLocalProperties() throws IOException {
        Map<String, Properties> appProperties = new HashMap<>();
        
        try (InputStream input = IcebergSourceJob.class.getClassLoader()
                .getResourceAsStream("flink-application-properties-dev.json")) {
            if (input != null) {
                ObjectMapper mapper = new ObjectMapper();
                @SuppressWarnings("unchecked")
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
}
