package com.aws.samples.iceberg.datastream;

import com.aws.samples.iceberg.config.IcebergConfig;
import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.util.EventToRowDataConverter;
import com.aws.samples.iceberg.util.OrderEventDeserializer;
import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.flink.maintenance.api.*;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * DataStream API sample demonstrating IcebergSink (SinkV2) with Apache Iceberg 1.10.
 * 
 * This job reads OrderEvent data from a Kinesis stream and writes to an Iceberg table
 * using the new IcebergSink (SinkV2-based) with support for:
 * - Table format v3 with delete vectors
 * - Upsert mode with equality deletes
 * - Branch writes for staging data
 * - Metrics for monitoring
 * 
 * Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6
 */
public class DataStreamIcebergJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(DataStreamIcebergJob.class);
    
    // Configuration keys
    private static final String KINESIS_STREAM_ARN = "kinesis.stream.arn";
    private static final String KINESIS_REGION = "kinesis.region";
    private static final String ICEBERG_CATALOG_NAME = "iceberg.catalog.name";
    private static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    private static final String ICEBERG_DATABASE = "iceberg.database";
    private static final String ICEBERG_TABLE = "iceberg.table";
    private static final String ICEBERG_WAREHOUSE = "iceberg.warehouse";
    private static final String S3TABLES_BUCKET_ARN = "s3tables.bucket.arn";
    private static final String AWS_REGION = "aws.region";
    private static final String CHECKPOINT_INTERVAL = "checkpoint.interval.ms";
    private static final String ICEBERG_BRANCH = "iceberg.branch";  // Optional branch for staging writes
    private static final String ENABLE_MAINTENANCE = "enable.maintenance";
    private static final String RDS_JDBC_URL = "rds.jdbc.url";
    private static final String RDS_USER = "rds.user";
    private static final String RDS_PASSWORD = "rds.password";
    private static final String WRITE_MODE = "write.mode";  // "append" or "upsert" (default: upsert)
    private static final String PRIMARY_KEY_COLUMNS = "primary.key.columns";  // Comma-separated list for upsert mode
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";
    
    /**
     * Check if running in local execution mode.
     */
    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }
    
    /**
     * Load application properties from Amazon Managed Service for Apache Flink runtime
     * or from local properties file when running locally.
     */
    private static Map<String, String> loadApplicationProperties(StreamExecutionEnvironment env) throws Exception {
        Map<String, String> config = new HashMap<>();
        
        if (isLocal(env)) {
            LOG.info("Loading configuration from local properties file: {}", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            // Load from local properties file for local development
            Map<String, Properties> props = KinesisAnalyticsRuntime.getApplicationProperties(
                DataStreamIcebergJob.class.getClassLoader().getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath()
            );
            Properties flinkProps = props.getOrDefault("FlinkApplicationProperties", new Properties());
            
            config.put(KINESIS_STREAM_ARN, flinkProps.getProperty("kinesis.stream.arn", ""));
            config.put(KINESIS_REGION, flinkProps.getProperty("kinesis.region", "us-east-1"));
            config.put(ICEBERG_CATALOG_NAME, flinkProps.getProperty("iceberg.catalog.name", "glue_catalog"));
            config.put(ICEBERG_CATALOG_TYPE, flinkProps.getProperty("iceberg.catalog.type", "glue"));
            config.put(ICEBERG_DATABASE, flinkProps.getProperty("iceberg.database", "iceberg_samples"));
            config.put(ICEBERG_TABLE, flinkProps.getProperty("iceberg.table", "orders"));
            config.put(ICEBERG_WAREHOUSE, flinkProps.getProperty("iceberg.warehouse", ""));
            config.put(S3TABLES_BUCKET_ARN, flinkProps.getProperty("s3tables.bucket.arn", ""));
            config.put(AWS_REGION, flinkProps.getProperty("aws.region", "us-east-1"));
            config.put(CHECKPOINT_INTERVAL, flinkProps.getProperty("checkpoint.interval.ms", "60000"));
            config.put(ICEBERG_BRANCH, flinkProps.getProperty("iceberg.branch", ""));
            config.put(ENABLE_MAINTENANCE, flinkProps.getProperty("enable.maintenance", "false"));
            config.put(RDS_JDBC_URL, flinkProps.getProperty("rds.jdbc.url", ""));
            config.put(RDS_USER, flinkProps.getProperty("rds.user", ""));
            config.put(RDS_PASSWORD, flinkProps.getProperty("rds.password", ""));
            config.put(WRITE_MODE, flinkProps.getProperty("write.mode", "upsert"));
            config.put(PRIMARY_KEY_COLUMNS, flinkProps.getProperty("primary.key.columns", "event_id,event_date,region"));
        } else {
            LOG.info("Loading configuration from Amazon Managed Service for Apache Flink runtime properties");
            // Load from Kinesis Analytics Runtime properties for MSF deployment
            Map<String, Properties> applicationProperties = KinesisAnalyticsRuntime.getApplicationProperties();
            
            Properties flinkProps = applicationProperties.getOrDefault("FlinkApplicationProperties", new Properties());
            
            config.put(KINESIS_STREAM_ARN, flinkProps.getProperty("kinesis.stream.arn", ""));
            config.put(KINESIS_REGION, flinkProps.getProperty("kinesis.region", "us-east-1"));
            config.put(ICEBERG_CATALOG_NAME, flinkProps.getProperty("iceberg.catalog.name", "glue_catalog"));
            config.put(ICEBERG_CATALOG_TYPE, flinkProps.getProperty("iceberg.catalog.type", "glue"));
            config.put(ICEBERG_DATABASE, flinkProps.getProperty("iceberg.database", "iceberg_samples"));
            config.put(ICEBERG_TABLE, flinkProps.getProperty("iceberg.table", "orders"));
            config.put(ICEBERG_WAREHOUSE, flinkProps.getProperty("iceberg.warehouse", ""));
            config.put(S3TABLES_BUCKET_ARN, flinkProps.getProperty("s3tables.bucket.arn", ""));
            config.put(AWS_REGION, flinkProps.getProperty("aws.region", "us-east-1"));
            config.put(CHECKPOINT_INTERVAL, flinkProps.getProperty("checkpoint.interval.ms", "60000"));
            config.put(ICEBERG_BRANCH, flinkProps.getProperty("iceberg.branch", ""));
            config.put(ENABLE_MAINTENANCE, flinkProps.getProperty("enable.maintenance", "false"));
            config.put(RDS_JDBC_URL, flinkProps.getProperty("rds.jdbc.url", ""));
            config.put(RDS_USER, flinkProps.getProperty("rds.user", ""));
            config.put(RDS_PASSWORD, flinkProps.getProperty("rds.password", ""));
            config.put(WRITE_MODE, flinkProps.getProperty("write.mode", "upsert"));
            config.put(PRIMARY_KEY_COLUMNS, flinkProps.getProperty("primary.key.columns", "event_id,event_date,region"));
        }
        
        LOG.info("Configuration loaded successfully");
        return config;
    }
    
    /**
     * Create execution environment with Web UI for local development.
     */
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        // Try to create local environment with Web UI
        // If flink-runtime-web is on classpath, this will enable the UI
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            if (isLocal(env)) {
                // Recreate with Web UI enabled
                org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
                config.setString("rest.port", "8081");
                config.setString("rest.bind-address", "localhost");
                env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
                LOG.info("Local execution detected - Flink Web UI available at http://localhost:8081");
            }
            return env;
        } catch (Exception e) {
            LOG.warn("Could not create environment with Web UI, falling back to standard environment", e);
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }
    
    public static void main(String[] args) throws Exception {
        LOG.info("Starting DataStream Iceberg Job");
        
        // Set up Flink execution environment with Web UI for local dev
        StreamExecutionEnvironment env = createExecutionEnvironment();
        
        // Load configuration from MSF runtime properties or environment variables
        Map<String, String> config = loadApplicationProperties(env);
        
        // Validate required configuration
        validateConfiguration(config);
        
        // Configure checkpointing for local development only
        // AWS Managed Flink configures checkpointing automatically
        if (isLocal(env)) {
            configureCheckpointing(env, config);
        } else {
            LOG.info("Running on AWS Managed Flink - checkpointing configured by the service");
        }
        
        // Create Kinesis source
        KinesisStreamsSource<OrderEvent> kinesisSource = createKinesisSource(config);
        
        // Read from Kinesis with watermark strategy
        // Explicitly provide type information to avoid type erasure issues on Managed Flink
        DataStream<OrderEvent> orderEvents = env.fromSource(
            kinesisSource,
            createWatermarkStrategy(),
            "Kinesis Source",
            org.apache.flink.api.common.typeinfo.TypeInformation.of(OrderEvent.class)
        )
        .uid("kinesis-source")
        .name("Read from Kinesis");
        
        // Convert OrderEvent to RowData for Iceberg
        DataStream<RowData> rowDataStream = orderEvents
            .map(EventToRowDataConverter::convertOrderEvent)
            .uid("event-to-rowdata")
            .name("Convert to RowData");
        
        // Create Iceberg catalog and table loaders
        CatalogLoader catalogLoader = createCatalogLoader(config);
        
        // Ensure table exists, create if necessary
        ensureTableExists(catalogLoader, config);
        
        TableLoader tableLoader = createTableLoader(catalogLoader, config);
        
        // Configure and add IcebergSink (SinkV2) with upsert mode
        configureIcebergSink(rowDataStream, tableLoader, config, env);
        
        LOG.info("DataStream Iceberg Job configured successfully");
        
        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            LOG.info("Shutdown signal received, cleaning up resources...");
        }));
        
        // Execute the job
        try {
            env.execute("DataStream Iceberg Job - Orders");
        } catch (Exception e) {
            LOG.error("Job execution failed", e);
            throw e;
        }
    }
    
    /**
     * Validate required configuration parameters.
     */
    private static void validateConfiguration(Map<String, String> config) {
        String streamArn = config.get(KINESIS_STREAM_ARN);
        String catalogType = config.getOrDefault(ICEBERG_CATALOG_TYPE, "glue");
        String warehouse = config.get(ICEBERG_WAREHOUSE);
        String s3TableBucketArn = config.get(S3TABLES_BUCKET_ARN);
        
        if (streamArn == null || streamArn.isEmpty()) {
            throw new IllegalArgumentException("KINESIS_STREAM_ARN is required");
        }
        
        // Validate catalog-specific requirements
        if ("s3tables".equals(catalogType)) {
            if (s3TableBucketArn == null || s3TableBucketArn.isEmpty()) {
                throw new IllegalArgumentException("S3TABLES_BUCKET_ARN is required when using S3 Tables catalog");
            }
            LOG.info("Using S3 Tables catalog with bucket ARN: {}", s3TableBucketArn);
        } else {
            if (warehouse == null || warehouse.isEmpty()) {
                throw new IllegalArgumentException("ICEBERG_WAREHOUSE is required when using Glue catalog");
            }
            LOG.info("Using Glue catalog with warehouse: {}", warehouse);
        }
        
        LOG.info("Configuration validated successfully");
    }
    
    /**
     * Configure checkpointing for local development only.
     * AWS Managed Flink configures checkpointing automatically.
     * Requirements: 3.1
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
        checkpointConfig.setTolerableCheckpointFailureNumber(3);  // Allow 3 failures before job fails
        checkpointConfig.setExternalizedCheckpointCleanup(
            CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        
        // Enable unaligned checkpoints for better performance under backpressure
        checkpointConfig.enableUnalignedCheckpoints(true);
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
        
        LOG.info("Checkpointing configured: interval={}ms, mode=EXACTLY_ONCE, unaligned=true", checkpointInterval);
    }
    
    /**
     * Create Kinesis source with OrderEvent deserializer and production-ready configuration.
     * Requirements: 3.1
     */
    private static KinesisStreamsSource<OrderEvent> createKinesisSource(Map<String, String> config) {
        String streamArn = config.get(KINESIS_STREAM_ARN);
        String region = config.get(KINESIS_REGION);
        
        if (streamArn == null || streamArn.isEmpty()) {
            throw new IllegalArgumentException("Kinesis stream ARN is required");
        }
        
        LOG.info("Creating Kinesis source for stream: {} in region: {}", streamArn, region);
        
        // Configure Kinesis source with production settings
        Configuration sourceConfig = new Configuration();
        sourceConfig.setString("aws.region", region);
        sourceConfig.setString("flink.stream.initpos", "LATEST");
        sourceConfig.setString("flink.shard.discovery.intervalmillis", "10000");  // Discover new shards every 10s
        sourceConfig.setString("flink.shard.getrecords.maxrecordcount", "10000");  // Max records per GetRecords call
        
        return KinesisStreamsSource.<OrderEvent>builder()
            .setStreamArn(streamArn)
            .setDeserializationSchema(new OrderEventDeserializer())
            .setSourceConfig(sourceConfig)
            .build();
    }
    
    /**
     * Create watermark strategy for handling out-of-order events with production settings.
     * Allows events up to 1 minute out of order and handles idle sources.
     * Requirements: 3.1
     */
    private static WatermarkStrategy<OrderEvent> createWatermarkStrategy() {
        return WatermarkStrategy
            .<OrderEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
            .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
            .withIdleness(Duration.ofMinutes(5));  // Mark source as idle after 5 minutes of no data
    }
    
    /**
     * Ensure the Iceberg table exists, creating it if necessary.
     * Creates both the database and table with proper configuration.
     * Requirements: 3.2, 3.3
     */
    private static void ensureTableExists(CatalogLoader catalogLoader, Map<String, String> config) {
        String database = config.getOrDefault(ICEBERG_DATABASE, "iceberg_samples");
        String table = config.getOrDefault(ICEBERG_TABLE, "orders");
        
        try {
            // Load the catalog
            Catalog catalog = catalogLoader.loadCatalog();
            
            TableIdentifier tableId = TableIdentifier.of(database, table);
            
            // Check if table exists
            if (catalog.tableExists(tableId)) {
                LOG.info("Table {}.{} already exists", database, table);
                return;
            }
            
            LOG.info("Table {}.{} does not exist, creating it now...", database, table);
            
            // Create the table with v2 format and delete vectors
            Schema schema = createOrderEventSchema();
            
            org.apache.iceberg.PartitionSpec partitionSpec = org.apache.iceberg.PartitionSpec.builderFor(schema)
                .day("event_date")
                .identity("region")
                .build();
            
            Map<String, String> tableProperties = new HashMap<>();
            tableProperties.put("format-version", "2");
            tableProperties.put("write.format.default", "parquet");
            tableProperties.put("write.parquet.compression-codec", "snappy");
            tableProperties.put("write.target-file-size-bytes", "134217728");
            tableProperties.put("write.delete.mode", "merge-on-read");
            tableProperties.put("write.update.mode", "merge-on-read");
            tableProperties.put("write.merge.mode", "merge-on-read");
            tableProperties.put("write.upsert.enabled", "true");
            
            // Try to create the table - this will also create the database if needed
            catalog.createTable(tableId, schema, partitionSpec, tableProperties);
            LOG.info("Successfully created table {}.{} with v2 format and upsert enabled", database, table);
            
        } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
            // Database doesn't exist - this shouldn't happen with Glue, but handle it
            LOG.error("Database {} does not exist. Please create it first using AWS Glue console or Athena.", database);
            throw new RuntimeException("Database does not exist: " + database + ". Create it first.", e);
        } catch (org.apache.iceberg.exceptions.AlreadyExistsException e) {
            LOG.info("Table {}.{} was created by another process", database, table);
        } catch (Exception e) {
            LOG.error("Failed to ensure table exists: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to create Iceberg table: " + e.getMessage(), e);
        }
    }
    
    /**
     * Create Iceberg catalog loader with Glue Catalog or S3 Tables Catalog configuration.
     * Requirements: 3.2
     */
    private static CatalogLoader createCatalogLoader(Map<String, String> config) {
        String catalogName = config.getOrDefault(ICEBERG_CATALOG_NAME, "glue_catalog");
        String catalogType = config.getOrDefault(ICEBERG_CATALOG_TYPE, "glue");
        String warehouse = config.get(ICEBERG_WAREHOUSE);
        String awsRegion = config.get(AWS_REGION);
        
        Map<String, String> catalogProperties = new HashMap<>();
        catalogProperties.put("type", "iceberg");
        
        if ("s3tables".equals(catalogType)) {
            // S3 Tables Catalog configuration
            String s3TableBucketArn = config.get(S3TABLES_BUCKET_ARN);
            if (s3TableBucketArn == null || s3TableBucketArn.isEmpty()) {
                throw new IllegalArgumentException("S3 Tables bucket ARN is required when using S3 Tables catalog");
            }
            
            catalogProperties.put("catalog-impl", "software.amazon.s3tables.iceberg.S3TablesCatalog");
            catalogProperties.put("warehouse", s3TableBucketArn);
            catalogProperties.put("client.region", awsRegion != null ? awsRegion : "us-east-1");
            
            LOG.info("Creating S3 Tables catalog loader: {} with table bucket: {}", catalogName, s3TableBucketArn);
            
            return CatalogLoader.custom(
                catalogName,
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "software.amazon.s3tables.iceberg.S3TablesCatalog"
            );
        } else {
            // Glue Catalog configuration (default)
            if (warehouse == null || warehouse.isEmpty()) {
                throw new IllegalArgumentException("Iceberg warehouse path is required");
            }
            
            catalogProperties.put("catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog");
            catalogProperties.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            catalogProperties.put("warehouse", warehouse);
            catalogProperties.put("aws.region", awsRegion != null ? awsRegion : "us-east-1");
            
            LOG.info("Creating Glue catalog loader: {} with warehouse: {}", catalogName, warehouse);
            
            return CatalogLoader.custom(
                catalogName,
                catalogProperties,
                new org.apache.hadoop.conf.Configuration(),
                "org.apache.iceberg.aws.glue.GlueCatalog"
            );
        }
    }
    
    /**
     * Create table loader for the target Iceberg table.
     * Requirements: 3.2
     */
    private static TableLoader createTableLoader(CatalogLoader catalogLoader, Map<String, String> config) {
        String database = config.getOrDefault(ICEBERG_DATABASE, "iceberg_samples");
        String table = config.getOrDefault(ICEBERG_TABLE, "orders");
        
        TableIdentifier tableIdentifier = TableIdentifier.of(database, table);
        
        LOG.info("Creating table loader for: {}.{}", database, table);
        
        return TableLoader.fromCatalog(catalogLoader, tableIdentifier);
    }
    
    /**
     * Configure IcebergSink (SinkV2) with configurable write mode (append or upsert).
     * Supports optional branch writes for staging data before merging to main.
     * Optionally adds post-commit maintenance topology with JDBC locks.
     * Requirements: 3.2, 3.3, 3.4, 3.5, 3.6
     */
    private static void configureIcebergSink(
            DataStream<RowData> rowDataStream,
            TableLoader tableLoader,
            Map<String, String> config,
            StreamExecutionEnvironment env) {
        
        // Get write mode configuration (default: upsert for backward compatibility)
        String writeMode = config.getOrDefault(WRITE_MODE, "upsert");
        boolean isUpsertMode = "upsert".equalsIgnoreCase(writeMode);
        
        // Get primary key columns for upsert mode
        // When using HASH distribution with partitioned tables, partition columns must be in equality fields
        String primaryKeyColumnsStr = config.getOrDefault(PRIMARY_KEY_COLUMNS, "event_id,event_date,region");
        List<String> equalityFieldColumns = Arrays.asList(primaryKeyColumnsStr.split(","));
        
        // Check if branch writes are enabled
        String branch = config.get(ICEBERG_BRANCH);
        boolean useBranchWrites = branch != null && !branch.isEmpty();
        
        LOG.info("Configuring IcebergSink with write mode: {}", writeMode);
        if (isUpsertMode) {
            LOG.info("Upsert mode enabled with equality fields: {}", equalityFieldColumns);
        } else {
            LOG.info("Append-only mode enabled (no deduplication)");
        }
        if (useBranchWrites) {
            LOG.info("Branch writes enabled to branch: {}", branch);
        }
        
        // Build IcebergSink with SinkV2 features
        var sinkBuilder = IcebergSink.forRowData(rowDataStream)
            .tableLoader(tableLoader)
            // Configure write properties for v2 table format
            .set("write.format.default", "parquet")
            .set("write.target-file-size-bytes", "134217728")  // 128 MB
            // Enable metrics for monitoring (Requirements: 3.6)
            .setSnapshotProperty("flink.job-id", "datastream-iceberg-job")
            .setSnapshotProperty("flink.max-committed-checkpoint-id", "0");
        
        // Configure upsert mode if enabled
        if (isUpsertMode) {
            sinkBuilder
                .upsert(true)  // Enable upsert mode for merge-on-read with delete vectors
                .equalityFieldColumns(equalityFieldColumns)
                .set("write.delete.mode", "merge-on-read")  // Use delete vectors
                .set("write.update.mode", "merge-on-read")
                .set("write.merge.mode", "merge-on-read")
                // Use HASH distribution mode with partition columns in equality fields
                .distributionMode(org.apache.iceberg.DistributionMode.HASH);
        } else {
            // Append-only mode - no upsert, no equality fields
            sinkBuilder
                .upsert(false)
                .distributionMode(org.apache.iceberg.DistributionMode.NONE);
        }
        
        // Add branch configuration if specified
        if (useBranchWrites) {
            sinkBuilder.toBranch(branch);
        }
        
        sinkBuilder.append();
        
        // Check if maintenance is enabled
        // Note: S3 Tables handles maintenance automatically, so skip if using S3 Tables
        String catalogType = config.getOrDefault(ICEBERG_CATALOG_TYPE, "glue");
        boolean enableMaintenance = Boolean.parseBoolean(config.getOrDefault(ENABLE_MAINTENANCE, "false"));
        
        if ("s3tables".equals(catalogType)) {
            LOG.info("Using S3 Tables catalog - maintenance is handled automatically by the service");
        } else if (enableMaintenance) {
            LOG.info("Maintenance ENABLED - configuring table maintenance topology");
            try {
                TriggerLockFactory lockFactory = createJdbcLockFactory(config);
                setupTableMaintenance(env, tableLoader, lockFactory);
            } catch (Exception e) {
                LOG.error("Failed to setup table maintenance", e);
                throw new RuntimeException("Failed to setup table maintenance", e);
            }
        } else {
            LOG.info("Maintenance DISABLED - running without maintenance topology");
        }
        
        LOG.info("IcebergSink configured successfully with {} mode and metrics", writeMode);
    }
    
    /**
     * Setup table maintenance with JDBC locks.
     * Configures ExpireSnapshots, RewriteDataFiles, and DeleteOrphanFiles.
     */
    private static void setupTableMaintenance(
            StreamExecutionEnvironment env,
            TableLoader tableLoader,
            TriggerLockFactory lockFactory) throws Exception {
        
        LOG.info("Configuring table maintenance tasks");
        
        TableMaintenance.forTable(env, tableLoader, lockFactory)
            .uidSuffix("datastream-maintenance")
            .rateLimit(Duration.ofMinutes(10))
            .lockCheckDelay(Duration.ofSeconds(30))
            .add(ExpireSnapshots.builder()
                .scheduleOnCommitCount(10)
                .maxSnapshotAge(Duration.ofHours(24))
                .retainLast(5))
            .add(RewriteDataFiles.builder()
                .scheduleOnDataFileCount(20)
                .targetFileSizeBytes(256 * 1024 * 1024)
                .minFileSizeBytes(32 * 1024 * 1024)
                .partialProgressEnabled(true)
                .partialProgressMaxCommits(5)
                .maxRewriteBytes(2L * 1024 * 1024 * 1024))
            .add(DeleteOrphanFiles.builder()
                .scheduleOnCommitCount(50)
                .minAge(Duration.ofDays(3)))
            .append();
        
        LOG.info("Table maintenance configured: ExpireSnapshots, RewriteDataFiles, DeleteOrphanFiles");
    }
    
    /**
     * Create JDBC-based lock factory for distributed maintenance coordination.
     * Uses PostgreSQL for lock management across multiple Flink jobs.
     */
    private static TriggerLockFactory createJdbcLockFactory(Map<String, String> config) {
        String jdbcUrl = config.get(RDS_JDBC_URL);
        String user = config.get(RDS_USER);
        String password = config.get(RDS_PASSWORD);
        String lockId = config.getOrDefault(ICEBERG_DATABASE, "iceberg_samples") + "." + 
                        config.getOrDefault(ICEBERG_TABLE, "orders");
        
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException("RDS JDBC URL is required when maintenance is enabled");
        }
        
        LOG.info("Creating JDBC lock factory with URL: {} and lock ID: {}", jdbcUrl, lockId);
        
        Map<String, String> jdbcProperties = new HashMap<>();
        if (user != null && !user.isEmpty()) {
            jdbcProperties.put("jdbc.user", user);
        }
        if (password != null && !password.isEmpty()) {
            jdbcProperties.put("jdbc.password", password);
        }
        // Enable automatic table creation
        jdbcProperties.put("flink-maintenance.lock.jdbc.init-lock-tables", "true");
        
        TriggerLockFactory lockFactory = new JdbcLockFactory(jdbcUrl, lockId, jdbcProperties);
        
        // Open the lock factory to initialize the database tables
        try {
            lockFactory.open();
            LOG.info("JDBC lock factory initialized successfully with auto-table creation");
        } catch (Exception e) {
            LOG.error("Failed to initialize JDBC lock factory", e);
            throw new RuntimeException("Failed to initialize JDBC lock factory", e);
        }
        
        return lockFactory;
    }
    
    /**
     * Create Iceberg schema for OrderEvent table.
     * Schema matches the RowData structure from EventToRowDataConverter.
     */
    private static Schema createOrderEventSchema() {
        return new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "event_type", Types.StringType.get()),
            Types.NestedField.required(4, "region", Types.StringType.get()),
            Types.NestedField.required(5, "event_date", Types.DateType.get()),
            Types.NestedField.required(6, "order_id", Types.StringType.get()),
            Types.NestedField.required(7, "customer_id", Types.StringType.get()),
            Types.NestedField.required(8, "amount", Types.DecimalType.of(18, 2)),
            Types.NestedField.required(9, "currency", Types.StringType.get()),
            Types.NestedField.required(10, "status", Types.StringType.get()),
            Types.NestedField.optional(11, "metadata", Types.MapType.ofOptional(
                12, 13,
                Types.StringType.get(),
                Types.StringType.get()
            ))
        );
    }
}
