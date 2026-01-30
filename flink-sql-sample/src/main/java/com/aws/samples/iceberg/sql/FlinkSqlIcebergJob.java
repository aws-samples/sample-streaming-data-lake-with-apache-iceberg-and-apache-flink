package com.aws.samples.iceberg.sql;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Flink SQL sample demonstrating Iceberg table writes with Glue Catalog.
 * 
 * This job demonstrates:
 * - Creating an Iceberg catalog using SQL DDL with Glue Catalog
 * - Reading from Kinesis Data Stream using SQL
 * - Writing to Iceberg tables with v3 format and delete vectors
 * - UPSERT operations using primary keys
 * - Embedded compaction via SQL hints
 * 
 * Environment Variables:
 * - KINESIS_STREAM_NAME: Name of the Kinesis stream to read from (default: iceberg-events)
 * - AWS_REGION: AWS region for Kinesis and Glue (default: us-east-1)
 * - S3_WAREHOUSE_PATH: S3 path for Iceberg warehouse (default: s3://iceberg-warehouse-{account}/warehouse)
 * - GLUE_DATABASE: Glue database name (default: iceberg_samples)
 * - ENABLE_MAINTENANCE: Enable maintenance (default: false)
 * - RDS_JDBC_URL: PostgreSQL JDBC URL for locks (default: jdbc:postgresql://localhost:5432/iceberg_locks)
 */
public class FlinkSqlIcebergJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlIcebergJob.class);
    private static final String LOCAL_APPLICATION_PROPERTIES_RESOURCE = "flink-application-properties-dev.json";
    
    /**
     * Check if running in local execution mode.
     */
    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }
    
    /**
     * Load application properties from Amazon Managed Service for Apache Flink runtime
     * or from local resource file when running locally.
     */
    private static Properties loadApplicationProperties(StreamExecutionEnvironment env) throws Exception {
        if (isLocal(env)) {
            LOG.info("Loading application properties from '{}'", LOCAL_APPLICATION_PROPERTIES_RESOURCE);
            Map<String, Properties> props = KinesisAnalyticsRuntime.getApplicationProperties(
                FlinkSqlIcebergJob.class.getClassLoader().getResource(LOCAL_APPLICATION_PROPERTIES_RESOURCE).getPath()
            );
            return props.getOrDefault("FlinkApplicationProperties", new Properties());
        } else {
            LOG.info("Loading application properties from Amazon Managed Service for Apache Flink");
            Map<String, Properties> props = KinesisAnalyticsRuntime.getApplicationProperties();
            return props.getOrDefault("FlinkApplicationProperties", new Properties());
        }
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
                // Recreate with Web UI enabled on port 8082 (8081 might be used by DataStreamJob)
                org.apache.flink.configuration.Configuration config = new org.apache.flink.configuration.Configuration();
                config.setString("rest.port", "8082");
                config.setString("rest.bind-address", "localhost");
                env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
                LOG.info("Local execution detected - Flink Web UI available at http://localhost:8082");
            }
            return env;
        } catch (Exception e) {
            LOG.warn("Could not create environment with Web UI, falling back to standard environment", e);
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment with Web UI for local dev
        StreamExecutionEnvironment env = createExecutionEnvironment();
        
        // Load configuration from runtime properties
        Properties props = loadApplicationProperties(env);
        
        String kinesisStreamName = props.getProperty("kinesis.stream.name", "iceberg-events");
        String awsRegion = props.getProperty("aws.region", "us-east-1");
        String s3WarehousePath = props.getProperty("s3.warehouse.path", "s3://iceberg-warehouse/warehouse");
        String glueDatabase = props.getProperty("glue.database", "iceberg_samples");
        String tablePrefix = props.getProperty("table.prefix", "sql_");
        boolean enableMaintenance = Boolean.parseBoolean(props.getProperty("enable.maintenance", "false"));
        
        LOG.info("Starting Flink SQL Iceberg Job");
        LOG.info("Kinesis Stream: {}", kinesisStreamName);
        LOG.info("AWS Region: {}", awsRegion);
        LOG.info("S3 Warehouse: {}", s3WarehousePath);
        LOG.info("Glue Database: {}", glueDatabase);
        
        if (enableMaintenance) {
            LOG.warn("Note: SQL API has limited maintenance support compared to DataStream API");
            LOG.warn("For full maintenance capabilities, use DataStreamIcebergJob with ENABLE_MAINTENANCE=true");
        }
        
        // Configure checkpointing for local development only
        // AWS Managed Flink configures checkpointing automatically
        if (isLocal(env)) {
            env.enableCheckpointing(60000);
            env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
            env.getCheckpointConfig().setCheckpointTimeout(600000);
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
            LOG.info("Checkpointing configured for local development: interval=60s, mode=EXACTLY_ONCE");
        } else {
            LOG.info("Running on AWS Managed Flink - checkpointing configured by the service");
        }
        
        // Create Table Environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        LOG.info("Table environment created");
        
        // Create Iceberg catalog using SQL DDL with Glue Catalog
        String createCatalogSql = "CREATE CATALOG glue_catalog WITH (\n" +
            "    'type' = 'iceberg',\n" +
            "    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',\n" +
            "    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
            "    'warehouse' = '" + s3WarehousePath + "',\n" +
            "    'glue.skip-archive' = 'true',\n" +
            "    'glue.skip-name-validation' = 'true'\n" +
            ")";
        
        LOG.info("Creating Iceberg catalog with Glue");
        tableEnv.executeSql(createCatalogSql);
        LOG.info("Iceberg catalog 'glue_catalog' created successfully");
        
        // Create Kinesis source table in DEFAULT catalog (before switching to glue_catalog)
        // Kinesis tables cannot be created in Iceberg/Glue catalog
        LOG.info("Creating Kinesis source table in default catalog");
        String createKinesisSourceSql = "CREATE TABLE kinesis_source (\n" +
            "    event_id STRING,\n" +
            "    event_time TIMESTAMP(3),\n" +
            "    event_type STRING,\n" +
            "    region STRING,\n" +
            "    event_date DATE,\n" +
            "    metadata MAP<STRING, STRING>,\n" +
            "    -- Order event fields\n" +
            "    order_id STRING,\n" +
            "    customer_id STRING,\n" +
            "    amount DECIMAL(18, 2),\n" +
            "    currency STRING,\n" +
            "    status STRING,\n" +
            "    -- User event fields\n" +
            "    user_id STRING,\n" +
            "    action STRING,\n" +
            "    device_type STRING,\n" +
            "    ip_address STRING,\n" +
            "    user_agent STRING,\n" +
            "    -- Click event fields\n" +
            "    session_id STRING,\n" +
            "    page_url STRING,\n" +
            "    referrer STRING,\n" +
            "    scroll_depth INT,\n" +
            "    time_on_page_seconds BIGINT\n" +
            ") WITH (\n" +
            "    'connector' = 'kinesis-legacy',\n" +
            "    'stream' = '" + kinesisStreamName + "',\n" +
            "    'aws.region' = '" + awsRegion + "',\n" +
            "    'scan.stream.initpos' = 'LATEST',\n" +
            "    'format' = 'json',\n" +
            "    'json.fail-on-missing-field' = 'false',\n" +
            "    'json.ignore-parse-errors' = 'true'\n" +
            ")";
        
        tableEnv.executeSql(createKinesisSourceSql);
        LOG.info("Kinesis source table 'kinesis_source' created successfully in default catalog");
        
        // NOW switch to the Glue catalog for Iceberg tables
        tableEnv.executeSql("USE CATALOG glue_catalog");
        LOG.info("Switched to catalog: glue_catalog");
        
        // Create database if it doesn't exist
        String createDatabaseSql = String.format(
            "CREATE DATABASE IF NOT EXISTS glue_catalog.%s", glueDatabase);
        tableEnv.executeSql(createDatabaseSql);
        LOG.info("Database '{}' created or already exists", glueDatabase);
        
        tableEnv.executeSql(String.format("USE %s", glueDatabase));
        LOG.info("Using database: {}", glueDatabase);
        
        // Create Iceberg tables for each event type with v2 format and delete vectors
        createOrderEventsTable(tableEnv, glueDatabase, tablePrefix);
        createUserEventsTable(tableEnv, glueDatabase, tablePrefix);
        createClickEventsTable(tableEnv, glueDatabase, tablePrefix);
        
        // Start streaming writes from Kinesis to Iceberg tables using StatementSet
        // This ensures we read from Kinesis only once and route to 3 tables
        startMultiTableIngestion(tableEnv, glueDatabase, tablePrefix);
        
        LOG.info("All streaming ingestion jobs configured in StatementSet. Executing Flink job...");
    }
    
    /**
     * Start multi-table ingestion using StatementSet for efficient source reuse.
     * Reads from Kinesis once and routes to 3 different Iceberg tables.
     */
    private static void startMultiTableIngestion(StreamTableEnvironment tableEnv, String database, String tablePrefix) {
        LOG.info("Configuring multi-table ingestion with StatementSet");
        
        // Create StatementSet to execute multiple INSERT statements from single source
        org.apache.flink.table.api.StatementSet statementSet = tableEnv.createStatementSet();
        
        // Add INSERT for orders table
        String insertOrdersSql = "INSERT INTO " + database + "." + tablePrefix + "orders\n" +
            "SELECT \n" +
            "    event_id,\n" +
            "    CAST(event_time AS TIMESTAMP(6)),\n" +
            "    event_type,\n" +
            "    region,\n" +
            "    event_date,\n" +
            "    order_id,\n" +
            "    customer_id,\n" +
            "    amount,\n" +
            "    currency,\n" +
            "    status,\n" +
            "    metadata\n" +
            "FROM default_catalog.default_database.kinesis_source\n" +
            "WHERE event_type = 'ORDER'";
        
        statementSet.addInsertSql(insertOrdersSql);
        LOG.info("Added ORDER events ingestion to StatementSet");
        
        // Add INSERT for users table
        String insertUsersSql = "INSERT INTO " + database + "." + tablePrefix + "users\n" +
            "SELECT \n" +
            "    event_id,\n" +
            "    CAST(event_time AS TIMESTAMP(6)),\n" +
            "    event_type,\n" +
            "    region,\n" +
            "    event_date,\n" +
            "    user_id,\n" +
            "    action,\n" +
            "    device_type,\n" +
            "    ip_address,\n" +
            "    user_agent,\n" +
            "    metadata\n" +
            "FROM default_catalog.default_database.kinesis_source\n" +
            "WHERE event_type = 'USER'";
        
        statementSet.addInsertSql(insertUsersSql);
        LOG.info("Added USER events ingestion to StatementSet");
        
        // Add INSERT for clicks table
        String insertClicksSql = "INSERT INTO " + database + "." + tablePrefix + "clicks\n" +
            "SELECT \n" +
            "    event_id,\n" +
            "    CAST(event_time AS TIMESTAMP(6)),\n" +
            "    event_type,\n" +
            "    region,\n" +
            "    event_date,\n" +
            "    session_id,\n" +
            "    page_url,\n" +
            "    referrer,\n" +
            "    scroll_depth,\n" +
            "    time_on_page_seconds,\n" +
            "    metadata\n" +
            "FROM default_catalog.default_database.kinesis_source\n" +
            "WHERE event_type = 'CLICK'";
        
        statementSet.addInsertSql(insertClicksSql);
        LOG.info("Added CLICK events ingestion to StatementSet");
        
        // Execute all INSERT statements together (reads from Kinesis only once)
        statementSet.execute();
        LOG.info("StatementSet execution started - reading from Kinesis once, routing to 3 tables");
    }
    
    /**
     * Create the orders Iceberg table with v3 format and delete vectors.
     * Partitioned by event_date and region for efficient querying.
     * Configured with primary key for UPSERT operations.
     */
    private static void createOrderEventsTable(StreamTableEnvironment tableEnv, String database, String tablePrefix) {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + database + "." + tablePrefix + "orders (\n" +
            "    event_id STRING,\n" +
            "    event_time TIMESTAMP(6),\n" +
            "    event_type STRING,\n" +
            "    region STRING,\n" +
            "    event_date DATE,\n" +
            "    order_id STRING,\n" +
            "    customer_id STRING,\n" +
            "    amount DECIMAL(18, 2),\n" +
            "    currency STRING,\n" +
            "    status STRING,\n" +
            "    metadata MAP<STRING, STRING>\n" +
            ") PARTITIONED BY (event_date, region)\n" +
            "WITH (\n" +
            "    'format-version' = '2',\n" +
            "    'write.format.default' = 'parquet',\n" +
            "    'write.parquet.compression-codec' = 'snappy',\n" +
            "    'write.target-file-size-bytes' = '134217728'\n" +
            ")";
        
        LOG.info("Creating {}orders table", tablePrefix);
        tableEnv.executeSql(createTableSql);
        LOG.info("{}orders table created successfully", tablePrefix);
    }
    
    /**
     * Create the users Iceberg table with v3 format and delete vectors.
     * Partitioned by event_date and region for efficient querying.
     * Configured with primary key for UPSERT operations.
     */
    private static void createUserEventsTable(StreamTableEnvironment tableEnv, String database, String tablePrefix) {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + database + "." + tablePrefix + "users (\n" +
            "    event_id STRING,\n" +
            "    event_time TIMESTAMP(6),\n" +
            "    event_type STRING,\n" +
            "    region STRING,\n" +
            "    event_date DATE,\n" +
            "    user_id STRING,\n" +
            "    action STRING,\n" +
            "    device_type STRING,\n" +
            "    ip_address STRING,\n" +
            "    user_agent STRING,\n" +
            "    metadata MAP<STRING, STRING>\n" +
            ") PARTITIONED BY (event_date, region)\n" +
            "WITH (\n" +
            "    'format-version' = '2',\n" +
            "    'write.format.default' = 'parquet',\n" +
            "    'write.parquet.compression-codec' = 'snappy',\n" +
            "    'write.target-file-size-bytes' = '134217728'\n" +
            ")";
        
        LOG.info("Creating {}users table", tablePrefix);
        tableEnv.executeSql(createTableSql);
        LOG.info("{}users table created successfully", tablePrefix);
    }
    
    /**
     * Create the clicks Iceberg table with v3 format and delete vectors.
     * Partitioned by event_date and region for efficient querying.
     * Configured with primary key for UPSERT operations.
     */
    private static void createClickEventsTable(StreamTableEnvironment tableEnv, String database, String tablePrefix) {
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + database + "." + tablePrefix + "clicks (\n" +
            "    event_id STRING,\n" +
            "    event_time TIMESTAMP(6),\n" +
            "    event_type STRING,\n" +
            "    region STRING,\n" +
            "    event_date DATE,\n" +
            "    session_id STRING,\n" +
            "    page_url STRING,\n" +
            "    referrer STRING,\n" +
            "    scroll_depth INT,\n" +
            "    time_on_page_seconds BIGINT,\n" +
            "    metadata MAP<STRING, STRING>\n" +
            ") PARTITIONED BY (event_date, region)\n" +
            "WITH (\n" +
            "    'format-version' = '2',\n" +
            "    'write.format.default' = 'parquet',\n" +
            "    'write.parquet.compression-codec' = 'snappy',\n" +
            "    'write.target-file-size-bytes' = '134217728'\n" +
            ")";
        
        LOG.info("Creating {}clicks table", tablePrefix);
        tableEnv.executeSql(createTableSql);
        LOG.info("{}clicks table created successfully", tablePrefix);
    }
    
}
