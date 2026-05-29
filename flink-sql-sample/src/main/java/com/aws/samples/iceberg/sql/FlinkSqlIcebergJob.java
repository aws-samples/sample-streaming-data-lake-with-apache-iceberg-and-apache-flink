package com.aws.samples.iceberg.sql;

import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Flink SQL sample demonstrating Iceberg table writes with Glue Catalog or S3 Tables.
 *
 * <p>Features: creating an Iceberg catalog via SQL DDL, reading Kinesis via SQL,
 * writing to Iceberg v2 tables, optional UPSERT with PK enforcement, SQL hints for
 * embedded maintenance via flink-maintenance.* options (compaction, snapshot expiration, orphan cleanup).
 *
 * <p>Configuration keys (via {@code FlinkApplicationProperties}):
 * <ul>
 *   <li>{@code kinesis.stream.arn} — Kinesis stream ARN (required)
 *   <li>{@code aws.region} — AWS region (default: {@code us-east-1})
 *   <li>{@code s3.warehouse.path} — S3 warehouse for Glue catalog
 *   <li>{@code s3tables.bucket.arn} — required for {@code iceberg.catalog.type=s3tables}
 *   <li>{@code glue.database} — target database (default: {@code iceberg_samples})
 *   <li>{@code table.prefix} — table name prefix (default: {@code sql_})
 *   <li>{@code iceberg.catalog.type} — {@code glue} (default) or {@code s3tables}
 *   <li>{@code write.mode} — {@code append} (default) or {@code upsert}
 *   <li>{@code primary.key.columns} — CSV list (default: {@code event_id,event_date,region})
 *   <li>{@code enable.maintenance} — enables SQL-embedded compaction, snapshot expiration, and orphan cleanup via Coordinator Lock
 * </ul>
 */
public class FlinkSqlIcebergJob {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkSqlIcebergJob.class);
    private static final int LOCAL_WEB_UI_PORT = 8084;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Properties props = AppProperties.load(env);
        
        String kinesisStreamArn = props.getProperty("kinesis.stream.arn",
                "arn:aws:kinesis:us-east-1:123456789012:stream/iceberg-source");
        String awsRegion = props.getProperty("aws.region", "us-east-1");
        String s3WarehousePath = props.getProperty("s3.warehouse.path",
                "s3://iceberg-warehouse-123456789012/warehouse/");
        String glueDatabase = props.getProperty("glue.database", "iceberg_samples");
        String tablePrefix = props.getProperty("table.prefix", "sql_");
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String s3TableBucketArn = props.getProperty("s3tables.bucket.arn", "");
        boolean enableMaintenance = Boolean.parseBoolean(props.getProperty("enable.maintenance", "false"));
        String writeMode = props.getProperty("write.mode", "append");  // "append" or "upsert"
        String primaryKeyColumns = props.getProperty("primary.key.columns", "event_id,event_date,region");
        boolean isUpsertMode = "upsert".equalsIgnoreCase(writeMode);

        LOG.info("Starting Flink SQL Iceberg Job");
        LOG.info("Kinesis Stream ARN: {}", kinesisStreamArn);
        LOG.info("AWS Region: {}", awsRegion);
        LOG.info("Catalog Type: {}", catalogType);
        LOG.info("Write Mode: {}", writeMode);
        if (isUpsertMode) {
            LOG.info("Primary Key Columns: {}", primaryKeyColumns);
        }
        if ("s3tables".equals(catalogType)) {
            LOG.info("S3 Table Bucket ARN: {}", s3TableBucketArn);
        } else {
            LOG.info("S3 Warehouse: {}", s3WarehousePath);
        }
        LOG.info("Database: {}", glueDatabase);
        
        if ("s3tables".equals(catalogType) && enableMaintenance) {
            LOG.warn("S3 Tables handles maintenance automatically - ignoring enable.maintenance setting");
            enableMaintenance = false;
        }
        
        
        // Configure checkpointing for local development only.
        // AWS Managed Flink configures checkpointing for us on the service side.
        if (FlinkEnvironments.isLocal(env)) {
            long interval = Long.parseLong(props.getProperty(
                    "checkpoint.interval.ms", Long.toString(Checkpointing.DEFAULT_INTERVAL_MS)));
            Checkpointing.configureLocalDefaults(env, interval);
        } else {
            LOG.info("Running on AWS Managed Flink — checkpointing configured by the service");
        }
        
        // Create Table Environment
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
            .inStreamingMode()
            .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        
        // Use SinkV2 (IcebergSink) for proper Java 17 checkpoint serialization.
        // On Managed Flink this must be set via runtime properties (table.exec.iceberg.use-v2-sink).
        if (FlinkEnvironments.isLocal(env)) {
            tableEnv.getConfig().set("table.exec.iceberg.use-v2-sink", "true");
        }
        
        LOG.info("Table environment created");

        // SQL-embedded maintenance (Iceberg 1.11.0+): compaction, expire, orphan cleanup.
        // MSF NOTE: flink-maintenance.* must be set as TABLE PROPERTIES (read by IcebergSink ->
        // FlinkMaintenanceConfig.writeProperties), NOT via tableEnv config — MSF rejects those
        // config mutations (MutatedConfigurationException). use-v2-sink (a table.exec option MSF
        // allows) is required so the SinkV2 IcebergSink, which runs maintenance, is used.
        String maintenanceProps = "";
        if (enableMaintenance && !"s3tables".equalsIgnoreCase(catalogType)) {
            LOG.info("Enabling SQL maintenance via table properties (Coordinator Lock, no JDBC)");
            tableEnv.getConfig().set("table.exec.iceberg.use-v2-sink", "true");
            maintenanceProps =
                "    'flink-maintenance.rewrite.enabled' = 'true',\n" +
                "    'flink-maintenance.expire-snapshots.enabled' = 'true',\n" +
                "    'flink-maintenance.delete-orphan-files.enabled' = 'true',\n" +
                "    'flink-maintenance.lock.type' = '',\n" +
                "    'flink-maintenance.rewrite.schedule.commit-count' = '10',\n" +
                "    'flink-maintenance.expire-snapshots.schedule.commit-count' = '50',\n" +
                "    'flink-maintenance.expire-snapshots.retain-last' = '5',\n";
        }
        
        // Create Iceberg catalog using SQL DDL - supports Glue Catalog or S3 Tables
        String catalogName;
        String createCatalogSql;
        
        if ("s3tables".equals(catalogType)) {
            // S3 Tables Catalog
            if (s3TableBucketArn == null || s3TableBucketArn.isEmpty()) {
                throw new IllegalArgumentException("S3 Table Bucket ARN is required when using S3 Tables catalog");
            }
            
            catalogName = "s3tables_catalog";
            createCatalogSql = "CREATE CATALOG " + catalogName + " WITH (\n" +
                "    'type' = 'iceberg',\n" +
                "    'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',\n" +
                "    'warehouse' = '" + s3TableBucketArn + "',\n" +
                "    'client.region' = '" + awsRegion + "'\n" +
                ")";
            
            LOG.info("Creating Iceberg catalog with S3 Tables");
        } else {
            // Glue Catalog (default)
            catalogName = "glue_catalog";
            createCatalogSql = "CREATE CATALOG " + catalogName + " WITH (\n" +
                "    'type' = 'iceberg',\n" +
                "    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',\n" +
                "    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n" +
                "    'warehouse' = '" + s3WarehousePath + "',\n" +
                "    'glue.skip-archive' = 'true',\n" +
                "    'glue.skip-name-validation' = 'true'\n" +
                ")";
            
            LOG.info("Creating Iceberg catalog with Glue");
        }
        
        tableEnv.executeSql(createCatalogSql);
        LOG.info("Iceberg catalog '{}' created successfully", catalogName);
        
        // Create Kinesis source table in DEFAULT catalog (before switching to glue_catalog)
        // Kinesis tables cannot be created in Iceberg/Glue catalog
        LOG.info("Creating Kinesis source table in default catalog");
        String createKinesisSourceSql = "CREATE TABLE kinesis_source (\n" +
            "    event_id STRING,\n" +
            "    event_time STRING,\n" +
            "    event_type STRING,\n" +
            "    region STRING,\n" +
            "    event_date STRING,\n" +
            "    metadata MAP<STRING, STRING>,\n" +
            "    order_id STRING,\n" +
            "    customer_id STRING,\n" +
            "    amount DOUBLE,\n" +
            "    currency STRING,\n" +
            "    status STRING,\n" +
            "    user_id STRING,\n" +
            "    action STRING,\n" +
            "    device_type STRING,\n" +
            "    ip_address STRING,\n" +
            "    user_agent STRING,\n" +
            "    session_id STRING,\n" +
            "    page_url STRING,\n" +
            "    referrer STRING,\n" +
            "    scroll_depth INT,\n" +
            "    time_on_page_seconds BIGINT\n" +
            ") WITH (\n" +
            "    'connector' = 'kinesis',\n" +
            "    'stream.arn' = '" + kinesisStreamArn + "',\n" +
            "    'aws.region' = '" + awsRegion + "',\n" +
            "    'source.init.position' = 'LATEST',\n" +
            "    'format' = 'json',\n" +
            "    'json.fail-on-missing-field' = 'false',\n" +
            "    'json.ignore-parse-errors' = 'true'\n" +
            ")";
        
        tableEnv.executeSql(createKinesisSourceSql);
        LOG.info("Kinesis source table 'kinesis_source' created successfully in default catalog");
        
        // NOW switch to the Iceberg catalog for Iceberg tables
        tableEnv.executeSql("USE CATALOG " + catalogName);
        LOG.info("Switched to catalog: {}", catalogName);
        
        // Create database/namespace if it doesn't exist
        String createDatabaseSql = String.format(
            "CREATE DATABASE IF NOT EXISTS %s.%s", catalogName, glueDatabase);
        tableEnv.executeSql(createDatabaseSql);
        LOG.info("Database '{}' created or already exists", glueDatabase);
        
        tableEnv.executeSql(String.format("USE %s", glueDatabase));
        LOG.info("Using database: {}", glueDatabase);
        
        // Create Iceberg tables for each event type with v2 format and delete vectors
        createOrderEventsTable(tableEnv, glueDatabase, tablePrefix, isUpsertMode, maintenanceProps);
        createUserEventsTable(tableEnv, glueDatabase, tablePrefix, isUpsertMode, maintenanceProps);
        createClickEventsTable(tableEnv, glueDatabase, tablePrefix, isUpsertMode, maintenanceProps);
        
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
            "    TO_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),\n" +
            "    event_type,\n" +
            "    region,\n" +
            "    TO_DATE(event_date, 'yyyy-MM-dd'),\n" +
            "    order_id,\n" +
            "    customer_id,\n" +
            "    CAST(amount AS DECIMAL(18, 2)),\n" +
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
            "    TO_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),\n" +
            "    event_type,\n" +
            "    region,\n" +
            "    TO_DATE(event_date, 'yyyy-MM-dd'),\n" +
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
            "    TO_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),\n" +
            "    event_type,\n" +
            "    region,\n" +
            "    TO_DATE(event_date, 'yyyy-MM-dd'),\n" +
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
     * Create the orders Iceberg table with v2 format.
     * Partitioned by event_date and region for efficient querying.
     * When upsert mode is enabled, adds PRIMARY KEY constraint for deduplication.
     */
    private static void createOrderEventsTable(StreamTableEnvironment tableEnv, String database, String tablePrefix, boolean isUpsertMode, String maintenanceProps) {
        String primaryKeyClause = isUpsertMode ? 
            ",\n    PRIMARY KEY (event_id, event_date, region) NOT ENFORCED\n" : "\n";
        
        String upsertProperties = isUpsertMode ?
            "    'write.upsert.enabled' = 'true',\n" +
            "    'write.delete.mode' = 'merge-on-read',\n" +
            "    'write.update.mode' = 'merge-on-read',\n" +
            "    'write.merge.mode' = 'merge-on-read',\n" : "";
        
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
            "    metadata MAP<STRING, STRING>" +
            primaryKeyClause +
            ") PARTITIONED BY (event_date, region)\n" +
            "WITH (\n" +
            "    'format-version' = '3',\n" +
            "    'write.format.default' = 'parquet',\n" +
            "    'write.parquet.compression-codec' = 'snappy',\n" +
            upsertProperties +
            maintenanceProps +
            "    'write.target-file-size-bytes' = '134217728'\n" +
            ")";
        
        LOG.info("Creating {}orders table (upsert mode: {})", tablePrefix, isUpsertMode);
        tableEnv.executeSql(createTableSql);
        LOG.info("{}orders table created successfully", tablePrefix);
    }
    
    /**
     * Create the users Iceberg table with v2 format.
     * Partitioned by event_date and region for efficient querying.
     * When upsert mode is enabled, adds PRIMARY KEY constraint for deduplication.
     */
    private static void createUserEventsTable(StreamTableEnvironment tableEnv, String database, String tablePrefix, boolean isUpsertMode, String maintenanceProps) {
        String primaryKeyClause = isUpsertMode ? 
            ",\n    PRIMARY KEY (event_id, event_date, region) NOT ENFORCED\n" : "\n";
        
        String upsertProperties = isUpsertMode ?
            "    'write.upsert.enabled' = 'true',\n" +
            "    'write.delete.mode' = 'merge-on-read',\n" +
            "    'write.update.mode' = 'merge-on-read',\n" +
            "    'write.merge.mode' = 'merge-on-read',\n" : "";
        
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
            "    metadata MAP<STRING, STRING>" +
            primaryKeyClause +
            ") PARTITIONED BY (event_date, region)\n" +
            "WITH (\n" +
            "    'format-version' = '3',\n" +
            "    'write.format.default' = 'parquet',\n" +
            "    'write.parquet.compression-codec' = 'snappy',\n" +
            upsertProperties +
            maintenanceProps +
            "    'write.target-file-size-bytes' = '134217728'\n" +
            ")";
        
        LOG.info("Creating {}users table (upsert mode: {})", tablePrefix, isUpsertMode);
        tableEnv.executeSql(createTableSql);
        LOG.info("{}users table created successfully", tablePrefix);
    }
    
    /**
     * Create the clicks Iceberg table with v2 format.
     * Partitioned by event_date and region for efficient querying.
     * When upsert mode is enabled, adds PRIMARY KEY constraint for deduplication.
     */
    private static void createClickEventsTable(StreamTableEnvironment tableEnv, String database, String tablePrefix, boolean isUpsertMode, String maintenanceProps) {
        String primaryKeyClause = isUpsertMode ? 
            ",\n    PRIMARY KEY (event_id, event_date, region) NOT ENFORCED\n" : "\n";
        
        String upsertProperties = isUpsertMode ?
            "    'write.upsert.enabled' = 'true',\n" +
            "    'write.delete.mode' = 'merge-on-read',\n" +
            "    'write.update.mode' = 'merge-on-read',\n" +
            "    'write.merge.mode' = 'merge-on-read',\n" : "";
        
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
            "    metadata MAP<STRING, STRING>" +
            primaryKeyClause +
            ") PARTITIONED BY (event_date, region)\n" +
            "WITH (\n" +
            "    'format-version' = '3',\n" +
            "    'write.format.default' = 'parquet',\n" +
            "    'write.parquet.compression-codec' = 'snappy',\n" +
            upsertProperties +
            maintenanceProps +
            "    'write.target-file-size-bytes' = '134217728'\n" +
            ")";
        
        LOG.info("Creating {}clicks table (upsert mode: {})", tablePrefix, isUpsertMode);
        tableEnv.executeSql(createTableSql);
        LOG.info("{}clicks table created successfully", tablePrefix);
    }
    
}
