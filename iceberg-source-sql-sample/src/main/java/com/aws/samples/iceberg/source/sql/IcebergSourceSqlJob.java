package com.aws.samples.iceberg.source.sql;

import com.aws.samples.iceberg.runtime.AppProperties;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Read an Iceberg table with Flink SQL and republish rows to Kinesis.
 *
 * <p>Features demonstrated: SQL-native Iceberg source configuration, streaming vs batch
 * modes selected via {@code iceberg.source.streaming}, time-travel and branch/tag
 * reads through SQL hints, and Kinesis as a SQL sink.
 *
 * <p><b>Important:</b> streaming reads only support append-only tables. Tables with
 * equality deletes are not supported as streaming sources.
 */
public class IcebergSourceSqlJob {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceSqlJob.class);

    public static void main(String[] args) throws Exception {
        // Load properties up-front; we need the streaming flag before we create the env.
        Properties flinkProps;
        {
            StreamExecutionEnvironment bootstrapEnv = StreamExecutionEnvironment.getExecutionEnvironment();
            flinkProps = AppProperties.load(bootstrapEnv);
        }
        validateConfiguration(flinkProps);

        boolean streaming = Boolean.parseBoolean(
                flinkProps.getProperty("iceberg.source.streaming", "true"));
        StreamExecutionEnvironment env = createExecutionEnvironment(flinkProps, streaming);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        buildPipeline(tableEnv, flinkProps);
    }
    
    private static void buildPipeline(StreamTableEnvironment tableEnv, Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String database = props.getProperty("iceberg.database", "iceberg_samples");
        String tableName = props.getProperty("iceberg.table", "orders");
        String region = props.getProperty("aws.region", "us-east-1");
        boolean streaming = Boolean.parseBoolean(props.getProperty("iceberg.source.streaming", "true"));
        String sinkStreamArn = props.getProperty("kinesis.sink.stream.arn");
        
        LOG.info("Building Iceberg Source SQL pipeline: catalog={}, database={}, table={}, streaming={}",
                catalogType, database, tableName, streaming);
        
        // Create Kinesis sink table in default catalog FIRST (before switching to Iceberg catalog)
        createKinesisSinkTable(tableEnv, sinkStreamArn, region);
        
        // Create Iceberg catalog and switch to it
        createIcebergCatalog(tableEnv, catalogType, props);
        
        // Build the query with SQL hints for streaming options
        // Use fully qualified table name for Kinesis sink (default_catalog.default_database.kinesis_sink)
        String sourceQuery = buildSourceQuery(database, tableName, streaming, props);
        
        LOG.info("Executing query: {}", sourceQuery);
        
        // Execute the pipeline - use fully qualified name for sink table
        tableEnv.executeSql(
            "INSERT INTO default_catalog.default_database.kinesis_sink " + sourceQuery
        );
    }
    
    private static void createIcebergCatalog(StreamTableEnvironment tableEnv, String catalogType, Properties props) {
        String region = props.getProperty("aws.region", "us-east-1");
        
        if ("s3tables".equalsIgnoreCase(catalogType)) {
            String tableBucketArn = props.getProperty("s3tables.bucket.arn");
            
            tableEnv.executeSql(String.format(
                "CREATE CATALOG iceberg_catalog WITH (" +
                "  'type' = 'iceberg'," +
                "  'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog'," +
                "  'warehouse' = '%s'," +
                "  's3tables.catalog.client.region' = '%s'" +
                ")",
                tableBucketArn, region
            ));
            
            LOG.info("Created S3 Tables catalog with bucket: {}", tableBucketArn);
        } else {
            String warehouse = props.getProperty("iceberg.warehouse");
            
            tableEnv.executeSql(String.format(
                "CREATE CATALOG iceberg_catalog WITH (" +
                "  'type' = 'iceberg'," +
                "  'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog'," +
                "  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'," +
                "  'warehouse' = '%s'," +
                "  'client.region' = '%s'" +
                ")",
                warehouse, region
            ));
            
            LOG.info("Created Glue catalog with warehouse: {}", warehouse);
        }
        
        tableEnv.useCatalog("iceberg_catalog");
    }
    
    private static void createKinesisSinkTable(StreamTableEnvironment tableEnv, String streamArn, String region) {
        // Create Kinesis sink table that accepts JSON - use stream.arn for sink connector
        tableEnv.executeSql(String.format(
            "CREATE TABLE kinesis_sink (" +
            "  event_id STRING," +
            "  event_time STRING," +
            "  event_type STRING," +
            "  event_date STRING," +
            "  region STRING," +
            "  order_id STRING," +
            "  customer_id STRING," +
            "  amount DOUBLE," +
            "  currency STRING," +
            "  status STRING" +
            ") WITH (" +
            "  'connector' = 'kinesis'," +
            "  'stream.arn' = '%s'," +
            "  'aws.region' = '%s'," +
            "  'format' = 'json'," +
            "  'json.timestamp-format.standard' = 'ISO-8601'" +
            ")",
            streamArn, region
        ));
        
        LOG.info("Created Kinesis sink table for stream: {}", streamArn);
    }
    
    private static String buildSourceQuery(String database, String tableName, boolean streaming, Properties props) {
        StringBuilder query = new StringBuilder();
        query.append("SELECT ");
        query.append("  CAST(event_id AS STRING) as event_id, ");
        query.append("  CAST(event_time AS STRING) as event_time, ");
        query.append("  CAST(event_type AS STRING) as event_type, ");
        query.append("  CAST(event_date AS STRING) as event_date, ");
        query.append("  CAST(region AS STRING) as region, ");
        query.append("  CAST(order_id AS STRING) as order_id, ");
        query.append("  CAST(customer_id AS STRING) as customer_id, ");
        query.append("  CAST(amount AS DOUBLE) as amount, ");
        query.append("  CAST(currency AS STRING) as currency, ");
        query.append("  CAST(status AS STRING) as status ");
        query.append("FROM ");
        query.append(database).append(".").append(tableName);
        
        // Add SQL hints for streaming configuration
        if (streaming) {
            String monitorInterval = props.getProperty("iceberg.source.monitor-interval", "60s");
            
            query.append(" /*+ OPTIONS(");
            query.append("'streaming' = 'true', ");
            query.append("'monitor-interval' = '").append(monitorInterval).append("'");
            
            // Add starting snapshot id if specified
            String startSnapshotId = props.getProperty("iceberg.source.start-snapshot-id");
            if (startSnapshotId != null && !startSnapshotId.isEmpty()) {
                query.append(", 'start-snapshot-id' = '").append(startSnapshotId).append("'");
            }
            
            query.append(") */");
        } else {
            // Batch mode - optional time travel
            String snapshotId = props.getProperty("iceberg.source.snapshot-id");
            String asOfTimestamp = props.getProperty("iceberg.source.as-of-timestamp");
            String branch = props.getProperty("iceberg.source.branch");
            String tag = props.getProperty("iceberg.source.tag");
            
            StringBuilder hints = new StringBuilder();
            boolean hasHints = false;
            
            if (snapshotId != null && !snapshotId.isEmpty()) {
                hints.append("'snapshot-id' = '").append(snapshotId).append("'");
                hasHints = true;
            }
            if (asOfTimestamp != null && !asOfTimestamp.isEmpty()) {
                if (hasHints) hints.append(", ");
                hints.append("'as-of-timestamp' = '").append(asOfTimestamp).append("'");
                hasHints = true;
            }
            if (branch != null && !branch.isEmpty()) {
                if (hasHints) hints.append(", ");
                hints.append("'branch' = '").append(branch).append("'");
                hasHints = true;
            }
            if (tag != null && !tag.isEmpty()) {
                if (hasHints) hints.append(", ");
                hints.append("'tag' = '").append(tag).append("'");
                hasHints = true;
            }
            
            if (hasHints) {
                query.append(" /*+ OPTIONS(").append(hints).append(") */");
            }
        }
        
        return query.toString();
    }
    
    /**
     * Example: Query Iceberg metadata tables
     */
    public static void queryMetadataTables(StreamTableEnvironment tableEnv, String database, String tableName) {
        // Query snapshots
        LOG.info("Querying snapshots for {}.{}", database, tableName);
        tableEnv.executeSql(String.format(
            "SELECT snapshot_id, committed_at, operation, summary " +
            "FROM %s.%s$snapshots " +
            "ORDER BY committed_at DESC LIMIT 10",
            database, tableName
        )).print();
        
        // Query history
        LOG.info("Querying history for {}.{}", database, tableName);
        tableEnv.executeSql(String.format(
            "SELECT made_current_at, snapshot_id, is_current_ancestor " +
            "FROM %s.%s$history " +
            "ORDER BY made_current_at DESC LIMIT 10",
            database, tableName
        )).print();
        
        // Query files
        LOG.info("Querying files for {}.{}", database, tableName);
        tableEnv.executeSql(String.format(
            "SELECT file_path, file_format, record_count, file_size_in_bytes " +
            "FROM %s.%s$files " +
            "LIMIT 10",
            database, tableName
        )).print();
        
        // Query partitions
        LOG.info("Querying partitions for {}.{}", database, tableName);
        tableEnv.executeSql(String.format(
            "SELECT partition, record_count, file_count " +
            "FROM %s.%s$partitions",
            database, tableName
        )).print();
    }
    
    private static StreamExecutionEnvironment createExecutionEnvironment(Properties props, boolean streaming) {
        Configuration config = new Configuration();
        
        // Set runtime mode BEFORE creating environment
        if (streaming) {
            config.setString("execution.runtime-mode", "STREAMING");
        } else {
            config.setString("execution.runtime-mode", "BATCH");
        }
        
        // Note: RestOptions.PORT is NOT set here because Managed Flink rejects it.
        // For local dev, pass -Drest.port=8085 as a JVM argument if needed.
        
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
}
