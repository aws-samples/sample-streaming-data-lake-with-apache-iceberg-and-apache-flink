package com.aws.samples.iceberg.sqldynamic;

import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Flink SQL writing to MANY Iceberg tables through ONE dynamic sink (Iceberg 1.11+).
 *
 * <p>The sink is a regular {@code 'connector'='iceberg'} table with two extra options:
 * {@code use-dynamic-iceberg-sink=true} and {@code dynamic-record-generator-impl} pointing at
 * {@link SqlRoutingGenerator}. A single {@code INSERT INTO} then fans records out per
 * {@code event_type}, creating and evolving the target tables on the fly — the SQL counterpart
 * of the DataStream {@code dynamic-sink-sample}.
 *
 * <p>NOTE: SQL-embedded maintenance ({@code flink-maintenance.*}) is NOT available on the
 * dynamic sink path. {@code IcebergTableSink} short-circuits to {@code DynamicIcebergSink}
 * before the maintenance topology is built (verified against Iceberg 1.11.0 source). Use Glue
 * auto-compaction, S3 Tables, or a separate maintenance job for the routed tables.
 *
 * <p>Configuration keys (via {@code FlinkApplicationProperties}):
 * <ul>
 *   <li>{@code kinesis.stream.arn} — source stream (required)
 *   <li>{@code aws.region} / {@code kinesis.region}
 *   <li>{@code iceberg.catalog.type} — {@code glue} (default) or {@code s3tables}
 *   <li>{@code iceberg.warehouse} — S3 warehouse path (Glue)
 *   <li>{@code s3tables.bucket.arn} — table bucket ARN (S3 Tables)
 *   <li>{@code enable.maintenance} — accepted for CDK parity; logs a warning and sets the
 *       {@code flink-maintenance.*} options so the (non-)effect on the dynamic path is observable
 * </ul>
 */
public final class SqlDynamicSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(SqlDynamicSinkJob.class);
    private static final int LOCAL_WEB_UI_PORT = 8087;

    private SqlDynamicSinkJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Map<String, String> config = AppProperties.loadAsMap(env);

        String streamArn = required(config, "kinesis.stream.arn");
        String awsRegion = config.getOrDefault("aws.region", "us-east-1");
        String catalogType = config.getOrDefault("iceberg.catalog.type", "glue");
        String warehouse = config.getOrDefault("iceberg.warehouse", "");
        String s3TableBucketArn = config.getOrDefault("s3tables.bucket.arn", "");
        boolean enableMaintenance = Boolean.parseBoolean(config.getOrDefault("enable.maintenance", "false"));

        LOG.info("SQL Dynamic Iceberg Sink: catalogType={}, database={}", catalogType, SqlRoutingGenerator.DATABASE);

        if (FlinkEnvironments.isLocal(env)) {
            Checkpointing.configureLocalDefaults(env, Checkpointing.DEFAULT_INTERVAL_MS);
        } else {
            LOG.info("Running on AWS Managed Flink — checkpointing configured by the service");
        }

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(
                env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // --- Kinesis source (default catalog) ---
        tableEnv.executeSql(
                "CREATE TABLE kinesis_source (\n"
                + "    event_id STRING,\n"
                + "    event_time STRING,\n"
                + "    event_type STRING,\n"
                + "    region STRING,\n"
                + "    event_date STRING,\n"
                + "    order_id STRING,\n"
                + "    customer_id STRING,\n"
                + "    amount DOUBLE,\n"
                + "    currency STRING,\n"
                + "    status STRING,\n"
                + "    user_id STRING,\n"
                + "    action STRING,\n"
                + "    device_type STRING,\n"
                + "    ip_address STRING,\n"
                + "    user_agent STRING,\n"
                + "    session_id STRING,\n"
                + "    page_url STRING,\n"
                + "    referrer STRING,\n"
                + "    scroll_depth INT,\n"
                + "    time_on_page_seconds BIGINT\n"
                + ") WITH (\n"
                + "    'connector' = 'kinesis',\n"
                + "    'stream.arn' = '" + streamArn + "',\n"
                + "    'aws.region' = '" + awsRegion + "',\n"
                + "    'source.init.position' = 'LATEST',\n"
                + "    'format' = 'json',\n"
                + "    'json.fail-on-missing-field' = 'false',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")");
        LOG.info("Kinesis source table created");

        // --- Dynamic Iceberg sink (connector table; catalog config inlined in WITH) ---
        String catalogProps;
        if ("s3tables".equalsIgnoreCase(catalogType)) {
            if (s3TableBucketArn.isEmpty()) {
                throw new IllegalArgumentException("s3tables.bucket.arn is required for catalogType=s3tables");
            }
            catalogProps =
                  "    'catalog-name' = 's3tables_catalog',\n"
                + "    'catalog-impl' = 'software.amazon.s3tables.iceberg.S3TablesCatalog',\n"
                + "    'warehouse' = '" + s3TableBucketArn + "',\n"
                + "    'client.region' = '" + awsRegion + "',\n";
        } else {
            if (warehouse.isEmpty()) {
                throw new IllegalArgumentException("iceberg.warehouse is required for catalogType=glue");
            }
            catalogProps =
                  "    'catalog-name' = 'glue_catalog',\n"
                + "    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',\n"
                + "    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',\n"
                + "    'warehouse' = '" + warehouse + "',\n"
                + "    'glue.skip-archive' = 'true',\n"
                + "    'glue.skip-name-validation' = 'true',\n";
        }

        // Maintenance is NOT supported on the dynamic sink path (see class Javadoc). We still
        // pass the options through when requested so the behaviour is observable in logs.
        String maintenanceProps = "";
        if (enableMaintenance) {
            LOG.warn("enable.maintenance=true has NO EFFECT with use-dynamic-iceberg-sink: "
                    + "IcebergTableSink bypasses the maintenance topology on the dynamic path. "
                    + "Use Glue auto-compaction, S3 Tables, or a dedicated maintenance job.");
            maintenanceProps =
                  "    'flink-maintenance.rewrite.enabled' = 'true',\n"
                + "    'flink-maintenance.lock.type' = '',\n"
                + "    'flink-maintenance.rewrite.schedule.commit-count' = '10',\n";
        }

        tableEnv.executeSql(
                "CREATE TABLE dynamic_sink (\n"
                + "    event_id STRING,\n"
                + "    event_time STRING,\n"
                + "    event_type STRING,\n"
                + "    region STRING,\n"
                + "    event_date DATE,\n"
                + "    order_id STRING,\n"
                + "    customer_id STRING,\n"
                + "    amount DOUBLE,\n"
                + "    currency STRING,\n"
                + "    status STRING,\n"
                + "    user_id STRING,\n"
                + "    action STRING,\n"
                + "    device_type STRING,\n"
                + "    ip_address STRING,\n"
                + "    user_agent STRING,\n"
                + "    session_id STRING,\n"
                + "    page_url STRING,\n"
                + "    referrer STRING,\n"
                + "    scroll_depth INT,\n"
                + "    time_on_page_seconds BIGINT\n"
                + ") WITH (\n"
                + "    'connector' = 'iceberg',\n"
                + catalogProps
                + maintenanceProps
                + "    'use-dynamic-iceberg-sink' = 'true',\n"
                + "    'dynamic-record-generator-impl' = '" + SqlRoutingGenerator.class.getName() + "',\n"
                + "    'write.format.default' = 'parquet',\n"
                + "    'format-version' = '3',\n"
                + "    'write.target-file-size-bytes' = '134217728',\n"
                + "    'write.parquet.compression-codec' = 'snappy'\n"
                + ")");
        LOG.info("Dynamic Iceberg sink table created (generator={})", SqlRoutingGenerator.class.getName());

        // --- One INSERT fans out to order_events / user_events / click_events ---
        tableEnv.executeSql(
                "INSERT INTO dynamic_sink\n"
                + "SELECT\n"
                + "    event_id, event_time, event_type, region,\n"
                + "    CAST(event_date AS DATE) AS event_date,\n"
                + "    order_id, customer_id, amount, currency, status,\n"
                + "    user_id, action, device_type, ip_address, user_agent,\n"
                + "    session_id, page_url, referrer, scroll_depth, time_on_page_seconds\n"
                + "FROM kinesis_source");
        LOG.info("INSERT INTO dynamic_sink submitted — routing on event_type");
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Required property missing: " + key);
        }
        return value;
    }
}
