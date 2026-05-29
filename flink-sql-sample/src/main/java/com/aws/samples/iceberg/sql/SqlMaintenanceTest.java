package com.aws.samples.iceberg.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

/**
 * Local Flink SQL maintenance test. Exercises the SAME config-driven IcebergSink maintenance
 * path as FlinkSqlIcebergJob: flink-maintenance.* + lock.type="" (Coordinator Lock, no JDBC).
 * Uses a datagen source (no Kinesis) and a low rewrite commit-count so compaction fires fast.
 * Unbounded INSERT — run in background, then check {db}.{table} snapshots for operation=replace.
 */
public class SqlMaintenanceTest {
    public static void main(String[] args) throws Exception {
        Properties p = System.getProperties();
        String db = p.getProperty("iceberg.database");
        String wh = p.getProperty("iceberg.warehouse");
        String region = p.getProperty("aws.region", "us-east-1");
        String table = p.getProperty("iceberg.table", "sqlmaint");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        StreamTableEnvironment t = StreamTableEnvironment.create(env);
        t.getConfig().set("table.exec.iceberg.use-v2-sink", "true");
        t.getConfig().set("flink-maintenance.rewrite.enabled", "true");
        t.getConfig().set("flink-maintenance.lock.type", ""); // Coordinator Lock, no RDS/ZK
        t.getConfig().set("flink-maintenance.rewrite.schedule.commit-count", "3");

        t.executeSql("CREATE CATALOG glue_catalog WITH ("
                + "'type'='iceberg','catalog-impl'='org.apache.iceberg.aws.glue.GlueCatalog',"
                + "'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',"
                + "'warehouse'='" + wh + "','client.region'='" + region
                + "','glue.region'='" + region + "','s3.region'='" + region + "')");
        t.executeSql("CREATE TABLE IF NOT EXISTS glue_catalog." + db + "." + table
                + " (id BIGINT, val STRING) WITH ('format-version'='3')");
        t.executeSql("CREATE TEMPORARY TABLE src (id BIGINT, val STRING) WITH "
                + "('connector'='datagen','rows-per-second'='200')");
        t.executeSql("INSERT INTO glue_catalog." + db + "." + table + " SELECT id, val FROM src").await();
    }
}
