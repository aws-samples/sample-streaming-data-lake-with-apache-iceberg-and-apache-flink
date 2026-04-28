package com.aws.samples.iceberg.dynamic.avro;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.aws.samples.iceberg.config.IcebergConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Dynamic Iceberg Sink driven by AWS Glue Schema Registry (Avro).
 *
 * Producers publish Avro records to Kinesis with GSR's wire format. This job reads
 * those bytes, resolves the writer schema from GSR (cached locally), converts each
 * record to an Iceberg DynamicRecord, and routes it to an Iceberg table named after
 * the GSR schema.
 *
 * Configuration:
 *   kinesis.stream.arn        - Kinesis stream ARN
 *   kinesis.region            - AWS region for Kinesis
 *   aws.region                - AWS region for GSR + catalog
 *   schema.registry.name      - GSR registry name (optional; 'default-registry' if omitted)
 *   iceberg.catalog.type      - 'glue' or 's3tables'
 *   iceberg.catalog.name      - catalog logical name
 *   iceberg.database          - Iceberg database/namespace to write into
 *   iceberg.warehouse         - S3 warehouse path (Glue catalog)
 *   s3tables.bucket.arn       - S3 Tables bucket ARN (S3 Tables catalog)
 *   iceberg.branch            - optional branch for all writes
 *   partition.candidates      - comma-separated field names to try for partitioning
 *   cache.max.size            - DynamicIcebergSink cache max size
 *   cache.refresh.ms          - DynamicIcebergSink cache refresh interval
 */
public class DynamicAvroSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicAvroSinkJob.class);

    private static final String LOCAL_PROPS_RESOURCE = "flink-application-properties-dev.json";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = createExecutionEnvironment();
        Map<String, String> config = loadApplicationProperties(env);

        String streamArn = required(config, "kinesis.stream.arn");
        String kinesisRegion = config.getOrDefault("kinesis.region",
                config.getOrDefault("aws.region", "us-east-1"));
        String awsRegion = config.getOrDefault("aws.region", kinesisRegion);
        String registryName = config.getOrDefault("schema.registry.name", "");
        String database = required(config, "iceberg.database");
        String branch = config.getOrDefault("iceberg.branch", "");
        List<String> partitionCandidates = Arrays.asList(
                config.getOrDefault("partition.candidates", "event_date,region,date").split(","));
        int cacheMaxSize = Integer.parseInt(config.getOrDefault("cache.max.size", "100"));
        long cacheRefreshMs = Long.parseLong(config.getOrDefault("cache.refresh.ms", "60000"));

        LOG.info("Starting Dynamic Avro Sink Job");
        LOG.info("  Kinesis stream: {}", streamArn);
        LOG.info("  GSR registry: {}", registryName.isEmpty() ? "default-registry" : registryName);
        LOG.info("  Iceberg database: {}", database);
        LOG.info("  Partition candidates: {}", partitionCandidates);

        if (isLocal(env)) {
            configureCheckpointing(env, config);
        }

        // Source: read bytes from Kinesis, deserialize against GSR
        GsrMultiSchemaDeserializer deserializer = new GsrMultiSchemaDeserializer(awsRegion, registryName);

        Configuration sourceConfig = new Configuration();
        sourceConfig.setString("aws.region", kinesisRegion);
        sourceConfig.setString("flink.stream.initpos", "LATEST");
        sourceConfig.setString("flink.shard.discovery.intervalmillis", "10000");

        KinesisStreamsSource<Tuple2<String, GenericRecord>> source =
                KinesisStreamsSource.<Tuple2<String, GenericRecord>>builder()
                        .setStreamArn(streamArn)
                        .setDeserializationSchema(deserializer)
                        .setSourceConfig(sourceConfig)
                        .build();

        DataStream<Tuple2<String, GenericRecord>> events = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kinesis Source (GSR Avro)")
                .uid("kinesis-gsr-source");

        // Sink: convert to DynamicRecord and write to Iceberg
        CatalogLoader catalogLoader = IcebergConfig.createCatalogLoader(config);

        AvroToDynamicRecordGenerator generator = new AvroToDynamicRecordGenerator(
                database, partitionCandidates, branch.isEmpty() ? null : branch);

        DynamicIcebergSink.forInput(events)
                .generator(generator)
                .catalogLoader(catalogLoader)
                .immediateTableUpdate(true)
                .cacheMaxSize(cacheMaxSize)
                .cacheRefreshMs(cacheRefreshMs)
                .set("write.format.default", "parquet")
                .set("format-version", "2")
                .set("write.target-file-size-bytes", "134217728")
                .set("write.parquet.compression-codec", "snappy")
                .append();

        env.execute("Dynamic Avro Iceberg Sink (GSR-driven)");
    }

    private static String required(Map<String, String> config, String key) {
        String value = config.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("Required property missing: " + key);
        }
        return value;
    }

    private static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    private static StreamExecutionEnvironment createExecutionEnvironment() {
        try {
            Configuration config = new Configuration();
            return StreamExecutionEnvironment.getExecutionEnvironment(config);
        } catch (Exception e) {
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }

    private static void configureCheckpointing(StreamExecutionEnvironment env, Map<String, String> config) {
        long interval = Long.parseLong(config.getOrDefault("checkpoint.interval.ms", "60000"));
        env.enableCheckpointing(interval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000);
        env.getCheckpointConfig().setCheckpointTimeout(600000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    }

    private static Map<String, String> loadApplicationProperties(StreamExecutionEnvironment env) throws Exception {
        Map<String, String> result = new HashMap<>();
        Map<String, Properties> allProps;
        try {
            allProps = KinesisAnalyticsRuntime.getApplicationProperties();
        } catch (Exception e) {
            allProps = new HashMap<>();
        }
        if (allProps == null || allProps.isEmpty()) {
            if (env.getClass().getClassLoader().getResource(LOCAL_PROPS_RESOURCE) != null) {
                allProps = KinesisAnalyticsRuntime.getApplicationProperties(
                        env.getClass().getClassLoader().getResource(LOCAL_PROPS_RESOURCE).getPath());
            }
        }
        Properties props = allProps.getOrDefault("FlinkApplicationProperties", new Properties());
        for (String key : props.stringPropertyNames()) {
            result.put(key, props.getProperty(key));
        }
        return result;
    }
}
