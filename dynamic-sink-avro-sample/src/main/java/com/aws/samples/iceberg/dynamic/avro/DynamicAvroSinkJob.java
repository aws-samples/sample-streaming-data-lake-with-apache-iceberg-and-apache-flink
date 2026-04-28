package com.aws.samples.iceberg.dynamic.avro;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.aws.samples.iceberg.config.IcebergConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
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
 * Pipeline:
 *   Kinesis (raw GSR-wrapped bytes)
 *     -> GsrAvroBytesDeserializer (pass-through; keeps wire format intact)
 *     -> AvroToDynamicRecordGenerator (resolves schema from GSR, decodes,
 *                                      converts to Iceberg DynamicRecord)
 *     -> DynamicIcebergSink (routes to Iceberg tables based on schema name)
 *
 * Schemas are registered in Glue Schema Registry by producers. No schemas need to be
 * pre-declared in this job - new schemas are discovered automatically when records
 * arrive and referenced schema versions are resolved on first use (GSR caches
 * subsequent lookups per schema UUID).
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

        if (isLocal(env)) {
            configureCheckpointing(env, config);
        }

        // Source: read raw GSR-wrapped bytes (no decoding on the source side to avoid
        // serializing Avro GenericRecord between operators)
        Configuration sourceConfig = new Configuration();
        sourceConfig.setString("aws.region", kinesisRegion);
        sourceConfig.setString("flink.stream.initpos", "LATEST");
        sourceConfig.setString("flink.shard.discovery.intervalmillis", "10000");

        KinesisStreamsSource<byte[]> source = KinesisStreamsSource.<byte[]>builder()
                .setStreamArn(streamArn)
                .setDeserializationSchema(new GsrAvroBytesDeserializer())
                .setSourceConfig(sourceConfig)
                .build();

        DataStream<byte[]> eventBytes = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kinesis Source (GSR wire-format bytes)",
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .uid("kinesis-gsr-bytes");

        // Sink: resolve schema, decode, convert, and write to Iceberg
        CatalogLoader catalogLoader = IcebergConfig.createCatalogLoader(config);

        AvroToDynamicRecordGenerator generator = new AvroToDynamicRecordGenerator(
                awsRegion, registryName, database, partitionCandidates,
                branch.isEmpty() ? null : branch);

        DynamicIcebergSink.forInput(eventBytes)
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
