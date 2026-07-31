package com.aws.samples.iceberg.dynamic.avro;

import com.aws.samples.iceberg.config.IcebergConfig;
import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import com.aws.samples.iceberg.runtime.KinesisSources;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Dynamic Iceberg Sink driven by AWS Glue Schema Registry (Avro).
 *
 * <p>Pipeline:
 * <pre>
 *   Kinesis (raw GSR-wrapped bytes)
 *     -> GsrAvroBytesDeserializer       // pass-through; keeps wire format intact
 *     -> AvroToDynamicRecordGenerator   // resolves schema from GSR, decodes,
 *                                       // converts to Iceberg DynamicRecord
 *     -> DynamicIcebergSink             // routes to Iceberg tables by schema name
 * </pre>
 *
 * <p>Schemas are registered in Glue Schema Registry by producers. No schemas need to be
 * pre-declared in this job — new schemas are discovered automatically when records arrive,
 * and resolved schema versions are cached per UUID by GSR's deserialization facade.
 */
public final class DynamicAvroSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicAvroSinkJob.class);

    private static final String KEY_KINESIS_STREAM_ARN = "kinesis.stream.arn";
    private static final String KEY_KINESIS_REGION = "kinesis.region";
    private static final String KEY_AWS_REGION = "aws.region";
    private static final String KEY_REGISTRY_NAME = "schema.registry.name";
    private static final String KEY_DATABASE = "iceberg.database";
    private static final String KEY_BRANCH = "iceberg.branch";
    private static final String KEY_PARTITION_CANDIDATES = "partition.candidates";
    private static final String KEY_CACHE_MAX_SIZE = "cache.max.size";
    private static final String KEY_CACHE_REFRESH_MS = "cache.refresh.ms";
    private static final String KEY_CHECKPOINT_INTERVAL = "checkpoint.interval.ms";

    private static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String DEFAULT_PARTITION_CANDIDATES = "event_date,region,date";
    private static final String DEFAULT_CACHE_MAX_SIZE = "100";
    private static final String DEFAULT_CACHE_REFRESH_MS = "60000";
    // Iceberg 1.11.0+: V3 tables get equality deletes + Delete Vectors (DVs).
    // DVs replace positional delete files for known-position deletes.
    private static final String FORMAT_VERSION = "3";
    private static final String TARGET_FILE_SIZE_BYTES = "134217728";
    private static final int LOCAL_WEB_UI_PORT = 8083;

    private static final String UID_KINESIS_SOURCE = "kinesis-gsr-bytes";

    private DynamicAvroSinkJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Map<String, String> config = AppProperties.loadAsMap(env);

        String streamArn = required(config, KEY_KINESIS_STREAM_ARN);
        String kinesisRegion = config.getOrDefault(KEY_KINESIS_REGION,
                config.getOrDefault(KEY_AWS_REGION, DEFAULT_AWS_REGION));
        String awsRegion = config.getOrDefault(KEY_AWS_REGION, kinesisRegion);
        String registryName = config.getOrDefault(KEY_REGISTRY_NAME, "");
        String database = required(config, KEY_DATABASE);
        String branch = config.getOrDefault(KEY_BRANCH, "");
        List<String> partitionCandidates = Arrays.asList(
                config.getOrDefault(KEY_PARTITION_CANDIDATES, DEFAULT_PARTITION_CANDIDATES).split(","));
        int cacheMaxSize = Integer.parseInt(config.getOrDefault(KEY_CACHE_MAX_SIZE, DEFAULT_CACHE_MAX_SIZE));
        long cacheRefreshMs = Long.parseLong(config.getOrDefault(KEY_CACHE_REFRESH_MS, DEFAULT_CACHE_REFRESH_MS));

        LOG.info("Starting Dynamic Avro Sink Job");
        LOG.info("  Kinesis stream: {}", streamArn);
        LOG.info("  GSR registry: {}", registryName.isEmpty() ? "default-registry" : registryName);
        LOG.info("  Iceberg database: {}", database);

        if (FlinkEnvironments.isLocal(env)) {
            long interval = Long.parseLong(config.getOrDefault(
                    KEY_CHECKPOINT_INTERVAL, Long.toString(Checkpointing.DEFAULT_INTERVAL_MS)));
            Checkpointing.configureLocalDefaults(env, interval);
        }

        // Source: raw GSR-wrapped bytes (no decoding on the source side so we don't pay
        // the Kryo cost for Avro GenericRecord between operators).
        KinesisStreamsSource<byte[]> source = KinesisSources.create(
                streamArn, kinesisRegion, new GsrAvroBytesDeserializer());

        DataStream<byte[]> eventBytes = env.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kinesis Source (GSR wire-format bytes)",
                        PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)
                .uid(UID_KINESIS_SOURCE);

        // Sink: resolve schema, decode, convert, and write to Iceberg.
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
                .set("format-version", FORMAT_VERSION)
                .set("write.target-file-size-bytes", TARGET_FILE_SIZE_BYTES)
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
}
