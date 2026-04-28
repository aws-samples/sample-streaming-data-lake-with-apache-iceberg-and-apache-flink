package com.aws.samples.iceberg.dynamic;

import com.aws.samples.iceberg.config.IcebergConfig;
import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import com.aws.samples.iceberg.runtime.KinesisSources;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Schema-agnostic Dynamic Iceberg Sink Job.
 *
 * <p>Reads raw JSON from Kinesis without typed POJOs and infers Iceberg schema at runtime.
 * Records are routed to a per-event-type table (configurable; default {@code event_type})
 * and schemas evolve automatically as new fields appear.
 *
 * <p>Table naming: {@code {routing_field_value}{tableSuffix}} (e.g. {@code order_events}).
 */
public final class DynamicSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicSinkJob.class);

    // Configuration keys
    private static final String KEY_KINESIS_STREAM_ARN = "kinesis.stream.arn";
    private static final String KEY_KINESIS_REGION = "kinesis.region";
    private static final String KEY_AWS_REGION = "aws.region";
    private static final String KEY_ICEBERG_DATABASE = "iceberg.database";
    private static final String KEY_CHECKPOINT_INTERVAL = "checkpoint.interval.ms";
    private static final String KEY_CACHE_MAX_SIZE = "cache.max.size";
    private static final String KEY_CACHE_REFRESH_MS = "cache.refresh.ms";

    // Schema-agnostic routing configuration
    private static final String KEY_ROUTING_FIELD = "routing.field";
    private static final String KEY_ROUTING_TABLE_SUFFIX = "routing.table.suffix";
    private static final String KEY_PARTITION_CANDIDATES = "partition.candidates";
    private static final String KEY_WRITE_MODE = "write.mode";
    private static final String KEY_PRIMARY_KEY_COLUMNS = "primary.key.columns";

    // Defaults
    private static final String DEFAULT_DATABASE = "iceberg_samples";
    private static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String DEFAULT_ROUTING_FIELD = "event_type";
    private static final String DEFAULT_TABLE_SUFFIX = "_events";
    private static final String DEFAULT_PARTITION_CANDIDATES = "event_date,region,date";
    private static final String DEFAULT_WRITE_MODE = "append";
    private static final String DEFAULT_PK_COLUMNS = "event_id,event_date,region";
    private static final String DEFAULT_CACHE_MAX_SIZE = "100";
    private static final String DEFAULT_CACHE_REFRESH_MS = "60000";
    private static final String TARGET_FILE_SIZE_BYTES = "134217728";
    // Flink 1.10's Iceberg sink emits v2-style equality + positional delete files.
    // See BLOG_POST.md for the full discussion of why v3 is not yet useful here.
    private static final String FORMAT_VERSION = "2";
    private static final int LOCAL_WEB_UI_PORT = 8082;

    private static final String UID_KINESIS_SOURCE = "kinesis-source-dynamic";

    private DynamicSinkJob() {}

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Schema-Agnostic Dynamic Iceberg Sink Job");

        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Map<String, String> config = AppProperties.loadAsMap(env);

        if (FlinkEnvironments.isLocal(env)) {
            long interval = Long.parseLong(config.getOrDefault(
                    KEY_CHECKPOINT_INTERVAL, Long.toString(Checkpointing.DEFAULT_INTERVAL_MS)));
            Checkpointing.configureLocalDefaults(env, interval);
        } else {
            LOG.info("Running on AWS Managed Flink — checkpointing configured by the service");
        }

        DataStream<JsonNode> events = env.fromSource(
                        createKinesisSource(config),
                        createWatermarkStrategy(),
                        "Kinesis Source (Schema-Agnostic JSON)",
                        TypeInformation.of(JsonNode.class))
                .uid(UID_KINESIS_SOURCE);

        configureDynamicSink(events, IcebergConfig.createCatalogLoader(config), config);

        env.execute("Dynamic Iceberg Sink Job - Schema Agnostic");
    }

    private static KinesisStreamsSource<JsonNode> createKinesisSource(Map<String, String> config) {
        String streamArn = config.get(KEY_KINESIS_STREAM_ARN);
        if (streamArn == null || streamArn.isEmpty()) {
            throw new IllegalArgumentException(KEY_KINESIS_STREAM_ARN + " is required");
        }
        String region = config.getOrDefault(KEY_KINESIS_REGION,
                config.getOrDefault(KEY_AWS_REGION, DEFAULT_AWS_REGION));
        LOG.info("Kinesis source: stream={}, region={}", streamArn, region);
        return KinesisSources.create(streamArn, region, new JsonNodeDeserializer());
    }

    private static WatermarkStrategy<JsonNode> createWatermarkStrategy() {
        return WatermarkStrategy
                .<JsonNode>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((json, timestamp) -> extractEventTime(json))
                .withIdleness(Duration.ofMinutes(5));
    }

    private static long extractEventTime(JsonNode json) {
        if (json.has("event_time")) {
            try {
                return Instant.parse(json.get("event_time").asText()).toEpochMilli();
            } catch (RuntimeException e) {
                // Fall through to processing time for records that don't carry a valid event_time.
            }
        }
        return System.currentTimeMillis();
    }

    private static void configureDynamicSink(
            DataStream<JsonNode> events,
            CatalogLoader catalogLoader,
            Map<String, String> config) {

        String database = config.getOrDefault(KEY_ICEBERG_DATABASE, DEFAULT_DATABASE);
        int cacheMaxSize = Integer.parseInt(config.getOrDefault(KEY_CACHE_MAX_SIZE, DEFAULT_CACHE_MAX_SIZE));
        long cacheRefreshMs = Long.parseLong(config.getOrDefault(KEY_CACHE_REFRESH_MS, DEFAULT_CACHE_REFRESH_MS));

        String routingField = config.getOrDefault(KEY_ROUTING_FIELD, DEFAULT_ROUTING_FIELD);
        String tableSuffix = config.getOrDefault(KEY_ROUTING_TABLE_SUFFIX, DEFAULT_TABLE_SUFFIX);
        List<String> partitionCandidates = Arrays.asList(
                config.getOrDefault(KEY_PARTITION_CANDIDATES, DEFAULT_PARTITION_CANDIDATES).split(","));

        String writeMode = config.getOrDefault(KEY_WRITE_MODE, DEFAULT_WRITE_MODE);
        boolean isUpsert = "upsert".equalsIgnoreCase(writeMode);
        List<String> primaryKeyColumns = Arrays.asList(
                config.getOrDefault(KEY_PRIMARY_KEY_COLUMNS, DEFAULT_PK_COLUMNS).split(","));

        LOG.info("Dynamic Iceberg Sink: db={}, routingField={}, tableSuffix={}, "
                        + "partitionCandidates={}, writeMode={}{}",
                database, routingField, tableSuffix, partitionCandidates, writeMode,
                isUpsert ? " (PKs: " + primaryKeyColumns + ")" : "");

        SchemaAgnosticRoutingGenerator generator = new SchemaAgnosticRoutingGenerator(
                database, routingField, null, tableSuffix, partitionCandidates);

        var sinkBuilder = DynamicIcebergSink.forInput(events)
                .generator(generator)
                .catalogLoader(catalogLoader)
                .immediateTableUpdate(true)
                .cacheMaxSize(cacheMaxSize)
                .cacheRefreshMs(cacheRefreshMs)
                .set("write.format.default", "parquet")
                .set("format-version", FORMAT_VERSION)
                .set("write.target-file-size-bytes", TARGET_FILE_SIZE_BYTES)
                .set("write.parquet.compression-codec", "snappy");

        if (isUpsert) {
            sinkBuilder
                    .set("write.upsert.enabled", "true")
                    .set("write.delete.mode", "merge-on-read")
                    .set("write.update.mode", "merge-on-read")
                    .set("write.merge.mode", "merge-on-read");
        }

        sinkBuilder.append();
    }
}
