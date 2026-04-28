package com.aws.samples.iceberg.runtime;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.connector.kinesis.source.serialization.KinesisDeserializationSchema;

/**
 * Build {@link KinesisStreamsSource}s with the production-friendly defaults used
 * across the samples (LATEST start position, 10-second shard discovery, 10 000
 * records per GetRecords call).
 *
 * <p>Both {@link DeserializationSchema} (body only) and
 * {@link KinesisDeserializationSchema} (full {@code Record} with partition key and
 * sequence number) are supported — the Kinesis connector accepts both.
 */
public final class KinesisSources {

    private static final String KEY_AWS_REGION = "aws.region";
    private static final String KEY_STREAM_INIT_POSITION = "flink.stream.initpos";
    private static final String KEY_SHARD_DISCOVERY_INTERVAL = "flink.shard.discovery.intervalmillis";
    private static final String KEY_GET_RECORDS_MAX = "flink.shard.getrecords.maxrecordcount";

    private static final String DEFAULT_INIT_POSITION = "LATEST";
    private static final String DEFAULT_SHARD_DISCOVERY_INTERVAL_MS = "10000";
    private static final String DEFAULT_GET_RECORDS_MAX = "10000";

    private KinesisSources() {}

    /**
     * Build a source using a plain {@link DeserializationSchema} (byte[] → T).
     */
    public static <T> KinesisStreamsSource<T> create(
            String streamArn,
            String region,
            DeserializationSchema<T> schema) {
        return KinesisStreamsSource.<T>builder()
                .setStreamArn(streamArn)
                .setDeserializationSchema(schema)
                .setSourceConfig(defaultSourceConfig(region))
                .build();
    }

    /**
     * Build a source using a {@link KinesisDeserializationSchema}, which gives access to
     * Kinesis record metadata (partition key, sequence number, stream, shard id).
     */
    public static <T> KinesisStreamsSource<T> create(
            String streamArn,
            String region,
            KinesisDeserializationSchema<T> schema) {
        return KinesisStreamsSource.<T>builder()
                .setStreamArn(streamArn)
                .setDeserializationSchema(schema)
                .setSourceConfig(defaultSourceConfig(region))
                .build();
    }

    /**
     * Default source configuration. Exposed so callers can override before using it —
     * e.g. to change the starting position for backfill scenarios.
     */
    public static Configuration defaultSourceConfig(String region) {
        Configuration sourceConfig = new Configuration();
        sourceConfig.setString(KEY_AWS_REGION, region);
        sourceConfig.setString(KEY_STREAM_INIT_POSITION, DEFAULT_INIT_POSITION);
        sourceConfig.setString(KEY_SHARD_DISCOVERY_INTERVAL, DEFAULT_SHARD_DISCOVERY_INTERVAL_MS);
        sourceConfig.setString(KEY_GET_RECORDS_MAX, DEFAULT_GET_RECORDS_MAX);
        return sourceConfig;
    }
}
