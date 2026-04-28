package com.aws.samples.iceberg.source;

import com.aws.samples.iceberg.config.IcebergConfig;
import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import com.aws.samples.iceberg.util.RowDataToJsonMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.apache.iceberg.flink.source.StreamingStartingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Read an Iceberg table and republish rows as JSON to Kinesis.
 *
 * <p>Features demonstrated:
 * <ul>
 *   <li>FLIP-27 {@link IcebergSource} for streaming and batch reads
 *   <li>Configurable starting strategies
 *   <li>Watermark generation hook (column-based watermarks when a column is configured)
 *   <li>Works with both Glue Catalog and S3 Tables
 * </ul>
 *
 * <p><b>Important:</b> streaming reads only work for append-only tables. Tables with
 * equality deletes (upsert mode) are not supported as streaming sources.
 */
public class IcebergSourceJob {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceJob.class);
    private static final int LOCAL_WEB_UI_PORT = 8085;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Properties props = AppProperties.load(env);
        validateConfiguration(props);

        if (FlinkEnvironments.isLocal(env)) {
            long interval = Long.parseLong(props.getProperty(
                    "checkpoint.interval.ms", Long.toString(Checkpointing.DEFAULT_INTERVAL_MS)));
            Checkpointing.configureLocalDefaults(env, interval);
        }

        buildPipeline(env, props);

        env.execute("Iceberg Source to Kinesis");
    }

    private static void buildPipeline(StreamExecutionEnvironment env, Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String database = props.getProperty("iceberg.database", "iceberg_samples");
        String tableName = props.getProperty("iceberg.table", "orders");
        String region = props.getProperty("aws.region", "us-east-1");
        boolean streaming = Boolean.parseBoolean(props.getProperty("iceberg.source.streaming", "true"));

        LOG.info("IcebergSource: catalog={}, region={}, db={}, table={}, streaming={}",
                catalogType, region, database, tableName, streaming);

        CatalogLoader catalogLoader = IcebergConfig.createCatalogLoader(catalogType, props);
        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);

        tableLoader.open();
        Table table = tableLoader.loadTable();
        Schema icebergSchema = table.schema();

        IcebergSource<RowData> icebergSource = buildIcebergSource(tableLoader, props, streaming);
        WatermarkStrategy<RowData> watermarkStrategy = buildWatermarkStrategy(props);

        DataStream<RowData> sourceStream = env.fromSource(
                        icebergSource,
                        watermarkStrategy,
                        "Iceberg Source: " + tableName,
                        TypeInformation.of(RowData.class))
                .uid("iceberg-source");

        DataStream<String> jsonStream = sourceStream
                .map(new RowDataToJsonMapper(icebergSchema))
                .uid("rowdata-to-json")
                .name("RowData to JSON");

        String sinkStreamArn = props.getProperty("kinesis.sink.stream.arn");
        jsonStream.sinkTo(buildKinesisSink(sinkStreamArn, region))
                .uid("kinesis-sink")
                .name("Kinesis Sink");
    }

    private static IcebergSource<RowData> buildIcebergSource(
            TableLoader tableLoader, Properties props, boolean streaming) {
        Duration monitorInterval = parseDuration(
                props.getProperty("iceberg.source.monitor-interval", "60s"));

        IcebergSource.Builder<RowData> builder = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .streaming(streaming);

        if (streaming) {
            StreamingStartingStrategy startingStrategy = parseStartingStrategy(
                    props.getProperty("iceberg.source.starting-strategy",
                            "INCREMENTAL_FROM_LATEST_SNAPSHOT"));
            builder.streamingStartingStrategy(startingStrategy);
            builder.monitorInterval(monitorInterval);
            LOG.info("Streaming source configured: strategy={}, monitorInterval={}",
                    startingStrategy, monitorInterval);
        }

        String splitSize = props.getProperty("iceberg.source.split-size");
        if (splitSize != null && !splitSize.isEmpty()) {
            builder.splitSize(Long.parseLong(splitSize));
        }

        return builder.build();
    }

    /**
     * Build a watermark strategy. When a watermark column is configured we use a bounded
     * out-of-orderness strategy — Iceberg's column-statistics watermarks are not yet
     * wired into this sample. Without a configured column, disable watermarks entirely.
     */
    private static WatermarkStrategy<RowData> buildWatermarkStrategy(Properties props) {
        String watermarkColumn = props.getProperty("iceberg.source.watermark-column");
        if (watermarkColumn != null && !watermarkColumn.isEmpty()) {
            LOG.info("Watermark generation enabled for column: {}", watermarkColumn);
            return WatermarkStrategy
                    .<RowData>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withIdleness(Duration.ofMinutes(1));
        }
        return WatermarkStrategy.noWatermarks();
    }

    private static KinesisStreamsSink<String> buildKinesisSink(String streamArn, String region) {
        Properties sinkProps = new Properties();
        sinkProps.setProperty(AWSConfigConstants.AWS_REGION, region);
        return KinesisStreamsSink.<String>builder()
                .setStreamArn(streamArn)
                .setSerializationSchema(new SimpleStringSchema())
                .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                .setKinesisClientProperties(sinkProps)
                .build();
    }

    private static StreamingStartingStrategy parseStartingStrategy(String strategy) {
        switch (strategy.toUpperCase()) {
            case "TABLE_SCAN_THEN_INCREMENTAL":
                return StreamingStartingStrategy.TABLE_SCAN_THEN_INCREMENTAL;
            case "INCREMENTAL_FROM_EARLIEST_SNAPSHOT":
                return StreamingStartingStrategy.INCREMENTAL_FROM_EARLIEST_SNAPSHOT;
            case "INCREMENTAL_FROM_SNAPSHOT_ID":
                return StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_ID;
            case "INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP":
                return StreamingStartingStrategy.INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP;
            case "INCREMENTAL_FROM_LATEST_SNAPSHOT":
            default:
                return StreamingStartingStrategy.INCREMENTAL_FROM_LATEST_SNAPSHOT;
        }
    }

    /** Accept "60s", "5m", "1h" or a bare seconds-number. */
    private static Duration parseDuration(String durationStr) {
        if (durationStr.endsWith("s")) {
            return Duration.ofSeconds(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
        }
        if (durationStr.endsWith("m")) {
            return Duration.ofMinutes(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
        }
        if (durationStr.endsWith("h")) {
            return Duration.ofHours(Long.parseLong(durationStr.substring(0, durationStr.length() - 1)));
        }
        return Duration.ofSeconds(Long.parseLong(durationStr));
    }

    private static void validateConfiguration(Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        if ("glue".equalsIgnoreCase(catalogType)) {
            requireProperty(props, "iceberg.warehouse",
                    "iceberg.warehouse is required for Glue catalog");
        } else if ("s3tables".equalsIgnoreCase(catalogType)) {
            requireProperty(props, "s3tables.bucket.arn",
                    "s3tables.bucket.arn is required for S3 Tables catalog");
        }
        requireProperty(props, "iceberg.database", "iceberg.database is required");
        requireProperty(props, "iceberg.table", "iceberg.table is required");
        requireProperty(props, "kinesis.sink.stream.arn", "kinesis.sink.stream.arn is required");

        if (Boolean.parseBoolean(props.getProperty("iceberg.source.streaming", "true"))) {
            LOG.warn("Streaming reads only support append-only tables. Tables with equality "
                    + "deletes (upsert mode) are not supported as streaming sources.");
        }
    }

    private static void requireProperty(Properties props, String key, String message) {
        String value = props.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }
}
