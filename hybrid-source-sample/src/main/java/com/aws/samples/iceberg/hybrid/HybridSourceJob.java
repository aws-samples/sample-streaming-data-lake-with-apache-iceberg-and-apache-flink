package com.aws.samples.iceberg.hybrid;

import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import com.aws.samples.iceberg.util.RowDataToJsonMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.aws.config.AWSConfigConstants;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.kinesis.sink.KinesisStreamsSink;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;

/**
 * Hybrid Source Job - Bootstrap from Iceberg historical data, then switch to real-time Kinesis.
 * 
 * This sample demonstrates the Flink HybridSource pattern (FLIP-150):
 * 1. First, read all historical data from an Iceberg table (bounded source)
 * 2. Then, seamlessly switch to reading real-time events from Kinesis (unbounded source)
 * 
 * The HybridSource provides automatic switchover - when the Iceberg source finishes
 * reading all historical data, it automatically transitions to the Kinesis source
 * for real-time streaming. This appears as a single source in the Flink job graph.
 * 
 * Key implementation details:
 * - Both sources produce RowData for unified processing
 * - Iceberg source uses IcebergSource.forRowData()
 * - Kinesis source uses JsonToRowDataDeserializer to convert JSON to RowData
 * - HybridSource handles the automatic switchover
 * 
 * Use cases:
 * - Backfilling a new streaming application with historical data
 * - Recovering from extended downtime without data loss
 * - Migrating from batch to streaming processing
 * - Lambda architecture replacement (unified batch + streaming)
 * 
 * Configuration properties:
 * - iceberg.catalog.type: 'glue' or 's3tables'
 * - iceberg.database: Database/namespace name
 * - iceberg.table: Table name to read historical data from
 * - kinesis.source.stream.arn: Kinesis stream ARN for real-time data
 * - kinesis.sink.stream.arn: Kinesis stream ARN to write processed data
 */
public class HybridSourceJob {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceJob.class);
    private static final int LOCAL_WEB_UI_PORT = 8086;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Properties flinkProps = AppProperties.load(env);
        validateConfiguration(flinkProps);

        if (FlinkEnvironments.isLocal(env)) {
            long interval = Long.parseLong(flinkProps.getProperty(
                    "checkpoint.interval.ms", Long.toString(Checkpointing.DEFAULT_INTERVAL_MS)));
            Checkpointing.configureLocalDefaults(env, interval);
        }

        env.disableOperatorChaining();
        buildPipeline(env, flinkProps);

        env.execute("HybridSource: Iceberg Bootstrap -> Kinesis Streaming");
    }
    
    private static void buildPipeline(StreamExecutionEnvironment env, Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        String database = props.getProperty("iceberg.database", "iceberg_samples");
        String tableName = props.getProperty("iceberg.table", "orders");
        String region = props.getProperty("aws.region", "us-east-1");
        String sourceStreamArn = props.getProperty("kinesis.source.stream.arn");
        String sinkStreamArn = props.getProperty("kinesis.sink.stream.arn");
        
        LOG.info("=== HybridSource Pipeline Configuration ===");
        LOG.info("Phase 1 (Bounded):   Iceberg table {}.{}", database, tableName);
        LOG.info("Phase 2 (Unbounded): Kinesis stream {}", sourceStreamArn);
        LOG.info("Output:              Kinesis stream {}", sinkStreamArn);
        LOG.info("============================================");
        
        // Create catalog loader and table loader for Iceberg
        CatalogLoader catalogLoader = com.aws.samples.iceberg.config.IcebergConfig.createCatalogLoader(catalogType, props);
        TableIdentifier tableId = TableIdentifier.of(database, tableName);
        TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableId);
        
        // Load table to get schema
        tableLoader.open();
        Table table = tableLoader.loadTable();
        Schema icebergSchema = table.schema();
        LOG.info("Loaded Iceberg table schema with {} columns", icebergSchema.columns().size());
        
        // Build the HybridSource with both sources producing RowData
        HybridSource<RowData> hybridSource = buildHybridSource(tableLoader, icebergSchema, sourceStreamArn, region);
        
        // Read from HybridSource
        DataStream<RowData> hybridStream = env.fromSource(
                hybridSource,
                WatermarkStrategy.<RowData>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withIdleness(Duration.ofMinutes(1)),
                "HybridSource: Iceberg -> Kinesis",
                TypeInformation.of(RowData.class)
        ).uid("hybrid-source");
        
        // Convert RowData to JSON for output
        DataStream<String> jsonStream = hybridStream
                .map(new RowDataToJsonMapper(icebergSchema))
                .uid("rowdata-to-json")
                .name("RowData to JSON");

        // Write to sink Kinesis stream
        KinesisStreamsSink<String> kinesisSink = buildKinesisSink(sinkStreamArn, region);
        jsonStream.sinkTo(kinesisSink).uid("kinesis-sink").name("Kinesis Sink");
        
        LOG.info("Pipeline built successfully");
    }
    
    /**
     * Builds a HybridSource that reads from Iceberg first (bounded), 
     * then switches to Kinesis (unbounded).
     * 
     * Both sources produce RowData for unified processing.
     */
    private static HybridSource<RowData> buildHybridSource(
            TableLoader tableLoader,
            Schema icebergSchema,
            String kinesisStreamArn,
            String region) {
        
        // Source 1: Iceberg (bounded) - reads all historical data
        IcebergSource<RowData> icebergSource = IcebergSource.forRowData()
                .tableLoader(tableLoader)
                .streaming(false)  // Bounded mode - completes after reading all data
                .build();
        
        // Source 2: Kinesis (unbounded) - real-time streaming
        // Uses custom deserializer to convert JSON to RowData matching Iceberg schema
        KinesisStreamsSource<RowData> kinesisSource = KinesisStreamsSource.<RowData>builder()
                .setStreamArn(kinesisStreamArn)
                .setDeserializationSchema(new JsonToRowDataDeserializer(icebergSchema))
                .setSourceConfig(buildKinesisSourceConfig(region))
                .build();
        
        // Build HybridSource: Iceberg first, then Kinesis
        // When Iceberg source completes (all historical data read),
        // HybridSource automatically switches to Kinesis source
        HybridSource<RowData> hybridSource = HybridSource
                .builder(icebergSource)
                .addSource(kinesisSource)
                .build();
        
        LOG.info("Built HybridSource: IcebergSource (bounded) -> KinesisSource (unbounded)");
        return hybridSource;
    }
    
    private static Configuration buildKinesisSourceConfig(String region) {
        Configuration config = new Configuration();
        config.setString("aws.region", region);
        // Start from LATEST - we only want new events after Iceberg bootstrap completes
        config.setString("flink.stream.initpos", "LATEST");
        config.setString("flink.shard.discovery.intervalmillis", "10000");
        return config;
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

    private static void validateConfiguration(Properties props) {
        String catalogType = props.getProperty("iceberg.catalog.type", "glue");
        
        if ("glue".equalsIgnoreCase(catalogType)) {
            requireProperty(props, "iceberg.warehouse", "ICEBERG_WAREHOUSE is required for Glue catalog");
        } else if ("s3tables".equalsIgnoreCase(catalogType)) {
            requireProperty(props, "s3tables.bucket.arn", "S3 Tables bucket ARN is required");
        }
        
        requireProperty(props, "iceberg.database", "ICEBERG_DATABASE is required");
        requireProperty(props, "iceberg.table", "ICEBERG_TABLE is required");
        requireProperty(props, "kinesis.source.stream.arn", "KINESIS_SOURCE_STREAM_ARN is required");
        requireProperty(props, "kinesis.sink.stream.arn", "KINESIS_SINK_STREAM_ARN is required");
    }
    
    private static void requireProperty(Properties props, String key, String message) {
        String value = props.getProperty(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(message);
        }
    }
}
