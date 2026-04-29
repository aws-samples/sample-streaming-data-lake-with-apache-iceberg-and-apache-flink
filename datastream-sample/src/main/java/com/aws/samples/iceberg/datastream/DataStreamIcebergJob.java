package com.aws.samples.iceberg.datastream;

import com.aws.samples.iceberg.config.IcebergConfig;
import com.aws.samples.iceberg.model.OrderEvent;
import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import com.aws.samples.iceberg.runtime.KinesisSources;
import com.aws.samples.iceberg.util.EventToRowDataConverter;
import com.aws.samples.iceberg.util.OrderEventDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.maintenance.api.DeleteOrphanFiles;
import org.apache.iceberg.flink.maintenance.api.ExpireSnapshots;
import org.apache.iceberg.flink.maintenance.api.JdbcLockFactory;
import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.apache.iceberg.flink.maintenance.api.TableMaintenance;
import org.apache.iceberg.flink.maintenance.api.TriggerLockFactory;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * DataStream API sample: reads {@link OrderEvent} from Kinesis and writes to an Iceberg
 * table using the SinkV2-based {@link IcebergSink}, optionally with in-job table
 * maintenance coordinated by a JDBC lock.
 *
 * <p>Every operator is given an explicit {@code uid} and {@code name} so that checkpoint
 * state can survive job-graph changes — see the
 * <a href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/production_ready/">
 * Flink Production Readiness</a> guide.
 *
 * <p>Configuration is passed through {@code FlinkApplicationProperties} on Managed Flink
 * or through the equivalent local JSON resource when run from an IDE.
 */
public final class DataStreamIcebergJob {

    private static final Logger LOG = LoggerFactory.getLogger(DataStreamIcebergJob.class);

    // Configuration keys (externalised so CDK / local config files stay in sync).
    private static final String KINESIS_STREAM_ARN = "kinesis.stream.arn";
    private static final String KINESIS_REGION = "kinesis.region";
    private static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    private static final String ICEBERG_DATABASE = "iceberg.database";
    private static final String ICEBERG_TABLE = "iceberg.table";
    private static final String AWS_REGION = "aws.region";
    private static final String CHECKPOINT_INTERVAL = "checkpoint.interval.ms";
    private static final String ICEBERG_BRANCH = "iceberg.branch";
    private static final String ENABLE_MAINTENANCE = "enable.maintenance";
    private static final String RDS_JDBC_URL = "rds.jdbc.url";
    private static final String RDS_USER = "rds.user";
    private static final String RDS_PASSWORD = "rds.password";
    private static final String WRITE_MODE = "write.mode";
    private static final String PRIMARY_KEY_COLUMNS = "primary.key.columns";
    private static final String TABLE_FORMAT_VERSION = "table.format.version";

    // Write-side tuning defaults.
    private static final String DEFAULT_TABLE_FORMAT_VERSION = "2";
    private static final String DEFAULT_WRITE_MODE = "upsert";
    private static final String DEFAULT_PK_COLUMNS = "event_id,event_date,region";
    private static final String DEFAULT_DATABASE = "iceberg_samples";
    private static final String DEFAULT_TABLE = "orders";
    private static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String DEFAULT_CATALOG_TYPE = "glue";
    private static final long DEFAULT_TARGET_FILE_SIZE = 128L * 1024 * 1024; // 128 MB
    private static final int LOCAL_WEB_UI_PORT = 8081;

    // Operator uid constants — keep stable across releases.
    private static final String UID_KINESIS_SOURCE = "kinesis-source";
    private static final String UID_TO_ROWDATA = "event-to-rowdata";

    private DataStreamIcebergJob() {
        // utility-style main class; not intended to be instantiated.
    }

    public static void main(String[] args) throws Exception {
        LOG.info("Starting DataStream Iceberg Job");

        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Map<String, String> config = AppProperties.loadAsMap(env);
        validateConfiguration(config);

        if (FlinkEnvironments.isLocal(env)) {
            long interval = Long.parseLong(config.getOrDefault(
                    CHECKPOINT_INTERVAL, Long.toString(Checkpointing.DEFAULT_INTERVAL_MS)));
            Checkpointing.configureLocalDefaults(env, interval);
        } else {
            LOG.info("Running on AWS Managed Flink — checkpointing configured by the service");
        }

        // Source: Kinesis -> OrderEvent
        DataStream<OrderEvent> orderEvents = env.fromSource(
                        createKinesisSource(config),
                        createWatermarkStrategy(),
                        "Kinesis Source (OrderEvent)",
                        TypeInformation.of(OrderEvent.class))
                .uid(UID_KINESIS_SOURCE)
                .name("Read from Kinesis");

        // Convert: OrderEvent -> RowData (explicit type info avoids Kryo fallback on JDK 17+).
        DataStream<RowData> rowDataStream = orderEvents
                .map(EventToRowDataConverter::convertOrderEvent)
                .returns(InternalTypeInfo.of(orderEventRowType()))
                .uid(UID_TO_ROWDATA)
                .name("Convert OrderEvent to RowData");

        CatalogLoader catalogLoader = IcebergConfig.createCatalogLoader(config);
        ensureTableExists(catalogLoader, config);
        TableLoader tableLoader = TableLoader.fromCatalog(
                catalogLoader,
                TableIdentifier.of(
                        config.getOrDefault(ICEBERG_DATABASE, DEFAULT_DATABASE),
                        config.getOrDefault(ICEBERG_TABLE, DEFAULT_TABLE)));

        configureIcebergSink(rowDataStream, tableLoader, config, env);

        LOG.info("Executing DataStream Iceberg Job");
        env.execute("DataStream Iceberg Job - Orders");
    }

    // ------------------------------------------------------------------------
    // Configuration validation
    // ------------------------------------------------------------------------

    private static void validateConfiguration(Map<String, String> config) {
        String streamArn = config.get(KINESIS_STREAM_ARN);
        if (streamArn == null || streamArn.isEmpty()) {
            throw new IllegalArgumentException(KINESIS_STREAM_ARN + " is required");
        }

        String catalogType = config.getOrDefault(ICEBERG_CATALOG_TYPE, DEFAULT_CATALOG_TYPE);
        if ("s3tables".equalsIgnoreCase(catalogType)) {
            String bucketArn = config.get("s3tables.bucket.arn");
            if (bucketArn == null || bucketArn.isEmpty()) {
                throw new IllegalArgumentException(
                        "s3tables.bucket.arn is required when using S3 Tables catalog");
            }
            LOG.info("Using S3 Tables catalog (bucket arn: {})", bucketArn);
        } else {
            String warehouse = config.get("iceberg.warehouse");
            if (warehouse == null || warehouse.isEmpty()) {
                throw new IllegalArgumentException(
                        "iceberg.warehouse is required when using Glue catalog");
            }
            LOG.info("Using Glue catalog (warehouse: {})", warehouse);
        }
    }

    // ------------------------------------------------------------------------
    // Sources
    // ------------------------------------------------------------------------

    private static KinesisStreamsSource<OrderEvent> createKinesisSource(Map<String, String> config) {
        String streamArn = config.get(KINESIS_STREAM_ARN);
        String region = config.getOrDefault(KINESIS_REGION,
                config.getOrDefault(AWS_REGION, DEFAULT_AWS_REGION));

        LOG.info("Kinesis source: stream={}, region={}", streamArn, region);

        return KinesisSources.create(streamArn, region, new OrderEventDeserializer());
    }

    private static WatermarkStrategy<OrderEvent> createWatermarkStrategy() {
        return WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofMinutes(1))
                .withTimestampAssigner((event, timestamp) -> event.getEventTime().toEpochMilli())
                .withIdleness(Duration.ofMinutes(5));
    }

    // ------------------------------------------------------------------------
    // Iceberg table bootstrap
    // ------------------------------------------------------------------------

    /**
     * Create the target Iceberg table if it does not already exist. Safe to call from all
     * task managers — concurrent creation attempts are handled via {@link AlreadyExistsException}.
     */
    private static void ensureTableExists(CatalogLoader catalogLoader, Map<String, String> config) {
        String database = config.getOrDefault(ICEBERG_DATABASE, DEFAULT_DATABASE);
        String table = config.getOrDefault(ICEBERG_TABLE, DEFAULT_TABLE);
        TableIdentifier tableId = TableIdentifier.of(database, table);

        Catalog catalog = catalogLoader.loadCatalog();
        if (catalog.tableExists(tableId)) {
            LOG.info("Table {} already exists", tableId);
            return;
        }

        LOG.info("Creating table {}", tableId);

        Schema schema = createOrderEventSchema();
        PartitionSpec partitionSpec = PartitionSpec.builderFor(schema)
                .day("event_date")
                .identity("region")
                .build();

        String formatVersion = config.getOrDefault(TABLE_FORMAT_VERSION, DEFAULT_TABLE_FORMAT_VERSION);
        Map<String, String> tableProperties = new HashMap<>();
        tableProperties.put("format-version", formatVersion);
        tableProperties.put("write.format.default", "parquet");
        tableProperties.put("write.parquet.compression-codec", "snappy");
        tableProperties.put("write.target-file-size-bytes", Long.toString(DEFAULT_TARGET_FILE_SIZE));
        tableProperties.put("write.delete.mode", "merge-on-read");
        tableProperties.put("write.update.mode", "merge-on-read");
        tableProperties.put("write.merge.mode", "merge-on-read");
        tableProperties.put("write.upsert.enabled", "true");

        try {
            catalog.createTable(tableId, schema, partitionSpec, tableProperties);
            LOG.info("Created table {} (format-version={})", tableId, formatVersion);
        } catch (AlreadyExistsException e) {
            LOG.info("Table {} was created by another process concurrently", tableId);
        }
    }

    private static Schema createOrderEventSchema() {
        return new Schema(
                Types.NestedField.required(1, "event_id", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "event_type", Types.StringType.get()),
                Types.NestedField.required(4, "region", Types.StringType.get()),
                Types.NestedField.required(5, "event_date", Types.DateType.get()),
                Types.NestedField.required(6, "order_id", Types.StringType.get()),
                Types.NestedField.required(7, "customer_id", Types.StringType.get()),
                Types.NestedField.required(8, "amount", Types.DecimalType.of(18, 2)),
                Types.NestedField.required(9, "currency", Types.StringType.get()),
                Types.NestedField.required(10, "status", Types.StringType.get()),
                Types.NestedField.optional(11, "metadata", Types.MapType.ofOptional(
                        12, 13,
                        Types.StringType.get(),
                        Types.StringType.get())));
    }

    private static RowType orderEventRowType() {
        return RowType.of(
                new LogicalType[]{
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new TimestampType(6),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new DateType(),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new DecimalType(18, 2),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new VarCharType(VarCharType.MAX_LENGTH),
                        new MapType(
                                new VarCharType(VarCharType.MAX_LENGTH),
                                new VarCharType(VarCharType.MAX_LENGTH))},
                new String[]{
                        "event_id", "event_time", "event_type", "region", "event_date",
                        "order_id", "customer_id", "amount", "currency", "status", "metadata"});
    }

    // ------------------------------------------------------------------------
    // Sink configuration
    // ------------------------------------------------------------------------

    private static void configureIcebergSink(
            DataStream<RowData> rowDataStream,
            TableLoader tableLoader,
            Map<String, String> config,
            StreamExecutionEnvironment env) {

        String writeMode = config.getOrDefault(WRITE_MODE, DEFAULT_WRITE_MODE);
        boolean isUpsert = "upsert".equalsIgnoreCase(writeMode);
        List<String> equalityFields = Arrays.asList(
                config.getOrDefault(PRIMARY_KEY_COLUMNS, DEFAULT_PK_COLUMNS).split(","));

        String branch = config.get(ICEBERG_BRANCH);
        boolean useBranch = branch != null && !branch.isEmpty();

        LOG.info("IcebergSink: writeMode={}{}{}",
                writeMode,
                isUpsert ? " (equality fields: " + equalityFields + ")" : "",
                useBranch ? " branch=" + branch : "");

        IcebergSink.Builder sinkBuilder = IcebergSink.forRowData(rowDataStream)
                .tableLoader(tableLoader)
                .set("write.format.default", "parquet")
                .set("write.target-file-size-bytes", Long.toString(DEFAULT_TARGET_FILE_SIZE))
                .setSnapshotProperty("flink.job-id", "datastream-iceberg-job");

        if (isUpsert) {
            sinkBuilder
                    .upsert(true)
                    .equalityFieldColumns(equalityFields)
                    .set("write.delete.mode", "merge-on-read")
                    .set("write.update.mode", "merge-on-read")
                    .set("write.merge.mode", "merge-on-read")
                    .distributionMode(DistributionMode.HASH);
        } else {
            sinkBuilder
                    .upsert(false)
                    .distributionMode(DistributionMode.NONE);
        }

        if (useBranch) {
            sinkBuilder.toBranch(branch);
        }

        sinkBuilder.append();

        // Optional maintenance topology (skipped for S3 Tables, which handles this automatically).
        String catalogType = config.getOrDefault(ICEBERG_CATALOG_TYPE, DEFAULT_CATALOG_TYPE);
        boolean enableMaintenance = Boolean.parseBoolean(
                config.getOrDefault(ENABLE_MAINTENANCE, "false"));

        if ("s3tables".equalsIgnoreCase(catalogType)) {
            LOG.info("S3 Tables catalog — maintenance handled by the service");
        } else if (enableMaintenance) {
            setupTableMaintenance(env, tableLoader, createJdbcLockFactory(config));
        } else {
            LOG.info("Maintenance disabled");
        }
    }

    /**
     * Attach compaction, snapshot expiration, and orphan-file cleanup as Flink operators.
     * The JDBC lock prevents concurrent conflicting commits when multiple Flink jobs
     * (or restarts of the same job) target the same table.
     */
    private static void setupTableMaintenance(
            StreamExecutionEnvironment env,
            TableLoader tableLoader,
            TriggerLockFactory lockFactory) {
        LOG.info("Configuring table maintenance topology");

        try {
            TableMaintenance.forTable(env, tableLoader, lockFactory)
                    .uidSuffix("datastream-maintenance")
                    .rateLimit(Duration.ofMinutes(10))
                    .lockCheckDelay(Duration.ofSeconds(30))
                    .add(ExpireSnapshots.builder()
                            .scheduleOnCommitCount(10)
                            .maxSnapshotAge(Duration.ofHours(24))
                            .retainLast(5))
                    .add(RewriteDataFiles.builder()
                            .scheduleOnDataFileCount(20)
                            .targetFileSizeBytes(256L * 1024 * 1024)
                            .minFileSizeBytes(32L * 1024 * 1024)
                            .partialProgressEnabled(true)
                            .partialProgressMaxCommits(5)
                            .maxRewriteBytes(2L * 1024 * 1024 * 1024))
                    .add(DeleteOrphanFiles.builder()
                            .scheduleOnCommitCount(50)
                            .minAge(Duration.ofDays(3)))
                    .append();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to set up table maintenance", e);
        }
    }

    private static TriggerLockFactory createJdbcLockFactory(Map<String, String> config) {
        String jdbcUrl = config.get(RDS_JDBC_URL);
        if (jdbcUrl == null || jdbcUrl.isEmpty()) {
            throw new IllegalArgumentException(
                    RDS_JDBC_URL + " is required when " + ENABLE_MAINTENANCE + "=true");
        }

        String user = config.get(RDS_USER);
        String password = config.get(RDS_PASSWORD);
        String lockId = config.getOrDefault(ICEBERG_DATABASE, DEFAULT_DATABASE)
                + "." + config.getOrDefault(ICEBERG_TABLE, DEFAULT_TABLE);

        LOG.info("JDBC lock: url={}, lockId={}", jdbcUrl, lockId);

        Map<String, String> jdbcProps = new HashMap<>();
        if (user != null && !user.isEmpty()) {
            jdbcProps.put("jdbc.user", user);
        }
        if (password != null && !password.isEmpty()) {
            jdbcProps.put("jdbc.password", password);
        }
        jdbcProps.put("flink-maintenance.lock.jdbc.init-lock-tables", "true");

        TriggerLockFactory lockFactory = new JdbcLockFactory(jdbcUrl, lockId, jdbcProps);
        try {
            lockFactory.open();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to initialise JDBC lock factory", e);
        }
        return lockFactory;
    }
}
