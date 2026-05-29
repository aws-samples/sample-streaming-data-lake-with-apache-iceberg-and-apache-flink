package com.aws.samples.iceberg.variant;

import com.aws.samples.iceberg.config.IcebergConfig;
import com.aws.samples.iceberg.runtime.AppProperties;
import com.aws.samples.iceberg.runtime.Checkpointing;
import com.aws.samples.iceberg.runtime.FlinkEnvironments;
import com.aws.samples.iceberg.runtime.KinesisSources;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kinesis.source.KinesisStreamsSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

/**
 * Streams JSON events from Kinesis into an Iceberg V3 table, storing the full (schema-less)
 * payload in a single {@code variant} column alongside a few typed identifier columns.
 *
 * <p>This is the "one story" for Variant: the producer's arbitrary JSON is converted into a
 * Flink {@link Variant} ({@code org.apache.flink.types.variant}) and written through
 * {@link IcebergSink} into an Iceberg {@link Types.VariantType} column (format-version 3) —
 * no flattening, no fixed schema for the nested data, full ACID/partitioning on the table.
 */
public final class VariantSinkJob {

    private static final Logger LOG = LoggerFactory.getLogger(VariantSinkJob.class);
    private static final int LOCAL_WEB_UI_PORT = 8087;
    private static final String DEFAULT_AWS_REGION = "us-east-1";

    /** Typed identifier columns + one flexible {@code payload} variant column. */
    private static final Schema SCHEMA = new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.optional(2, "event_type", Types.StringType.get()),
            Types.NestedField.optional(3, "event_date", Types.StringType.get()),
            Types.NestedField.optional(4, "payload", Types.VariantType.get()));

    private VariantSinkJob() {}

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
        Map<String, String> config = AppProperties.loadAsMap(env);

        if (FlinkEnvironments.isLocal(env)) {
            long interval = Long.parseLong(config.getOrDefault(
                    "checkpoint.interval.ms", Long.toString(Checkpointing.DEFAULT_INTERVAL_MS)));
            Checkpointing.configureLocalDefaults(env, interval);
        }

        String database = config.getOrDefault("iceberg.database", "iceberg_variant");
        String table = config.getOrDefault("iceberg.table", "events_variant");
        String streamArn = config.get("kinesis.stream.arn");
        if (streamArn == null || streamArn.isEmpty()) {
            throw new IllegalArgumentException("kinesis.stream.arn is required");
        }
        String region = config.getOrDefault("kinesis.region",
                config.getOrDefault("aws.region", DEFAULT_AWS_REGION));

        CatalogLoader catalogLoader = IcebergConfig.createCatalogLoader(config);
        TableIdentifier tableId = TableIdentifier.of(database, table);
        ensureTable(catalogLoader, tableId);

        KinesisStreamsSource<JsonNode> source =
                KinesisSources.create(streamArn, region, new JsonNodeDeserializer());
        DataStream<JsonNode> events = env.fromSource(
                        source, WatermarkStrategy.noWatermarks(),
                        "Kinesis Source (JSON)", TypeInformation.of(JsonNode.class))
                .uid("kinesis-source-variant");

        RowType rowType = FlinkSchemaUtil.convert(SCHEMA);
        DataStream<RowData> rows = events
                .map(new ToVariantRow())
                .uid("json-to-variant")
                .name("JSON to Iceberg Variant")
                .returns(InternalTypeInfo.of(rowType));

        IcebergSink.forRowData(rows)
                .tableLoader(TableLoader.fromCatalog(catalogLoader, tableId))
                .set("write.format.default", "parquet")
                .uidSuffix("variant-iceberg-sink")
                .append();

        env.execute("Variant Sink Job - JSON to Iceberg Variant");
    }

    private static void ensureTable(CatalogLoader catalogLoader, TableIdentifier tableId) {
        Catalog catalog = catalogLoader.loadCatalog();
        if (!catalog.tableExists(tableId)) {
            catalog.createTable(tableId, SCHEMA, PartitionSpec.unpartitioned(),
                    Map.of("format-version", "3", "write.format.default", "parquet"));
            LOG.info("Created V3 variant table {}", tableId);
        }
    }

    /** Maps a raw JSON event to a RowData with the whole record packed into a Flink Variant. */
    static final class ToVariantRow implements MapFunction<JsonNode, RowData> {
        private static final long serialVersionUID = 1L;

        @Override
        public RowData map(JsonNode json) {
            GenericRowData row = new GenericRowData(4);
            String eventId = json.hasNonNull("event_id")
                    ? json.get("event_id").asText() : UUID.randomUUID().toString();
            row.setField(0, StringData.fromString(eventId));
            row.setField(1, json.hasNonNull("event_type")
                    ? StringData.fromString(json.get("event_type").asText()) : null);
            row.setField(2, json.hasNonNull("event_date")
                    ? StringData.fromString(json.get("event_date").asText()) : null);
            row.setField(3, toVariant(Variant.newBuilder(), json));
            return row;
        }
    }

    /** Recursively convert any JSON node into a Flink {@link Variant}. */
    static Variant toVariant(VariantBuilder builder, JsonNode node) {
        if (node == null || node.isNull()) {
            return builder.ofNull();
        }
        if (node.isObject()) {
            VariantBuilder.VariantObjectBuilder obj = builder.object();
            Iterator<Map.Entry<String, JsonNode>> it = node.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> e = it.next();
                obj.add(e.getKey(), toVariant(builder, e.getValue()));
            }
            return obj.build();
        }
        if (node.isArray()) {
            VariantBuilder.VariantArrayBuilder arr = builder.array();
            for (JsonNode element : node) {
                arr.add(toVariant(builder, element));
            }
            return arr.build();
        }
        if (node.isBoolean()) {
            return builder.of(node.booleanValue());
        }
        if (node.isIntegralNumber()) {
            return builder.of(node.longValue());
        }
        if (node.isNumber()) {
            return builder.of(node.doubleValue());
        }
        return builder.of(node.asText());
    }
}
