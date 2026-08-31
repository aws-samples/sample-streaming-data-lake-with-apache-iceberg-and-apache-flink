package com.aws.samples.iceberg.pyflink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecordGenerator;
import org.apache.iceberg.types.Types;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * A compiled-Java {@link DynamicRecordGenerator} that a PyFlink job wires into
 * Iceberg's {@code DynamicIcebergSink} via Py4J.
 *
 * <p>Why this class is Java and not Python: {@code DynamicRecordGenerator} is
 * invoked per record inside the sink's Java operator ({@code
 * DynamicRecordProcessor}) on Flink task threads. A Py4J Python proxy cannot be
 * serialized into the Flink job graph, so the routing logic has to be a compiled
 * Java class on the classpath. On Amazon Managed Service for Apache Flink (MSF)
 * this class ships inside the application fat jar; the {@code jarfile} run option
 * places that jar on the PyFlink gateway classpath.
 *
 * <p>Behavior: parse each input String as JSON, build a row against a FIXED
 * Iceberg schema aligned to the events emitted by the shared {@code
 * data-generator} module ({@code event_id}, {@code event_type}, {@code
 * event_time}, {@code region}, {@code event_date}), and route it to {@code
 * <database>.<event_type_lowercase><tableSuffix>}. Only event types on the
 * allowlist are routed; every other type is dropped (never collected). The
 * allowlist is supplied as a comma-separated String constructor argument so a
 * PyFlink job can construct the generator over Py4J without building a Java
 * collection.
 *
 * <p>The schema is fixed and JSON-only. There is no Glue Schema Registry
 * dependency anywhere in this module.
 */
public class FixedSchemaRoutingGenerator implements DynamicRecordGenerator<String> {

    private static final long serialVersionUID = 1L;

    /**
     * Fixed schema aligned to the common fields every {@code data-generator}
     * event carries (see {@code BaseEvent}). Field IDs are stable and explicit,
     * as Iceberg requires. {@code event_id} and {@code event_type} are required;
     * the rest are optional so records missing an occasional field still route.
     */
    public static final Schema EVENT_SCHEMA = new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "event_type", Types.StringType.get()),
            Types.NestedField.optional(3, "event_time", Types.StringType.get()),
            Types.NestedField.optional(4, "region", Types.StringType.get()),
            Types.NestedField.optional(5, "event_date", Types.StringType.get()));

    private final Set<String> allowed;
    private final String database;
    private final String tableSuffix;

    // ObjectMapper is created lazily and kept transient so the generator itself
    // stays trivially serializable into the Flink job graph.
    private transient ObjectMapper mapper;

    /**
     * @param database    target namespace (e.g. {@code iceberg_pyflink_dynamic})
     * @param allowedCsv  comma-separated allowlist of event types, matched
     *                    case-insensitively (e.g. {@code "order,user,click"})
     * @param tableSuffix suffix appended to the lowercased event type to form the
     *                    table name (e.g. {@code "_events"} -> {@code order_events})
     */
    public FixedSchemaRoutingGenerator(String database, String allowedCsv, String tableSuffix) {
        this.database = database;
        this.tableSuffix = tableSuffix;
        Set<String> parsed = new LinkedHashSet<>();
        if (allowedCsv != null) {
            for (String token : allowedCsv.split(",")) {
                String trimmed = token.trim().toLowerCase();
                if (!trimmed.isEmpty()) {
                    parsed.add(trimmed);
                }
            }
        }
        this.allowed = parsed.isEmpty()
                ? Collections.emptySet()
                : Collections.unmodifiableSet(new HashSet<>(parsed));
    }

    private ObjectMapper mapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        return mapper;
    }

    @Override
    public void generate(String value, Collector<DynamicRecord> out) throws Exception {
        JsonNode node = mapper().readTree(value);
        if (node == null || !node.isObject()) {
            return;
        }

        String eventType = node.path("event_type").asText(null);
        if (eventType == null || !allowed.contains(eventType.toLowerCase())) {
            // Not on the allowlist (or missing type): drop.
            return;
        }

        String tableName = eventType.toLowerCase() + tableSuffix;
        TableIdentifier tableId = TableIdentifier.of(database, tableName);

        GenericRowData row = new GenericRowData(RowKind.INSERT, 5);
        row.setField(0, StringData.fromString(node.path("event_id").asText(null)));
        row.setField(1, StringData.fromString(eventType));
        row.setField(2, textOrNull(node, "event_time"));
        row.setField(3, textOrNull(node, "region"));
        row.setField(4, textOrNull(node, "event_date"));

        out.collect(new DynamicRecord(
                tableId,
                // null branch = write to the default main branch. An explicit "main"
                // makes every parallel subtask race to create the ref on a freshly
                // created table; the losers throw "Ref main already exists"
                // (TableUpdater#findOrCreateBranch only tolerates CommitFailedException)
                // and the Kinesis source escalates the failure to a global restart.
                null,
                EVENT_SCHEMA,
                row,
                PartitionSpec.unpartitioned(),
                DistributionMode.NONE,
                1));
    }

    private static StringData textOrNull(JsonNode node, String field) {
        JsonNode child = node.get(field);
        if (child == null || child.isNull()) {
            return null;
        }
        return StringData.fromString(child.asText());
    }

    /** The event types this generator routes (unmodifiable, lowercased). */
    public Set<String> allowedTypes() {
        return allowed;
    }

    @Override
    public String toString() {
        return "FixedSchemaRoutingGenerator{database='" + database
                + "', tableSuffix='" + tableSuffix
                + "', allowed=" + new HashSet<>(allowed)
                + '}';
    }
}
