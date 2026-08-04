package com.aws.samples.iceberg.sqldynamic;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.apache.iceberg.flink.sink.dynamic.DynamicTableRecordGenerator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SQL-side dynamic router (Iceberg 1.11+). Referenced by class name from the sink table DDL:
 *
 * <pre>
 *   'use-dynamic-iceberg-sink' = 'true',
 *   'dynamic-record-generator-impl' = 'com.aws.samples.iceberg.sqldynamic.SqlRoutingGenerator'
 * </pre>
 *
 * <p>The Iceberg connector instantiates this class reflectively with the sink table's
 * {@link RowType} (the SQL schema of the INSERT). For each {@link RowData} it reads the
 * {@code event_type} column, projects the columns relevant to that event type, and emits a
 * {@link DynamicRecord} targeting {@code <event_type>_events} in {@link #DATABASE}. Tables are
 * created on first write, partitioned by identity(event_date) and identity(region).
 *
 * <p>The reflective contract only passes the RowType, so the routing configuration
 * (database name, per-type column subsets) is defined here rather than in job properties.
 */
public class SqlRoutingGenerator extends DynamicTableRecordGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(SqlRoutingGenerator.class);

    /** Must match the CDK-provisioned Glue database for appType=sql-dynamic. */
    public static final String DATABASE = "iceberg_sql_dynamic";

    private static final String ROUTING_COLUMN = "event_type";
    private static final List<String> COMMON_COLUMNS =
            Arrays.asList("event_id", "event_time", "event_type", "region", "event_date");
    private static final Map<String, List<String>> TYPE_COLUMNS = new LinkedHashMap<>();
    static {
        TYPE_COLUMNS.put("order", Arrays.asList("order_id", "customer_id", "amount", "currency", "status"));
        TYPE_COLUMNS.put("user", Arrays.asList("user_id", "action", "device_type", "ip_address", "user_agent"));
        TYPE_COLUMNS.put("click", Arrays.asList("session_id", "page_url", "referrer", "scroll_depth", "time_on_page_seconds"));
    }
    private static final List<String> PARTITION_COLUMNS = Arrays.asList("event_date", "region");

    private final int routingFieldIndex;
    // Per-event-type routing state, built lazily and cached (generate() is single-threaded per subtask).
    private final Map<String, Route> routes = new ConcurrentHashMap<>();

    /** Reflective contract: the Iceberg connector calls this with the sink table's RowType. */
    public SqlRoutingGenerator(RowType rowType) {
        super(rowType);
        this.routingFieldIndex = rowType.getFieldIndex(ROUTING_COLUMN);
        if (routingFieldIndex < 0) {
            throw new IllegalArgumentException(
                    "Sink schema must contain routing column '" + ROUTING_COLUMN + "': " + rowType);
        }
        LOG.info("SqlRoutingGenerator created for schema with {} fields", rowType.getFieldCount());
    }

    @Override
    public void generate(RowData row, Collector<DynamicRecord> out) {
        if (row.isNullAt(routingFieldIndex)) {
            LOG.warn("Skipping record with null {}", ROUTING_COLUMN);
            return;
        }
        String eventType = row.getString(routingFieldIndex).toString().toLowerCase();
        Route route = routes.computeIfAbsent(eventType, this::buildRoute);
        if (route == null) {
            return; // unknown type already logged
        }

        GenericRowData projected = new GenericRowData(route.fieldGetters.length);
        for (int i = 0; i < route.fieldGetters.length; i++) {
            projected.setField(i, route.fieldGetters[i].getFieldOrNull(row));
        }

        out.collect(new DynamicRecord(
                route.tableId,
                "main",
                route.schema,
                projected,
                route.spec,
                DistributionMode.HASH,
                2));
    }

    /** Resolve column subset -> field getters, Iceberg schema, and partition spec for one event type. */
    private Route buildRoute(String eventType) {
        List<String> extra = TYPE_COLUMNS.get(eventType);
        if (extra == null) {
            LOG.warn("Unknown event_type '{}' — record skipped (known: {})", eventType, TYPE_COLUMNS.keySet());
            return null;
        }
        List<String> columns = new ArrayList<>(COMMON_COLUMNS);
        columns.addAll(extra);

        RowType rowType = rowType();
        RowData.FieldGetter[] getters = new RowData.FieldGetter[columns.size()];
        List<Types.NestedField> fields = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            String col = columns.get(i);
            int idx = rowType.getFieldIndex(col);
            if (idx < 0) {
                throw new IllegalArgumentException("Sink schema is missing expected column: " + col);
            }
            LogicalType type = rowType.getTypeAt(idx);
            getters[i] = RowData.createFieldGetter(type, idx);
            Type icebergType = FlinkSchemaUtil.convert(type);
            fields.add(Types.NestedField.optional(i + 1, col, icebergType));
        }
        Schema schema = new Schema(fields);

        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
        for (String partitionColumn : PARTITION_COLUMNS) {
            if (schema.findField(partitionColumn) != null) {
                specBuilder.identity(partitionColumn);
            }
        }

        TableIdentifier tableId = TableIdentifier.of(DATABASE, eventType + "_events");
        LOG.info("Route created: {} -> {} ({} columns)", eventType, tableId, columns.size());
        return new Route(tableId, schema, specBuilder.build(), getters);
    }

    private static final class Route {
        final TableIdentifier tableId;
        final Schema schema;
        final PartitionSpec spec;
        final RowData.FieldGetter[] fieldGetters;

        Route(TableIdentifier tableId, Schema schema, PartitionSpec spec, RowData.FieldGetter[] fieldGetters) {
            this.tableId = tableId;
            this.schema = schema;
            this.spec = spec;
            this.fieldGetters = fieldGetters;
        }
    }
}
