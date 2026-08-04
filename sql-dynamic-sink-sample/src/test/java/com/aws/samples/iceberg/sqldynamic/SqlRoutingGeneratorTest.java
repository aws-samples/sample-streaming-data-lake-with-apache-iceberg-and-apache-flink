package com.aws.samples.iceberg.sqldynamic;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.Collector;
import org.apache.iceberg.flink.sink.dynamic.DynamicRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for the SQL-side dynamic router. */
class SqlRoutingGeneratorTest {

    private static final List<String> COLUMNS = Arrays.asList(
            "event_id", "event_time", "event_type", "region", "event_date",
            "order_id", "customer_id", "amount", "currency", "status",
            "user_id", "action", "device_type", "ip_address", "user_agent",
            "session_id", "page_url", "referrer", "scroll_depth", "time_on_page_seconds");

    private RowType rowType;
    private SqlRoutingGenerator generator;
    private List<DynamicRecord> collected;
    private Collector<DynamicRecord> collector;

    @BeforeEach
    void setUp() {
        List<RowType.RowField> fields = new ArrayList<>();
        for (String col : COLUMNS) {
            fields.add(new RowType.RowField(col, typeFor(col)));
        }
        rowType = new RowType(fields);
        generator = new SqlRoutingGenerator(rowType);
        collected = new ArrayList<>();
        collector = new Collector<>() {
            @Override public void collect(DynamicRecord record) { collected.add(record); }
            @Override public void close() {}
        };
    }

    private static LogicalType typeFor(String col) {
        switch (col) {
            case "event_date": return new DateType();
            case "amount": return new DoubleType();
            case "scroll_depth": return new IntType();
            case "time_on_page_seconds": return new BigIntType();
            default: return VarCharType.STRING_TYPE;
        }
    }

    private GenericRowData rowWithType(String eventType) {
        GenericRowData row = new GenericRowData(COLUMNS.size());
        row.setField(COLUMNS.indexOf("event_id"), StringData.fromString("e-1"));
        row.setField(COLUMNS.indexOf("event_type"), StringData.fromString(eventType));
        row.setField(COLUMNS.indexOf("region"), StringData.fromString("eu-west-1"));
        row.setField(COLUMNS.indexOf("event_date"), (int) java.time.LocalDate.of(2026, 8, 4).toEpochDay());
        row.setField(COLUMNS.indexOf("order_id"), StringData.fromString("ORD-1"));
        row.setField(COLUMNS.indexOf("amount"), 42.0d);
        return row;
    }

    @Test
    void routesOrderToOrderEventsWithProjectedSchema() {
        generator.generate(rowWithType("ORDER"), collector);

        assertEquals(1, collected.size());
        DynamicRecord record = collected.get(0);
        assertEquals("iceberg_sql_dynamic.order_events", record.tableIdentifier().toString());
        assertEquals(10, record.schema().columns().size());
        assertNotNull(record.schema().findField("order_id"));
        assertNull(record.schema().findField("session_id"), "click columns must not leak into order table");
        RowData projected = record.rowData();
        assertEquals(10, projected.getArity());
    }

    @Test
    void partitionSpecUsesEventDateAndRegionIdentity() {
        generator.generate(rowWithType("order"), collector);
        DynamicRecord record = collected.get(0);
        assertEquals(2, record.spec().fields().size());
        assertTrue(record.spec().fields().stream().allMatch(f -> f.transform().isIdentity()));
    }

    @Test
    void unknownTypeAndNullTypeAreSkipped() {
        generator.generate(rowWithType("payment"), collector);
        GenericRowData nullType = rowWithType("order");
        nullType.setField(COLUMNS.indexOf("event_type"), null);
        generator.generate(nullType, collector);
        assertEquals(0, collected.size());
    }

    @Test
    void missingRoutingColumnFailsFast() {
        RowType noRouting = new RowType(List.of(new RowType.RowField("foo", VarCharType.STRING_TYPE)));
        assertThrows(IllegalArgumentException.class, () -> new SqlRoutingGenerator(noRouting));
    }
}
