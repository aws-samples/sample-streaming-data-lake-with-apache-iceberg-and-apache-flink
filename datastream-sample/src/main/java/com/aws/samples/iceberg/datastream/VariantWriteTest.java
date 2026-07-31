package com.aws.samples.iceberg.datastream;

import com.aws.samples.iceberg.config.IcebergConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.variant.Variant;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.IcebergSink;
import org.apache.iceberg.types.Types;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Variant write test: creates a V3 table {db}.{table} with a Types.VariantType column and
 * writes one row whose payload is a Flink org.apache.flink.types.variant.Variant, via IcebergSink.
 * Proves the Flink Variant -> Iceberg Variant (FlinkParquetWriters.VariantWriter) write path.
 */
public class VariantWriteTest {
    public static void main(String[] args) throws Exception {
        Properties p = System.getProperties();
        String db = p.getProperty("iceberg.database");
        String tbl = p.getProperty("iceberg.table", "variant_test");
        CatalogLoader cl = IcebergConfig.createCatalogLoader("glue", p);
        Catalog cat = cl.loadCatalog();
        TableIdentifier id = TableIdentifier.of(db, tbl);

        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "payload", Types.VariantType.get()));
        if (cat.tableExists(id)) {
            cat.dropTable(id, true);
        }
        cat.createTable(id, schema, PartitionSpec.unpartitioned(), Map.of("format-version", "3"));

        Variant v = Variant.newBuilder().object()
                .add("tier", Variant.newBuilder().of("gold"))
                .add("points", Variant.newBuilder().of(100))
                .build();
        GenericRowData row = new GenericRowData(2);
        row.setField(0, 1L);
        row.setField(1, v);

        RowType rowType = FlinkSchemaUtil.convert(schema);
        TypeInformation<RowData> ti = InternalTypeInfo.of(rowType);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<RowData> stream = env.fromCollection(Collections.singletonList((RowData) row), ti);
        IcebergSink.forRowData(stream)
                .tableLoader(TableLoader.fromCatalog(cl, id))
                .set("write.format.default", "parquet")
                .append();
        env.execute("variant-write");
        System.out.println("VARIANT_WRITE_DONE table=" + db + "." + tbl);
    }
}
