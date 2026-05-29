package com.aws.samples.iceberg.source;

import com.aws.samples.iceberg.config.IcebergConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.CloseableIterator;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.IcebergSource;

import java.util.Properties;

/**
 * Minimal local BATCH read via Flink {@link IcebergSource} — counts current rows.
 * Batch mode applies positional deletes / Delete Vectors, so the count reflects the
 * true logical row count of a V3 upsert table (which Athena cannot read).
 * Config via -D system properties: iceberg.catalog.type, iceberg.database, iceberg.table,
 * iceberg.warehouse (glue) or s3tables.bucket.arn (s3tables), aws.region.
 */
public class IcebergCountJob {
    public static void main(String[] args) throws Exception {
        Properties p = System.getProperties();
        CatalogLoader cl = IcebergConfig.createCatalogLoader(
                p.getProperty("iceberg.catalog.type", "glue"), p);
        TableLoader tl = TableLoader.fromCatalog(cl,
                TableIdentifier.of(p.getProperty("iceberg.database"), p.getProperty("iceberg.table")));
        IcebergSource<RowData> src = IcebergSource.forRowData().tableLoader(tl).streaming(false).build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<RowData> ds = env.fromSource(
                src, WatermarkStrategy.noWatermarks(), "iceberg-batch-source", TypeInformation.of(RowData.class));

        long n = 0;
        try (CloseableIterator<RowData> it = ds.executeAndCollect()) {
            while (it.hasNext()) { it.next(); n++; }
        }
        System.out.println("ICEBERG_SOURCE_ROW_COUNT=" + n);
    }
}
