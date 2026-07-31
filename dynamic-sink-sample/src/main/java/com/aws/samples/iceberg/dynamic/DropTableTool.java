package com.aws.samples.iceberg.dynamic;

import com.aws.samples.iceberg.config.IcebergConfig;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Properties;

/** Minimal Iceberg drop-table test: catalog.dropTable(id, purge=true). Config via -D. */
public class DropTableTool {
    public static void main(String[] args) {
        Properties p = System.getProperties();
        Catalog c = IcebergConfig.createCatalogLoader("glue", p).loadCatalog();
        boolean dropped = c.dropTable(
                TableIdentifier.of(p.getProperty("iceberg.database"), p.getProperty("iceberg.table")), true);
        System.out.println("DROP_TABLE_RESULT=" + dropped);
    }
}
