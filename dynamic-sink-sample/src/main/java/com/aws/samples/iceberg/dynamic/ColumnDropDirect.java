package com.aws.samples.iceberg.dynamic;

import com.aws.samples.iceberg.config.IcebergConfig;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;

import java.util.Properties;

/** Canonical column drop: table.updateSchema().deleteColumn(col).commit(). Config + -Dcolumn via -D. */
public class ColumnDropDirect {
    public static void main(String[] args) {
        Properties p = System.getProperties();
        Catalog c = IcebergConfig.createCatalogLoader("glue", p).loadCatalog();
        Table t = c.loadTable(TableIdentifier.of(p.getProperty("iceberg.database"), p.getProperty("iceberg.table")));
        String col = p.getProperty("column");
        t.updateSchema().deleteColumn(col).commit();
        System.out.println("COLUMN_DROPPED=" + col + " ; schema now: " + t.schema().columns());
    }
}
