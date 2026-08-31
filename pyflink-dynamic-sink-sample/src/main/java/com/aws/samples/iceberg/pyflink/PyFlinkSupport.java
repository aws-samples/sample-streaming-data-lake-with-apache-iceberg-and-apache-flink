/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 */
package com.aws.samples.iceberg.pyflink;

import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.CatalogLoader;

/**
 * Static factories for the pieces a PyFlink job cannot construct via Py4J after shading.
 *
 * <p>The fat jar relocates Hadoop classes (see the parent pom's shade configuration), so
 * {@code org.apache.hadoop.conf.Configuration} does not exist under its original name at runtime.
 * Compiled references in this class are rewritten by the shade plugin automatically; Python code
 * calls these factories instead of naming Hadoop classes directly.
 */
public final class PyFlinkSupport {

    private PyFlinkSupport() {}

    /** CatalogLoader backed by the AWS Glue Data Catalog (MSF mode). */
    public static CatalogLoader glueCatalogLoader(String catalogName, String warehouse, String region) {
        Map<String, String> props = new HashMap<>();
        props.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.aws.glue.GlueCatalog");
        props.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        props.put("client.region", region);
        props.put("glue.region", region);
        props.put("s3.region", region);
        return CatalogLoader.custom(
                catalogName, props, new Configuration(), "org.apache.iceberg.aws.glue.GlueCatalog");
    }

    /** CatalogLoader backed by a filesystem HadoopCatalog (local mode). */
    public static CatalogLoader hadoopCatalogLoader(String warehouse) {
        Map<String, String> props = new HashMap<>();
        props.put("warehouse", warehouse);
        return CatalogLoader.hadoop("hadoop", new Configuration(), props);
    }

    /**
     * Committed row count of a table in a filesystem HadoopCatalog warehouse, read from the
     * current snapshot's {@code total-records} summary (metadata-only; local-mode verification).
     */
    public static long countRows(String warehouse, String database, String table) {
        try (org.apache.iceberg.hadoop.HadoopCatalog catalog =
                new org.apache.iceberg.hadoop.HadoopCatalog(new Configuration(), warehouse)) {
            org.apache.iceberg.Table t =
                    catalog.loadTable(
                            org.apache.iceberg.catalog.TableIdentifier.of(database, table));
            org.apache.iceberg.Snapshot snapshot = t.currentSnapshot();
            if (snapshot == null) {
                return 0;
            }
            return Long.parseLong(snapshot.summary().getOrDefault("total-records", "0"));
        } catch (java.io.IOException e) {
            throw new RuntimeException("Failed reading " + database + "." + table, e);
        }
    }
}
