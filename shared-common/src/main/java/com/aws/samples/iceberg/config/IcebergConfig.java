package com.aws.samples.iceberg.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.s3tables.iceberg.S3TablesCatalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utility class for configuring Apache Iceberg with AWS Glue Catalog or S3 Tables Catalog.
 * Supports both local development and AWS Managed Flink environments.
 */
public class IcebergConfig {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergConfig.class);

    // Environment variable keys
    private static final String ENV_WAREHOUSE = "ICEBERG_WAREHOUSE";
    private static final String ENV_CATALOG_NAME = "ICEBERG_CATALOG_NAME";
    private static final String ENV_AWS_REGION = "AWS_REGION";
    private static final String ENV_GLUE_DATABASE = "GLUE_DATABASE";

    // Default values
    private static final String DEFAULT_WAREHOUSE = "s3://iceberg-warehouse/warehouse";
    private static final String DEFAULT_CATALOG_NAME = "glue_catalog";
    private static final String DEFAULT_AWS_REGION = "us-east-1";
    private static final String DEFAULT_GLUE_DATABASE = "iceberg_samples";

    private final String warehousePath;
    private final String catalogName;
    private final String awsRegion;
    private final String glueDatabase;

    public IcebergConfig() {
        this.warehousePath = getEnvOrDefault(ENV_WAREHOUSE, DEFAULT_WAREHOUSE);
        this.catalogName = getEnvOrDefault(ENV_CATALOG_NAME, DEFAULT_CATALOG_NAME);
        this.awsRegion = getEnvOrDefault(ENV_AWS_REGION, DEFAULT_AWS_REGION);
        this.glueDatabase = getEnvOrDefault(ENV_GLUE_DATABASE, DEFAULT_GLUE_DATABASE);
        LOG.info("Initialized IcebergConfig: warehouse={}, catalog={}, region={}, database={}",
                warehousePath, catalogName, awsRegion, glueDatabase);
    }

    public IcebergConfig(String warehousePath, String catalogName, String awsRegion, String glueDatabase) {
        this.warehousePath = warehousePath;
        this.catalogName = catalogName;
        this.awsRegion = awsRegion;
        this.glueDatabase = glueDatabase;
        LOG.info("Initialized IcebergConfig: warehouse={}, catalog={}, region={}, database={}",
                warehousePath, catalogName, awsRegion, glueDatabase);
    }

    /**
     * Get catalog properties for Glue Catalog with S3FileIO.
     */
    public Map<String, String> getCatalogProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName());
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
        properties.put("client.region", awsRegion);
        return properties;
    }

    /**
     * Create a CatalogLoader for Glue catalog (default).
     */
    public CatalogLoader createCatalogLoader() {
        return createGlueCatalogLoader(catalogName, warehousePath, awsRegion);
    }

    /**
     * Create a CatalogLoader based on catalog type, reading config from a properties map.
     * This is the unified factory method that supports both Glue and S3 Tables.
     *
     * Expected keys:
     *   - iceberg.catalog.name (optional, defaults to glue_catalog)
     *   - iceberg.catalog.type (optional, defaults to glue)
     *   - iceberg.warehouse (required for glue)
     *   - aws.region (optional, defaults to us-east-1)
     *   - s3tables.bucket.arn (required for s3tables)
     */
    public static CatalogLoader createCatalogLoader(Map<String, String> config) {
        String catalogName = config.getOrDefault("iceberg.catalog.name", "glue_catalog");
        String catalogType = config.getOrDefault("iceberg.catalog.type", "glue");
        String region = config.getOrDefault("aws.region", "us-east-1");

        if ("s3tables".equalsIgnoreCase(catalogType)) {
            String bucketArn = config.get("s3tables.bucket.arn");
            if (bucketArn == null || bucketArn.isEmpty()) {
                throw new IllegalArgumentException("S3 Tables bucket ARN is required when using S3 Tables catalog");
            }
            return createS3TablesCatalogLoader(catalogName, bucketArn, region);
        } else {
            String warehouse = config.get("iceberg.warehouse");
            if (warehouse == null || warehouse.isEmpty()) {
                throw new IllegalArgumentException("Iceberg warehouse path is required for Glue catalog");
            }
            return createGlueCatalogLoader(catalogName, warehouse, region);
        }
    }

    /**
     * Overload accepting Properties (for jobs that use Properties instead of Map).
     */
    public static CatalogLoader createCatalogLoader(String catalogType, Properties props) {
        String region = props.getProperty("aws.region", "us-east-1");

        if ("s3tables".equalsIgnoreCase(catalogType)) {
            String bucketArn = props.getProperty("s3tables.bucket.arn");
            if (bucketArn == null || bucketArn.isEmpty()) {
                throw new IllegalArgumentException("S3 Tables bucket ARN is required when using S3 Tables catalog");
            }
            return createS3TablesCatalogLoader("s3tables_catalog", bucketArn, region);
        } else {
            String warehouse = props.getProperty("iceberg.warehouse");
            if (warehouse == null || warehouse.isEmpty()) {
                throw new IllegalArgumentException("Iceberg warehouse path is required for Glue catalog");
            }
            return createGlueCatalogLoader("glue_catalog", warehouse, region);
        }
    }

    private static CatalogLoader createGlueCatalogLoader(String catalogName, String warehouse, String region) {
        LOG.info("Creating Glue CatalogLoader: name={}, warehouse={}, region={}", catalogName, warehouse, region);
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName());
        catalogProps.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        catalogProps.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        catalogProps.put("client.region", region);
        catalogProps.put("glue.region", region);
        catalogProps.put("s3.region", region);
        return CatalogLoader.custom(catalogName, catalogProps, new Configuration(), GlueCatalog.class.getName());
    }

    private static CatalogLoader createS3TablesCatalogLoader(String catalogName, String bucketArn, String region) {
        LOG.info("Creating S3 Tables CatalogLoader: name={}, bucket={}, region={}", catalogName, bucketArn, region);
        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put(CatalogProperties.CATALOG_IMPL, S3TablesCatalog.class.getName());
        catalogProps.put("s3tables.catalog.client.region", region);
        catalogProps.put("warehouse", bucketArn);
        catalogProps.put("client.region", region);
        return CatalogLoader.custom(catalogName, catalogProps, new Configuration(), S3TablesCatalog.class.getName());
    }

    /**
     * Create a TableLoader for a specific table.
     */
    public TableLoader createTableLoader(String tableName) {
        TableIdentifier tableId = parseTableIdentifier(tableName);
        CatalogLoader catalogLoader = createCatalogLoader();
        LOG.info("Creating TableLoader for table: {}", tableId);
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

    public TableLoader createTableLoader(String database, String table) {
        TableIdentifier tableId = TableIdentifier.of(database, table);
        CatalogLoader catalogLoader = createCatalogLoader();
        LOG.info("Creating TableLoader for table: {}", tableId);
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }

    /**
     * Create a Catalog instance for direct catalog operations.
     */
    public Catalog createCatalog() {
        Map<String, String> catalogProps = getCatalogProperties();
        Configuration hadoopConf = new Configuration();
        GlueCatalog catalog = new GlueCatalog();
        catalog.setConf(hadoopConf);
        catalog.initialize(catalogName, catalogProps);
        LOG.info("Created Glue Catalog: {}", catalogName);
        return catalog;
    }

    private TableIdentifier parseTableIdentifier(String tableName) {
        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            return TableIdentifier.of(parts[0], parts[1]);
        }
        return TableIdentifier.of(glueDatabase, tableName);
    }

    private String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }

    public String getWarehousePath() { return warehousePath; }
    public String getCatalogName() { return catalogName; }
    public String getAwsRegion() { return awsRegion; }
    public String getGlueDatabase() { return glueDatabase; }

    @Override
    public String toString() {
        return "IcebergConfig{warehousePath='" + warehousePath + "', catalogName='" + catalogName +
               "', awsRegion='" + awsRegion + "', glueDatabase='" + glueDatabase + "'}";
    }
}
