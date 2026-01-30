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

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for configuring Apache Iceberg with AWS Glue Catalog and S3FileIO.
 * Supports both local development and AWS Managed Flink environments.
 * 
 * Configuration is read from environment variables:
 * - ICEBERG_WAREHOUSE: S3 warehouse path (default: s3://iceberg-warehouse/warehouse)
 * - ICEBERG_CATALOG_NAME: Catalog name (default: glue_catalog)
 * - AWS_REGION: AWS region (default: us-east-1)
 * - GLUE_DATABASE: Glue database name (default: iceberg_samples)
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
    
    /**
     * Create IcebergConfig with default values from environment variables.
     */
    public IcebergConfig() {
        this.warehousePath = getEnvOrDefault(ENV_WAREHOUSE, DEFAULT_WAREHOUSE);
        this.catalogName = getEnvOrDefault(ENV_CATALOG_NAME, DEFAULT_CATALOG_NAME);
        this.awsRegion = getEnvOrDefault(ENV_AWS_REGION, DEFAULT_AWS_REGION);
        this.glueDatabase = getEnvOrDefault(ENV_GLUE_DATABASE, DEFAULT_GLUE_DATABASE);
        
        LOG.info("Initialized IcebergConfig: warehouse={}, catalog={}, region={}, database={}",
                warehousePath, catalogName, awsRegion, glueDatabase);
    }
    
    /**
     * Create IcebergConfig with custom values.
     */
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
        
        // Catalog implementation
        properties.put(CatalogProperties.CATALOG_IMPL, GlueCatalog.class.getName());
        
        // S3 FileIO implementation
        properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
        
        // Warehouse location
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehousePath);
        
        // AWS region - use the property key directly as string
        properties.put("client.region", awsRegion);
        
        return properties;
    }
    
    /**
     * Create a CatalogLoader for Flink integration.
     * This is used by Flink's IcebergSink and table operations.
     */
    public CatalogLoader createCatalogLoader() {
        Map<String, String> catalogProps = getCatalogProperties();
        Configuration hadoopConf = new Configuration();
        
        LOG.info("Creating CatalogLoader with properties: {}", catalogProps);
        
        return CatalogLoader.custom(
            catalogName,
            catalogProps,
            hadoopConf,
            GlueCatalog.class.getName()
        );
    }
    
    /**
     * Create a TableLoader for a specific table.
     * This is used by Flink's IcebergSink for writing to a table.
     * 
     * @param tableName Fully qualified table name (e.g., "database.table")
     */
    public TableLoader createTableLoader(String tableName) {
        TableIdentifier tableId = parseTableIdentifier(tableName);
        CatalogLoader catalogLoader = createCatalogLoader();
        
        LOG.info("Creating TableLoader for table: {}", tableId);
        
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }
    
    /**
     * Create a TableLoader for a specific table with explicit database and table names.
     * 
     * @param database Database name
     * @param table Table name
     */
    public TableLoader createTableLoader(String database, String table) {
        TableIdentifier tableId = TableIdentifier.of(database, table);
        CatalogLoader catalogLoader = createCatalogLoader();
        
        LOG.info("Creating TableLoader for table: {}", tableId);
        
        return TableLoader.fromCatalog(catalogLoader, tableId);
    }
    
    /**
     * Create a Catalog instance for direct catalog operations.
     * Useful for table creation, schema evolution, and metadata queries.
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
    
    /**
     * Parse a table identifier from a string.
     * Supports formats: "table", "database.table"
     */
    private TableIdentifier parseTableIdentifier(String tableName) {
        if (tableName.contains(".")) {
            String[] parts = tableName.split("\\.", 2);
            return TableIdentifier.of(parts[0], parts[1]);
        } else {
            return TableIdentifier.of(glueDatabase, tableName);
        }
    }
    
    /**
     * Get environment variable or return default value.
     */
    private String getEnvOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
    
    // Getters
    
    public String getWarehousePath() {
        return warehousePath;
    }
    
    public String getCatalogName() {
        return catalogName;
    }
    
    public String getAwsRegion() {
        return awsRegion;
    }
    
    public String getGlueDatabase() {
        return glueDatabase;
    }
    
    @Override
    public String toString() {
        return "IcebergConfig{" +
               "warehousePath='" + warehousePath + '\'' +
               ", catalogName='" + catalogName + '\'' +
               ", awsRegion='" + awsRegion + '\'' +
               ", glueDatabase='" + glueDatabase + '\'' +
               '}';
    }
}
