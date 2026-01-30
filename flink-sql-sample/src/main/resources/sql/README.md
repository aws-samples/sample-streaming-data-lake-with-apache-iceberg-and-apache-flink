# Iceberg Table DDL Scripts

This directory contains SQL DDL scripts for creating Iceberg tables with Apache Flink SQL.

## Table Schemas

### Orders Table (`create_orders_table.sql`)
Stores order events with the following key features:
- **Primary Key**: `event_id` for UPSERT operations
- **Partitioning**: By `event_date` and `region` for efficient querying
- **Sort Order**: By `event_time` and `event_id` for better read performance
- **Format**: Iceberg v2 with merge-on-read delete mode

### Users Table (`create_users_table.sql`)
Stores user activity events with the following key features:
- **Primary Key**: `event_id` for UPSERT operations
- **Partitioning**: By `event_date` and `region` for efficient querying
- **Sort Order**: By `event_time` and `user_id` for better read performance
- **Format**: Iceberg v2 with merge-on-read delete mode
- **Schema Evolution**: Includes optional `user_agent` field

### Clicks Table (`create_clicks_table.sql`)
Stores click stream events with the following key features:
- **Primary Key**: `event_id` for UPSERT operations
- **Partitioning**: By `event_date` and `region` for efficient querying
- **Sort Order**: By `event_time` and `session_id` for better read performance
- **Format**: Iceberg v2 with merge-on-read delete mode

## Table Configuration

All tables are configured with:

- **Format Version**: 2 (supports delete vectors and merge-on-read)
- **File Format**: Parquet with Snappy compression
- **Target File Size**: 128 MB (134217728 bytes)
- **Delete Mode**: merge-on-read (uses delete vectors for efficient updates)
- **Update Mode**: merge-on-read (uses equality deletes for updates)
- **UPSERT**: Enabled via primary key constraint
- **Distribution Mode**: Hash distribution for parallel writes

## Usage

These scripts can be executed in Flink SQL CLI or programmatically via the Table API:

```sql
-- First, create the Iceberg catalog
CREATE CATALOG glue_catalog WITH (
    'type' = 'iceberg',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'warehouse' = 's3://your-bucket/warehouse'
);

-- Use the catalog
USE CATALOG glue_catalog;

-- Create database
CREATE DATABASE IF NOT EXISTS iceberg_samples;
USE iceberg_samples;

-- Execute the DDL scripts
-- (Copy and paste the contents of each script)
```

## Partitioning Strategy

The partitioning strategy (`event_date`, `region`) is designed to:
1. Enable efficient time-based queries (e.g., "last 7 days")
2. Support region-specific queries (e.g., "all events in us-east-1")
3. Facilitate partition pruning for better query performance
4. Allow for easy data lifecycle management (e.g., dropping old partitions)

## Sort Order

The sort order is configured to optimize common query patterns:
- **Orders**: Sorted by `event_time` and `event_id` for time-series queries
- **Users**: Sorted by `event_time` and `user_id` for user activity analysis
- **Clicks**: Sorted by `event_time` and `session_id` for session analysis

## UPSERT Behavior

With `write.upsert.enabled = 'true'` and a primary key defined:
- Multiple events with the same `event_id` will result in a single row
- The most recent event (by `event_time`) will be retained
- Updates are implemented using equality delete files (merge-on-read)
- This is ideal for CDC (Change Data Capture) scenarios

## Performance Tuning

Key configuration parameters for performance:

- `write.target-file-size-bytes`: Controls the target size of data files (128 MB)
- `write.parquet.compression-codec`: Snappy provides good compression with fast decompression
- `write.distribution-mode`: Hash distribution ensures even data distribution across writers
- Partitioning reduces the amount of data scanned for queries
- Sort order improves read performance for common query patterns

## Maintenance

For optimal performance, consider running periodic maintenance tasks:
- **Compaction**: Merge small files into larger ones
- **Snapshot Expiration**: Remove old snapshots to reduce metadata overhead
- **Orphan File Cleanup**: Remove unreferenced files

See the `maintenance-job` module for examples of automated maintenance.
