-- Create users Iceberg table with v3 format and delete vectors
-- Partitioned by event_date and region for efficient querying
-- Configured with primary key for UPSERT operations

CREATE TABLE IF NOT EXISTS users (
    event_id STRING,
    event_time TIMESTAMP(6),
    event_type STRING,
    region STRING,
    event_date DATE,
    metadata MAP<STRING, STRING>,
    user_id STRING,
    action STRING,
    device_type STRING,
    ip_address STRING,
    user_agent STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) PARTITIONED BY (event_date, region)
WITH (
    'format-version' = '2',
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.target-file-size-bytes' = '134217728',
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read',
    'write.merge.mode' = 'merge-on-read',
    'write.upsert.enabled' = 'true',
    'write.distribution-mode' = 'hash'
);

-- Add sort order for better query performance
ALTER TABLE users WRITE ORDERED BY event_time, user_id;
