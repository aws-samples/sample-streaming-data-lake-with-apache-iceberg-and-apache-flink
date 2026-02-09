# Iceberg Source DataStream Sample

Read from Iceberg tables using the FLIP-27 IcebergSource and write to Kinesis.

## Overview

This sample demonstrates reading from Iceberg tables using Flink's DataStream API with the modern FLIP-27 source interface. It supports both batch and streaming reads, with configurable starting strategies and watermark generation.

## Key Features

- **FLIP-27 IcebergSource**: Modern source interface with better scalability
- **Streaming & Batch modes**: Switch between incremental streaming and full table scans
- **Multiple starting strategies**: Start from latest, earliest, or specific snapshots
- **Watermark generation**: Generate watermarks from Iceberg column statistics
- **Glue & S3 Tables support**: Works with both catalog types

## Important Limitations

⚠️ **Streaming reads only work for APPEND-ONLY tables.**

Tables with upserts (equality deletes) are NOT supported for streaming reads. This is a fundamental limitation of Iceberg's streaming read implementation - CDC/merge-on-read is not yet supported.

## Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `iceberg.catalog.type` | Catalog type: `glue` or `s3tables` | `glue` |
| `iceberg.warehouse` | S3 warehouse path (Glue only) | - |
| `s3tables.bucket.arn` | S3 Tables bucket ARN (S3 Tables only) | - |
| `iceberg.database` | Database/namespace name | `iceberg_samples` |
| `iceberg.table` | Table name to read from | `orders` |
| `iceberg.source.streaming` | Enable streaming mode | `true` |
| `iceberg.source.starting-strategy` | Starting strategy for streaming | `INCREMENTAL_FROM_LATEST_SNAPSHOT` |
| `iceberg.source.monitor-interval` | Interval to check for new snapshots | `60s` |
| `iceberg.source.watermark-column` | Column for watermark generation | - |
| `kinesis.sink.stream.arn` | Kinesis stream ARN to write to | - |

## Starting Strategies

For streaming mode, you can configure how the source starts reading:

- `INCREMENTAL_FROM_LATEST_SNAPSHOT`: Start from the latest snapshot (inclusive)
- `INCREMENTAL_FROM_EARLIEST_SNAPSHOT`: Start from the earliest snapshot
- `TABLE_SCAN_THEN_INCREMENTAL`: Full table scan, then switch to incremental
- `INCREMENTAL_FROM_SNAPSHOT_ID`: Start from a specific snapshot ID
- `INCREMENTAL_FROM_SNAPSHOT_TIMESTAMP`: Start from a specific timestamp

## Running Locally

```bash
# Build
mvn clean package -pl iceberg-source-datastream -am -DskipTests

# Run (configure flink-application-properties-dev.json first)
java -jar target/iceberg-source-datastream-1.0-SNAPSHOT.jar
```

## Flink UI

Local development runs on port 8084: http://localhost:8084
