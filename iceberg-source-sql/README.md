# Iceberg Source SQL Sample

Read from Iceberg tables using Flink SQL and write to Kinesis.

## Overview

This sample demonstrates reading from Iceberg tables using Flink's SQL API. It showcases SQL hints for configuring streaming/batch reads, time travel queries, and metadata table access.

## Key Features

- **Pure SQL approach**: Declarative pipeline definition
- **SQL hints**: Configure streaming options inline with queries
- **Time travel**: Query historical snapshots by ID or timestamp
- **Branch/tag support**: Read from specific branches or tags
- **Metadata tables**: Query `$snapshots`, `$history`, `$files`, `$partitions`

## Important Limitations

⚠️ **Streaming reads only work for APPEND-ONLY tables.**

Tables with upserts (equality deletes) are NOT supported for streaming reads.

## Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `iceberg.catalog.type` | Catalog type: `glue` or `s3tables` | `glue` |
| `iceberg.warehouse` | S3 warehouse path (Glue only) | - |
| `s3tables.bucket.arn` | S3 Tables bucket ARN (S3 Tables only) | - |
| `iceberg.database` | Database/namespace name | `iceberg_samples` |
| `iceberg.table` | Table name to read from | `orders` |
| `iceberg.source.streaming` | Enable streaming mode | `true` |
| `iceberg.source.monitor-interval` | Interval to check for new snapshots | `60s` |
| `kinesis.sink.stream.name` | Kinesis stream name to write to | - |

## SQL Hints Examples

### Streaming Read
```sql
SELECT * FROM database.table 
/*+ OPTIONS('streaming' = 'true', 'monitor-interval' = '60s') */
```

### Time Travel by Snapshot ID
```sql
SELECT * FROM database.table 
/*+ OPTIONS('snapshot-id' = '1234567890') */
```

### Read from Branch
```sql
SELECT * FROM database.table 
/*+ OPTIONS('branch' = 'feature-branch') */
```

## Metadata Tables

Query Iceberg metadata directly:

```sql
-- View snapshots
SELECT * FROM database.table$snapshots;

-- View history
SELECT * FROM database.table$history;

-- View data files
SELECT * FROM database.table$files;

-- View partitions
SELECT * FROM database.table$partitions;
```

## Running Locally

```bash
# Build
mvn clean package -pl iceberg-source-sql -am -DskipTests

# Run
java -jar target/iceberg-source-sql-1.0-SNAPSHOT.jar
```

## Flink UI

Local development runs on port 8085: http://localhost:8085
