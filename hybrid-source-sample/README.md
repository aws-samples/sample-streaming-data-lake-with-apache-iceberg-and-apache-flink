# Hybrid Source Sample

Bootstrap from Iceberg historical data, then seamlessly switch to real-time Kinesis streaming.

## Overview

This sample demonstrates the Flink HybridSource pattern for combining bounded and unbounded sources. It reads all historical data from an Iceberg table first, then switches to real-time Kinesis streaming - all appearing as a single source in the Flink job graph.

## Use Cases

- **Backfilling**: Populate a new streaming application with historical data before going live
- **Recovery**: Recover from extended downtime by replaying historical data
- **Migration**: Migrate from batch to streaming processing without data gaps
- **Testing**: Validate streaming logic against historical data before production

## How It Works

```
┌─────────────────────┐     ┌─────────────────────┐     ┌─────────────────┐
│  Iceberg Table      │────▶│  Kinesis Stream     │────▶│  Kinesis Sink   │
│  (Historical Data)  │     │  (Real-time Data)   │     │  (Output)       │
│  [Bounded]          │     │  [Unbounded]        │     │                 │
└─────────────────────┘     └─────────────────────┘     └─────────────────┘
         │                           │
         └───────────┬───────────────┘
                     │
              HybridSource
         (Seamless Switchover)
```

1. **Phase 1**: Read all data from Iceberg table (bounded source completes)
2. **Phase 2**: Automatically switch to Kinesis stream (unbounded, runs forever)
3. **Output**: Unified stream written to sink Kinesis stream

## Key Features

- **Seamless switchover**: No manual intervention needed
- **Single source abstraction**: Appears as one source in Flink UI
- **Configurable Kinesis start position**: LATEST, TRIM_HORIZON, or AT_TIMESTAMP
- **Glue & S3 Tables support**: Works with both catalog types

## Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `iceberg.catalog.type` | Catalog type: `glue` or `s3tables` | `glue` |
| `iceberg.warehouse` | S3 warehouse path (Glue only) | - |
| `s3tables.bucket.arn` | S3 Tables bucket ARN (S3 Tables only) | - |
| `iceberg.database` | Database/namespace name | `iceberg_samples` |
| `iceberg.table` | Table name for historical data | `orders` |
| `kinesis.source.stream.arn` | Kinesis stream ARN for real-time data | - |
| `kinesis.source.starting.position` | Where to start in Kinesis | `LATEST` |
| `kinesis.source.starting.timestamp` | Timestamp for AT_TIMESTAMP (ISO-8601) | - |
| `kinesis.sink.stream.arn` | Kinesis stream ARN for output | - |

## Kinesis Starting Positions

- `LATEST`: Start from the newest records (default)
- `TRIM_HORIZON`: Start from the oldest available records
- `AT_TIMESTAMP`: Start from a specific timestamp

## Best Practices

1. **Coordinate timestamps**: Ensure Iceberg data and Kinesis starting position don't overlap or have gaps
2. **Use AT_TIMESTAMP**: For precise control over the switchover point
3. **Monitor checkpoints**: Verify checkpointing works correctly during switchover
4. **Test with small data**: Validate the pattern with limited historical data first

## Running Locally

```bash
# Build
mvn clean package -pl hybrid-source-sample -am -DskipTests

# Run
java -jar target/hybrid-source-sample-1.0-SNAPSHOT.jar
```

## Flink UI

Local development runs on port 8086: http://localhost:8086
