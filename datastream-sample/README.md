# DataStream API Iceberg Sample

This module demonstrates using Apache Flink's DataStream API with Apache Iceberg 1.10 and the new IcebergSink (SinkV2).
## Features

- **IcebergSink (SinkV2)**: Uses the new Flink Sink interface for improved streaming writes
- **Table Format v3**: Leverages Iceberg v3 format with delete vectors for efficient updates
- **Upsert Mode**: Demonstrates merge-on-read upsert operations using equality deletes
- **Branch Writes**: Supports writing to staging branches before merging to main
- **Metrics**: Includes monitoring configuration for write performance

## Requirements

- Apache Flink 2.2.0
- Apache Iceberg 1.10.1
- AWS Kinesis Data Streams
- AWS Glue Data Catalog
- Amazon S3

## Configuration

The job can be configured via environment variables or command-line arguments:

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `kinesis.stream.arn` | `KINESIS_STREAM_ARN` | - | ARN of the Kinesis stream to read from |
| `kinesis.region` | `KINESIS_REGION` | `us-east-1` | AWS region for Kinesis |
| `iceberg.catalog.name` | `ICEBERG_CATALOG_NAME` | `glue_catalog` | Name of the Iceberg catalog |
| `iceberg.database` | `ICEBERG_DATABASE` | `iceberg_samples` | Glue database name |
| `iceberg.table` | `ICEBERG_TABLE` | `orders` | Iceberg table name |
| `iceberg.warehouse` | `ICEBERG_WAREHOUSE` | - | S3 path for Iceberg warehouse |
| `iceberg.branch` | `ICEBERG_BRANCH` | - | Optional: Branch name for staging writes |
| `write.mode` | `WRITE_MODE` | `upsert` | Write mode: `append` or `upsert` |
| `primary.key.columns` | `PRIMARY_KEY_COLUMNS` | `event_id,event_date,region` | Comma-separated primary key columns for upsert mode |
| `aws.region` | `AWS_REGION` | `us-east-1` | AWS region for Glue and S3 |
| `checkpoint.interval.ms` | `CHECKPOINT_INTERVAL_MS` | `60000` | Checkpoint interval in milliseconds |

## Running Locally

### Prerequisites

1. Set up AWS credentials in your environment
2. Create a Kinesis stream
3. Create a Glue database
4. Create an S3 bucket for the Iceberg warehouse
5. Update `src/main/resources/flink-application-properties-dev.json` with your configuration

### Run the Job

```bash
mvn clean package
mvn exec:java -Dexec.mainClass="com.aws.samples.iceberg.datastream.DataStreamIcebergJob"
```

Or use the provided IntelliJ run configuration: `.run/DataStreamIcebergJob.run.xml`

### Local Configuration

Edit `src/main/resources/flink-application-properties-dev.json`:

```json
{
  "PropertyGroupId": "FlinkApplicationProperties",
  "PropertyMap": {
    "kinesis.stream.arn": "arn:aws:kinesis:us-east-1:123456789:stream/events",
    "kinesis.region": "us-east-1",
    "iceberg.warehouse": "s3://my-bucket/warehouse",
    "iceberg.database": "iceberg_samples",
    "iceberg.table": "orders",
    "write.mode": "upsert",
    "primary.key.columns": "event_id,event_date,region"
  }
}
```

## Branch Writes for Staging

Branch writes allow you to write data to a staging branch before merging it to the main branch. This is useful for:

- Testing data quality before production
- Implementing approval workflows
- Isolating experimental writes

### Using Branch Writes

1. **Enable branch writes** by setting the `ICEBERG_BRANCH` environment variable:

```bash
export ICEBERG_BRANCH="staging"
```

2. **Run the job** - data will be written to the `staging` branch instead of `main`

3. **Query the staging branch** using Spark or Athena:

```sql
-- Spark SQL
SELECT * FROM glue_catalog.iceberg_samples.orders VERSION AS OF 'staging';

-- Or using branch syntax
SELECT * FROM glue_catalog.iceberg_samples.orders.branch_staging;
```

4. **Merge the branch** to main after validation:

```scala
// Using Spark
val table = spark.table("glue_catalog.iceberg_samples.orders")
table.manageSnapshots()
  .cherrypick(stagingSnapshotId)
  .commit()

// Or using Iceberg API
Table table = catalog.loadTable(TableIdentifier.of("iceberg_samples", "orders"));
table.manageSnapshots()
  .cherrypick(stagingSnapshotId)
  .commit();
```

5. **Fast-forward merge** (if no conflicts):

```scala
// Using Iceberg API
table.manageSnapshots()
  .fastForward("main", "staging")
  .commit();
```

### Branch Merge Workflow

```
┌─────────────────┐
│  Kinesis Stream │
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  DataStream Iceberg Job │
│  (ICEBERG_BRANCH=staging)│
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Iceberg Table          │
│  Branch: staging        │
└────────┬────────────────┘
         │
         │ Validate data quality
         │ Run tests
         │ Approval workflow
         │
         ▼
┌─────────────────────────┐
│  Merge to main branch   │
│  (cherrypick or         │
│   fast-forward)         │
└────────┬────────────────┘
         │
         ▼
┌─────────────────────────┐
│  Iceberg Table          │
│  Branch: main           │
│  (Production data)      │
└─────────────────────────┘
```

## Upsert vs Append Mode

The job supports two write modes, configurable via `WRITE_MODE`:

### Upsert Mode (Default)

When `WRITE_MODE=upsert`:
- Deduplicates records based on `PRIMARY_KEY_COLUMNS`
- Uses merge-on-read with delete vectors (v2 format)
- Partition columns must be included in primary key when using HASH distribution
- Best for: Event sourcing, CDC, or when duplicate events may arrive

```bash
export WRITE_MODE="upsert"
export PRIMARY_KEY_COLUMNS="event_id,event_date,region"
```

### Append Mode

When `WRITE_MODE=append`:
- No deduplication - all records are appended
- Better write performance (no equality checks)
- Best for: Log data, metrics, or when duplicates are acceptable

```bash
export WRITE_MODE="append"
```

## Upsert Semantics

When using upsert mode with `event_id` as the primary key:

1. The most recent event (by `event_time`) is kept
2. Older events are marked as deleted using delete vectors (v3 format)
3. No data files are rewritten during upsert (merge-on-read)

## Table Format v3 Features

This sample uses Iceberg table format v3 which provides:

- **Delete Vectors**: Efficient row-level deletes without rewriting data files
- **Merge-on-Read**: Updates and deletes are applied at read time
- **Puffin Files**: Compressed bitmaps for tracking deleted rows

## Monitoring

The IcebergSink exposes metrics for monitoring write performance and throughput:

### Flink Metrics

The sink automatically reports the following Flink metrics:

- `numRecordsOut`: Number of records written to Iceberg
- `numBytesOut`: Number of bytes written
- `currentSendTime`: Time spent writing data
- `flushDuration`: Time spent flushing data files to S3
- `commitDuration`: Time spent committing snapshots to Iceberg

### Iceberg Snapshot Properties

The sink also writes metadata to Iceberg snapshot properties:

- `flink.job-id`: Identifies which Flink job wrote the data
- `flink.max-committed-checkpoint-id`: Tracks checkpoint progress

### Custom Metrics

You can add custom metrics by extending the job with a `RichMapFunction`:

```java
DataStream<RowData> rowDataWithMetrics = rowDataStream
    .map(new RichMapFunction<RowData, RowData>() {
        private transient Counter recordCounter;
        private transient Meter throughputMeter;
        
        @Override
        public void open(Configuration parameters) {
            recordCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("custom_records_processed");
            
            throughputMeter = getRuntimeContext()
                .getMetricGroup()
                .meter("custom_throughput", new MeterView(60));
        }
        
        @Override
        public RowData map(RowData value) {
            recordCounter.inc();
            throughputMeter.markEvent();
            return value;
        }
    });
```

### Monitoring in AWS Managed Flink

When deployed to AWS Managed Flink, metrics are automatically sent to CloudWatch:

1. Navigate to CloudWatch Metrics
2. Select "AWS/KinesisAnalytics" namespace
3. View metrics by application name and operator

Key metrics to monitor:
- **Backpressure**: `bufferPoolUsage` > 80% indicates backpressure
- **Checkpoint Duration**: `lastCheckpointDuration` should be < checkpoint interval
- **Records Lag**: `millisBehindLatest` for Kinesis source
- **Throughput**: `numRecordsOutPerSecond` for sink operators

### Alerting

Set up CloudWatch alarms for:
- High checkpoint duration (> 5 minutes)
- High backpressure (> 80%)
- Checkpoint failures
- Job restarts

## Building

```bash
mvn clean package
```

The build produces a fat JAR suitable for deployment to AWS Managed Flink:

```
target/datastream-sample-1.0-SNAPSHOT.jar
```

## Deploying to AWS Managed Flink

1. Upload the JAR to S3
2. Create a Managed Flink application
3. Configure environment properties
4. Set IAM role with permissions for Kinesis, Glue, and S3
5. Start the application

See the `cdk-infrastructure` module for automated deployment.

## Testing

Run the property-based tests:

```bash
mvn test
```

The tests verify:
- Data integrity through the conversion pipeline
- Batch conversion preserves all events
- Distinct events produce distinct RowData
- Edge cases are handled correctly

## Troubleshooting

### ClassNotFoundException for Hadoop Configuration

If you encounter `ClassNotFoundException: org.apache.hadoop.conf.Configuration`, ensure the `hadoop-common` dependency is included and properly shaded in the JAR.

### S3 Access Denied

Ensure your IAM role has the following permissions:
- `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` on the warehouse bucket
- `glue:GetDatabase`, `glue:GetTable`, `glue:UpdateTable` on the Glue database

### Kinesis Stream Not Found

Verify the `KINESIS_STREAM_ARN` is correct and the stream exists in the specified region.

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Flink Iceberg Connector](https://iceberg.apache.org/docs/latest/flink/)
- [AWS Managed Flink](https://docs.aws.amazon.com/managed-flink/)
- [Iceberg Table Format v3](https://iceberg.apache.org/spec/#version-3)
