# Flink SQL Iceberg Sample

This module demonstrates Apache Iceberg integration with Apache Flink SQL, showcasing streaming ingestion from Amazon Kinesis to Iceberg tables stored in S3 with Glue Catalog.

## Features

- **Flink SQL API**: Uses Flink Table API and SQL for declarative data processing
- **Kinesis Source**: Reads streaming events from Amazon Kinesis Data Streams
- **Iceberg v3 Format**: Uses Iceberg table format v3 with delete vectors
- **UPSERT Operations**: Demonstrates row-level updates using primary keys
- **Partitioning**: Tables partitioned by `event_date` and `region`
- **SQL Hints**: Uses SQL hints for write optimization
- **Glue Catalog**: Integrates with AWS Glue Data Catalog for metadata management

## Architecture

```
Kinesis Stream → Flink SQL Job → Iceberg Tables (S3 + Glue Catalog)
                      ↓
              [Orders, Users, Clicks]
```

## Prerequisites

- Java 17+
- Maven 3.6+
- AWS Account with:
  - Kinesis Data Stream
  - S3 Bucket for Iceberg warehouse
  - Glue Database
  - IAM permissions for Kinesis, S3, and Glue

## Configuration

Set the following environment variables:

```bash
export KINESIS_STREAM_NAME=iceberg-events
export AWS_REGION=us-east-1
export S3_WAREHOUSE_PATH=s3://your-bucket/warehouse
export GLUE_DATABASE=iceberg_samples
export WRITE_MODE=append  # or "upsert" for deduplication
export PRIMARY_KEY_COLUMNS=event_id,event_date,region  # for upsert mode
```

### Write Mode Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `write.mode` | `append` | Write mode: `append` (no deduplication) or `upsert` (deduplicate by primary key) |
| `primary.key.columns` | `event_id,event_date,region` | Comma-separated primary key columns for upsert mode |

**Append Mode** (default): All records are appended without deduplication. Best for log data or when duplicates are acceptable.

**Upsert Mode**: Deduplicates records based on primary key columns. Tables are created with `PRIMARY KEY ... NOT ENFORCED` constraint and merge-on-read settings.

## Building

```bash
mvn clean package
```

This creates a fat JAR: `target/flink-sql-sample-1.0-SNAPSHOT.jar`

## Running Locally

```bash
java -jar target/flink-sql-sample-1.0-SNAPSHOT.jar
```

Or use the provided IntelliJ run configuration: `.run/FlinkSqlIcebergJob.run.xml`

## Running on AWS Managed Flink

1. Upload the JAR to S3
2. Create a Managed Flink application
3. Configure environment properties:
   - `kinesis.stream`: Name of your Kinesis stream
   - `iceberg.warehouse`: S3 path for Iceberg warehouse
   - `iceberg.catalog`: Glue catalog name
4. Start the application

## Table Schemas

Tables are created dynamically based on the `write.mode` configuration:

### Append Mode (Default)
Tables are created without PRIMARY KEY constraint:

```sql
CREATE TABLE orders (
    event_id STRING,
    event_time TIMESTAMP(6),
    ...
) PARTITIONED BY (event_date, region);
```

### Upsert Mode
Tables are created with PRIMARY KEY constraint and merge-on-read settings:

### Orders Table
```sql
CREATE TABLE orders (
    event_id STRING,
    event_time TIMESTAMP(6),
    event_type STRING,
    region STRING,
    event_date DATE,
    metadata MAP<STRING, STRING>,
    order_id STRING,
    customer_id STRING,
    amount DECIMAL(18, 2),
    currency STRING,
    status STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) PARTITIONED BY (event_date, region);
```

### Users Table
```sql
CREATE TABLE users (
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
) PARTITIONED BY (event_date, region);
```

### Clicks Table
```sql
CREATE TABLE clicks (
    event_id STRING,
    event_time TIMESTAMP(6),
    event_type STRING,
    region STRING,
    event_date DATE,
    metadata MAP<STRING, STRING>,
    session_id STRING,
    page_url STRING,
    referrer STRING,
    scroll_depth INT,
    time_on_page_seconds BIGINT,
    PRIMARY KEY (event_id) NOT ENFORCED
) PARTITIONED BY (event_date, region);
```

## Event Routing

The job automatically routes events to the appropriate table based on `event_type`:
- `ORDER` events → `orders` table
- `USER` events → `users` table
- `CLICK` events → `clicks` table

## UPSERT Behavior

When `write.mode=upsert`, all tables are configured with:
- Primary key on `event_id, event_date, region`
- `write.upsert.enabled = 'true'`
- Merge-on-read delete mode

When multiple events with the same `event_id` arrive:
1. Only one row per `event_id` is retained
2. The row contains values from the most recent event (by `event_time`)
3. Updates are implemented using equality delete files

## SQL Hints

The job uses SQL hints for write optimization:

```sql
INSERT INTO orders /*+ OPTIONS('write.distribution-mode'='hash') */
SELECT * FROM kinesis_source WHERE event_type = 'ORDER';
```

## Checkpointing

The job is configured with:
- Checkpoint interval: 60 seconds
- Checkpoint mode: EXACTLY_ONCE
- Min pause between checkpoints: 30 seconds
- Checkpoint timeout: 10 minutes

## Querying with Athena

After data is written, query the tables using Amazon Athena:

```sql
-- Query orders from the last 7 days
SELECT * FROM iceberg_samples.orders
WHERE event_date >= CURRENT_DATE - INTERVAL '7' DAY;

-- Query by region
SELECT region, COUNT(*) as order_count, SUM(amount) as total_amount
FROM iceberg_samples.orders
WHERE event_date = CURRENT_DATE
GROUP BY region;

-- Check for duplicate event_ids (should be none with UPSERT)
SELECT event_id, COUNT(*) as count
FROM iceberg_samples.orders
GROUP BY event_id
HAVING COUNT(*) > 1;
```

## Testing

The module includes property-based tests for UPSERT semantics:

```bash
mvn test
```

### Property Tests

- **Property 2: Upsert Semantics Correctness**
  - Validates that duplicate keys result in a single row
  - Verifies the most recent event is retained
  - Tests out-of-order event handling

## Monitoring

Key metrics to monitor:
- Kinesis consumer lag
- Checkpoint duration and failures
- Records processed per second
- Iceberg commit duration
- File counts and sizes

## Troubleshooting

### ClassNotFoundException for Hadoop Configuration
If you encounter `ClassNotFoundException: org.apache.hadoop.conf.Configuration`:
- Ensure `hadoop-common` is included in the JAR
- Check that Hadoop classes are properly shaded/relocated
- Verify the `HadoopUtils` workaround is in place if needed

### Glue Catalog Errors
- Verify IAM permissions for Glue operations
- Check that the Glue database exists
- Ensure S3 warehouse path is accessible

### Kinesis Connection Issues
- Verify IAM permissions for Kinesis operations
- Check that the stream exists and is active
- Ensure the AWS region is correctly configured

## Performance Tuning

For production deployments:

1. **Parallelism**: Adjust based on Kinesis shard count
   ```java
   env.setParallelism(4); // Match Kinesis shard count
   ```

2. **Checkpoint Interval**: Balance between latency and overhead
   ```java
   env.enableCheckpointing(60000); // 60 seconds
   ```

3. **File Size**: Configure target file size for optimal read performance
   ```sql
   'write.target-file-size-bytes' = '134217728' -- 128 MB
   ```

4. **Compaction**: Run periodic maintenance for file optimization
   - See `maintenance-job` module for automated compaction

## Related Modules

- `shared-common`: Common data models and utilities
- `data-generator`: Test data generator for Kinesis
- `datastream-sample`: DataStream API alternative
- `maintenance-job`: Standalone maintenance tasks

## References

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Apache Flink SQL Documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/overview/)
- [AWS Managed Flink Documentation](https://docs.aws.amazon.com/managed-flink/)
- [Iceberg Flink Integration](https://iceberg.apache.org/docs/latest/flink/)
