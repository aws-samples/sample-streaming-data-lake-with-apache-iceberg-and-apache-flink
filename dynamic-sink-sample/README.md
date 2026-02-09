# Dynamic Iceberg Sink Sample

This module demonstrates schema-agnostic dynamic routing to Iceberg tables using Apache Flink's DataStream API.

## Features

- **Schema-Agnostic Processing**: Handles any JSON structure without predefined POJOs
- **Dynamic Table Routing**: Routes events to tables based on a configurable field
- **Automatic Schema Inference**: Infers Iceberg schema from JSON structure
- **Schema Evolution**: Handles new fields as they appear in the data
- **Configurable Write Mode**: Supports both append and upsert modes

## Architecture

```
Kinesis Stream (JSON) → Dynamic Sink Job → Multiple Iceberg Tables
                              ↓
                    [order_events, user_events, click_events, ...]
```

## Configuration

| Parameter | Environment Variable | Default | Description |
|-----------|---------------------|---------|-------------|
| `kinesis.stream.arn` | `KINESIS_STREAM_ARN` | - | ARN of the Kinesis stream |
| `kinesis.region` | `KINESIS_REGION` | `us-east-1` | AWS region for Kinesis |
| `iceberg.catalog.name` | `ICEBERG_CATALOG_NAME` | `glue_catalog` | Catalog name |
| `iceberg.catalog.type` | `ICEBERG_CATALOG_TYPE` | `glue` | Catalog type: `glue` or `s3tables` |
| `iceberg.database` | `ICEBERG_DATABASE` | `iceberg_samples` | Database/namespace name |
| `iceberg.warehouse` | `ICEBERG_WAREHOUSE` | - | S3 warehouse path (Glue only) |
| `s3tables.bucket.arn` | `S3TABLES_BUCKET_ARN` | - | S3 Table Bucket ARN (S3 Tables only) |
| `routing.field` | `ROUTING_FIELD` | `event_type` | Field used for table routing |
| `routing.table.suffix` | `ROUTING_TABLE_SUFFIX` | `_events` | Suffix for auto-generated table names |
| `partition.candidates` | `PARTITION_CANDIDATES` | `event_date,region,date` | Comma-separated partition field candidates |
| `write.mode` | `WRITE_MODE` | `append` | Write mode: `append` or `upsert` |
| `primary.key.columns` | `PRIMARY_KEY_COLUMNS` | `event_id,event_date,region` | Primary key columns for upsert mode |

## Write Mode Configuration

### Append Mode (Default)

When `WRITE_MODE=append`:
- All records are appended without deduplication
- Better write performance
- Best for: Log data, metrics, or when duplicates are acceptable

### Upsert Mode

When `WRITE_MODE=upsert`:
- Enables merge-on-read with delete vectors
- Deduplicates based on primary key columns
- Best for: Event sourcing, CDC, or when duplicate events may arrive

```bash
export WRITE_MODE="upsert"
export PRIMARY_KEY_COLUMNS="event_id,event_date,region"
```

## Table Naming

Tables are named based on the routing field value:
- `event_type=ORDER` → `order_events`
- `event_type=USER` → `user_events`
- `event_type=CLICK` → `click_events`

The naming pattern is: `{routing_field_value.toLowerCase()}{table_suffix}`

## Building

```bash
mvn clean package
```

This creates a fat JAR: `target/dynamic-sink-sample-1.0-SNAPSHOT.jar`

## Running Locally

```bash
export KINESIS_STREAM_ARN="arn:aws:kinesis:us-east-1:123456789:stream/events"
export ICEBERG_WAREHOUSE="s3://my-bucket/warehouse"

java -jar target/dynamic-sink-sample-1.0-SNAPSHOT.jar
```

Or use the provided IntelliJ run configuration: `.run/DynamicSinkJob.run.xml`

## Schema Inference

The dynamic sink automatically infers Iceberg schema from JSON:

| JSON Type | Iceberg Type |
|-----------|--------------|
| String | StringType |
| Integer | LongType |
| Float | DoubleType |
| Boolean | BooleanType |
| Object | StructType |
| Array | ListType |
| Null | StringType (nullable) |

## Partitioning

The sink automatically partitions tables based on fields matching `partition.candidates`:
- If `event_date` exists → partition by `event_date`
- If `region` exists → partition by `region`
- Multiple matching fields create multi-level partitioning

## Testing

```bash
mvn test
```

The tests verify:
- Dynamic routing correctness
- Schema evolution handling
- Partition inference

## References

- [Apache Iceberg Dynamic Sink](https://iceberg.apache.org/docs/latest/flink/#dynamic-sink)
- [Apache Flink DataStream API](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/overview/)
