# Dynamic Sink with AWS Glue Schema Registry (Avro)

Truly schema-agnostic Iceberg sink driven by [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html) and Avro. Unlike the JSON-based `dynamic-sink-sample` which infers schemas at runtime by walking payload fields, this sample uses registered schemas as the source of truth.

## How it works

```
Producer → Avro + GSR wire format → Kinesis
                                      │
                                      ▼
                              GsrMultiSchemaDeserializer
                                      │  (reads schema UUID from payload header,
                                      │   fetches schema from GSR — cached locally)
                                      ▼
                             Tuple2<schemaName, GenericRecord>
                                      │
                                      ▼
                          AvroToDynamicRecordGenerator
                                      │  (schema name → table name,
                                      │   Avro schema → Iceberg schema,
                                      │   GenericRecord → RowData)
                                      ▼
                           DynamicIcebergSink → Iceberg tables
```

## Why this matters

- **No runtime schema inference.** Schemas are registered in GSR before data is produced. The consumer never guesses types.
- **Efficient wire format.** Avro is more compact than JSON and GSR caches schemas locally, so only the first record of a unique schema version incurs a network call.
- **Schema evolution is centrally governed.** Compatibility rules (BACKWARD, FORWARD, FULL) are enforced at registration time, not inferred by looking at a sample of records.
- **Table routing is explicit.** Each Avro schema maps to one Iceberg table (table name = schema name), so producers control routing by choosing which schema they write with.

## Configuration

| Property | Description | Default |
|----------|-------------|---------|
| `kinesis.stream.arn` | Kinesis stream ARN to read from | required |
| `kinesis.region` | AWS region for Kinesis | inherits `aws.region` |
| `aws.region` | AWS region for Glue Schema Registry and Iceberg catalog | `us-east-1` |
| `schema.registry.name` | GSR registry name. Empty = `default-registry` | `""` |
| `iceberg.catalog.type` | `glue` or `s3tables` | `glue` |
| `iceberg.database` | Iceberg database / S3 Tables namespace | required |
| `iceberg.warehouse` | S3 warehouse path (Glue catalog) | — |
| `s3tables.bucket.arn` | S3 Table Bucket ARN (S3 Tables catalog) | — |
| `partition.candidates` | Comma-separated field names tried for partitioning | `event_date,region,date` |
| `cache.max.size` | DynamicIcebergSink in-memory cache size | `100` |
| `cache.refresh.ms` | DynamicIcebergSink cache refresh interval | `60000` |

## Deploying

```bash
cd cdk-infrastructure
npx cdk deploy -c appType=dynamic-avro
```

The CDK creates a Glue Schema Registry named `iceberg-dynamic-avro`. The data generator will auto-register schemas into it the first time it produces records.

## Generating Avro data

```bash
mvn package -pl data-generator -am -DskipTests
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar avro \
    iceberg-events-dynamic-avro us-east-1 iceberg-dynamic-avro 100 60
```

The generator registers three schemas (`OrderEvent`, `UserEvent`, `ClickEvent`) on first run. Each record is Avro-encoded and wrapped with the GSR header before being sent to Kinesis.

## Routing model

- Schema name `OrderEvent` → Iceberg table `orderevent`
- Schema name `UserEvent` → Iceberg table `userevent`
- Schema name `ClickEvent` → Iceberg table `clickevent`

Hyphens and dots in schema names are converted to underscores to satisfy Iceberg identifier rules.

## Extending

To add a new event type, register a new Avro schema in GSR (any AVRO-format schema works). The Flink app will pick it up on the next record — no code changes needed in the consumer.
