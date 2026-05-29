# Variant Sample

Streams JSON events from Kinesis into an Apache Iceberg **V3** table, storing the full
schema-less payload in a single `variant` column using the **Flink `Variant` type**
(`org.apache.flink.types.variant`). This is the end-to-end "one story" for Variant:
Flink Variant → Iceberg Variant.

## What it shows

- **Flink Variant**: arbitrary JSON is converted into a Flink `Variant` value (objects,
  arrays, and scalars) via `Variant.newBuilder()`.
- **Iceberg Variant (V3)**: the value is written through `IcebergSink` into an Iceberg
  `Types.VariantType` column on a `format-version=3` table — no flattening and no fixed
  schema for the nested data, while the table keeps ACID, partitioning, and time-travel.
- **Typed + flexible together**: a few typed identifier columns (`event_id`, `event_type`,
  `event_date`) sit alongside the flexible `payload` variant.

## Table schema

| column | type | notes |
|--------|------|-------|
| `event_id` | string (required) | falls back to a generated UUID if absent |
| `event_type` | string | optional |
| `event_date` | string | optional |
| `payload` | **variant** | the entire JSON record |

The table is auto-created (format-version 3) on first run if it does not exist.

## Configuration

| property | default | description |
|----------|---------|-------------|
| `kinesis.stream.arn` | _(required)_ | source Kinesis stream |
| `kinesis.region` / `aws.region` | `us-east-1` | region |
| `iceberg.catalog.type` | `glue` | `glue` or `s3tables` |
| `iceberg.database` | `iceberg_variant` | target database |
| `iceberg.table` | `events_variant` | target table |
| `iceberg.warehouse` | _(required for Glue)_ | warehouse path |

## Deploy

```bash
cd cdk-infrastructure
AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 \
CDK_DEFAULT_ACCOUNT=<account> CDK_DEFAULT_REGION=us-east-1 \
  npx cdk deploy -c appType=variant -c stackSuffix=variant -c catalogType=glue \
  --require-approval never
```

Then feed the source stream with the JSON `data-generator`.

## Notes

- Reading Variant back requires an engine that supports the Iceberg V3 spec (Flink
  Iceberg 1.11.0+, Spark 4+). Amazon Athena does not yet support V3.
- The Flink Iceberg runtime needs `commons-lang3 >= 3.13` on the classpath for the
  Variant read path (the shaded runtime bundles an older one).
