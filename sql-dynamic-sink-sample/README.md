# Flink SQL Dynamic Iceberg Sink Sample

Write to many Iceberg tables from plain Flink SQL through one sink, with routing decided
per record by a small Java class. This is the SQL counterpart of `dynamic-sink-sample`:
the pipeline is SQL (`CREATE TABLE` + one `INSERT INTO`), the routing logic is Java.

Requires Apache Iceberg 1.11.0+ (SQL support for the dynamic sink,
[apache/iceberg#15279](https://github.com/apache/iceberg/pull/15279)).

## How it works

The sink is a regular `'connector' = 'iceberg'` table with two extra options:

```sql
CREATE TABLE dynamic_sink ( ... full flat event schema ... ) WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'glue_catalog',
    'catalog-impl' = 'org.apache.iceberg.aws.glue.GlueCatalog',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    'warehouse' = 's3://<bucket>/warehouse',
    'use-dynamic-iceberg-sink' = 'true',
    'dynamic-record-generator-impl' = 'com.aws.samples.iceberg.sqldynamic.SqlRoutingGenerator'
);

INSERT INTO dynamic_sink SELECT ... FROM kinesis_source;
```

The Iceberg connector instantiates `SqlRoutingGenerator` reflectively with the sink table's
`RowType`. For each row it:

1. reads the `event_type` column,
2. projects the columns relevant to that event type (common + per-type subsets),
3. emits a `DynamicRecord` targeting `iceberg_sql_dynamic.<event_type>_events` with a
   per-table Iceberg schema and an identity partition spec on `event_date` and `region`.

Tables are created on first write and evolve automatically — one `INSERT INTO` fans out to
`order_events`, `user_events`, and `click_events`.

Because the generator is constructed reflectively with only the `RowType`, the routing
configuration (database name, per-type column subsets) lives in the generator class rather
than in job properties.

## Maintenance is NOT supported on this path

Verified against the Iceberg 1.11.0 source and empirically on Managed Service for Apache
Flink: `IcebergTableSink` short-circuits to `DynamicIcebergSink` before the SQL-embedded
maintenance topology (`flink-maintenance.*`) is built. Setting those options on the sink
table has no effect — no `TriggerManager`, rewrite, or expire operators are created.

For the routed tables use one of:
- **Amazon S3 Tables** (`-c catalogType=s3tables`) — managed compaction and snapshot cleanup,
- **AWS Glue auto-compaction** on the database,
- a **dedicated maintenance job** per table (see `datastream-sample`).

## Deploy

```bash
mvn clean package -DskipTests -pl sql-dynamic-sink-sample -am
cd cdk-infrastructure
npx cdk deploy -c appType=sql-dynamic                          # Glue catalog
npx cdk deploy -c appType=sql-dynamic -c catalogType=s3tables  # S3 Tables
```

Generate data (all three event types on one stream):

```bash
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar \
    iceberg-events-sql-dynamic <region> 100 60 v1
```

Then query `iceberg_sql_dynamic.order_events` / `user_events` / `click_events` in Athena.

## Files

| File | Purpose |
|---|---|
| `SqlDynamicSinkJob.java` | Kinesis source DDL, dynamic sink DDL, single `INSERT INTO` |
| `SqlRoutingGenerator.java` | `DynamicTableRecordGenerator` implementation: routing, projection, schema, partition spec |

## Related upstream work

The Variant flavor of this pattern (`VariantAvroDynamicTableRecordGenerator`, GSR Avro bytes
into a single `VARIANT` column) merged after 1.11.0 ([apache/iceberg#16450](https://github.com/apache/iceberg/pull/16450))
and ships in the next Iceberg release.
