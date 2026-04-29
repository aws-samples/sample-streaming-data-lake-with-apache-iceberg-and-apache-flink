# Streaming to Apache Iceberg with Apache Flink on AWS

Production-ready samples that showcase Apache Iceberg 1.10 on Apache Flink 2.2 running on AWS Managed Service for Apache Flink. The samples cover seven patterns — DataStream and SQL sinks, a dynamic sink driven by JSON inference, a dynamic sink driven by AWS Glue Schema Registry (Avro), and three variants for reading Iceberg tables (DataStream, SQL, and hybrid batch-then-stream).

Every sample works against either AWS Glue Data Catalog or Amazon S3 Tables, is deployable via a single parameterized CDK stack, and shares a small `runtime` toolbox that keeps each job's `main()` focused on its own pipeline.

## What this repository gives you

- **Seven deployable Flink jobs** covering sink, source, dynamic-routing, and hybrid patterns.
- **One CDK stack** parameterised by `-c appType=…` and `-c catalogType=…` that provisions the full infrastructure for any of them.
- **Shared runtime utilities** (`shared-common`) for environment bootstrapping, property loading, checkpointing defaults, Kinesis source configuration, and Iceberg catalog loading — so the samples demonstrate their pipeline logic, not their boilerplate.
- **A configurable data generator** that emits JSON or Avro events, registers schemas in Glue Schema Registry on demand, and simulates duplicate keys + late arrivals.
- **Property-based tests** (jqwik) that validate upsert semantics, compaction behaviour, snapshot expiration, and orphan cleanup on their own.

---

## The samples

| Sample | API | Pattern | Write path | Notes |
|---|---|---|---|---|
| `datastream-sample` | DataStream | Single table, upsert + in-job maintenance | `IcebergSink` (SinkV2) | Optional compaction/snapshot expiration coordinated via RDS PostgreSQL JDBC lock |
| `flink-sql-sample` | Table / SQL | Multi-table routing from one Kinesis stream | `StatementSet` + Iceberg SQL connector | Declarative DDL; good for SQL-first teams |
| `dynamic-sink-sample` | DataStream | Multi-table routing from one Kinesis stream (JSON) | `DynamicIcebergSink` | Schema inferred from JSON at runtime; routes by a configurable field |
| `dynamic-sink-avro-sample` | DataStream | Multi-table routing driven by Avro schemas in AWS Glue Schema Registry | `DynamicIcebergSink` | Producers register schemas in GSR; job resolves schema by UUID and evolves Iceberg tables automatically |
| `iceberg-source-datastream-sample` | DataStream | Read an Iceberg table, write rows to Kinesis | `IcebergSource` (FLIP-27) | Streaming or batch; append-only source tables only for streaming |
| `iceberg-source-sql-sample` | Table / SQL | Read Iceberg with SQL hints, write rows to Kinesis | Iceberg SQL connector | Supports branches, tags, time travel via SQL hints |
| `hybrid-source-sample` | DataStream | Bootstrap from Iceberg, switch to Kinesis streaming | `HybridSource` | Backfill-then-stream for migrations |

All write-path samples use Iceberg format version 2. See the "Delete files and format versions" section below for why — short version: Flink 1.10's Iceberg sink does not write deletion vectors, and v3 requires them for positional deletes.

---

## Architecture at a glance

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────────┐
│  data-generator │─────▶│  Kinesis stream  │─────▶│  Managed Flink app  │
└─────────────────┘      └──────────────────┘      └──────────┬──────────┘
                                                              │
                                   ┌──────────────────────────┼──────────────────────┐
                                   │                          ▼                      │
                                   │  Catalog: Glue                                   │
                                   │  ┌───────────────────┐    ┌──────────────────┐  │
                                   │  │  S3 warehouse     │◀───│  Iceberg tables  │  │
                                   │  │  (versioned)      │    │  (Glue catalog)  │  │
                                   │  └───────────────────┘    └──────────────────┘  │
                                   │                                                  │
                                   │  Catalog: S3 Tables                              │
                                   │  ┌──────────────────────────────────────────┐    │
                                   │  │  S3 Table Bucket (automatic maintenance) │    │
                                   │  └──────────────────────────────────────────┘    │
                                   │                                                  │
                                   │  Optional: DataStream + maintenance              │
                                   │  ┌───────────────────┐    ┌──────────────────┐  │
                                   │  │  RDS PostgreSQL   │◀───│  Maintenance     │  │
                                   │  │  (JDBC lock)      │    │  coordinator     │  │
                                   │  └───────────────────┘    └──────────────────┘  │
                                   └──────────────────────────────────────────────────┘
```

Dynamic Avro adds a Glue Schema Registry in front of the Kinesis stream. Source samples reverse the diagram — they read from an existing Iceberg table and write rows as JSON to a Kinesis stream.

---

## Repository layout

```
streaming-data-lake-with-apache-iceberg-and-apache-flink/
├── shared-common/                              # Code reused by every sample
│   └── src/main/java/com/aws/samples/iceberg/
│       ├── config/IcebergConfig.java           # Unified Glue + S3 Tables catalog loader
│       ├── runtime/                            # NEW: env + properties + checkpointing
│       │   ├── AppProperties.java              #   loads FlinkApplicationProperties (MSF or local)
│       │   ├── Checkpointing.java              #   exactly-once defaults
│       │   ├── FlinkEnvironments.java          #   isLocal() + getOrCreateLocal(port)
│       │   └── KinesisSources.java             #   KinesisStreamsSource with production defaults
│       ├── model/                              # Event POJOs (OrderEvent, UserEvent, ClickEvent)
│       └── util/                               # Serde + RowData conversion helpers
├── datastream-sample/                          # IcebergSink (SinkV2) + optional maintenance
├── flink-sql-sample/                           # SQL API, multi-table routing
├── dynamic-sink-sample/                        # Schema-agnostic dynamic sink (JSON)
├── dynamic-sink-avro-sample/                   # Dynamic sink driven by Glue Schema Registry
├── iceberg-source-datastream-sample/           # FLIP-27 IcebergSource → Kinesis
├── iceberg-source-sql-sample/                  # SQL IcebergSource → Kinesis
├── hybrid-source-sample/                       # HybridSource: Iceberg bootstrap + Kinesis stream
├── data-generator/                             # JSON and Avro data generators
├── cdk-infrastructure/                         # Single parameterized CDK stack
│   └── lib/
│       ├── iceberg-flink-stack.ts              # Main stack
│       └── constructs/                         # KinesisStreams, CatalogResources, MaintenanceResources, FlinkIam
├── docker-compose.yml                          # Local Postgres for JDBC lock testing
├── .run/                                       # IntelliJ run configurations
└── pom.xml                                     # Parent POM (Flink 2.2, Iceberg 1.10)
```

Every sample `main()` follows the same shape:

```java
StreamExecutionEnvironment env = FlinkEnvironments.getOrCreateLocal(LOCAL_WEB_UI_PORT);
Map<String, String> config = AppProperties.loadAsMap(env);
validateConfiguration(config);
if (FlinkEnvironments.isLocal(env)) {
    Checkpointing.configureLocalDefaults(env, interval);
}
// build source → transform → sink → env.execute(...)
```

---

## Prerequisites

**Local development**
- Java 17+
- Apache Maven 3.9+
- Docker (for the local Postgres; also used by CDK bundling)
- AWS CLI configured with credentials
- IntelliJ IDEA or VS Code with Java support

**AWS deployment**
- AWS CDK CLI (`npm install -g aws-cdk`)
- Node.js 18+ and npm
- Docker running (CDK bundles the Flink JARs inside a Maven image)

---

## Quick start — local

### 1. Build everything
```bash
mvn clean package -DskipTests
```

### 2. Start local Postgres (only needed for the DataStream sample with maintenance)
```bash
docker-compose up -d
```

### 3. Configure local properties
Edit `src/main/resources/flink-application-properties-dev.json` in the sample you want to run:
```json
[{
  "PropertyGroupId": "FlinkApplicationProperties",
  "PropertyMap": {
    "kinesis.stream.arn": "arn:aws:kinesis:us-east-1:123456789012:stream/iceberg-events-datastream",
    "kinesis.region": "us-east-1",
    "aws.region": "us-east-1",
    "iceberg.warehouse": "s3://your-bucket/warehouse",
    "iceberg.database": "iceberg_samples",
    "iceberg.table": "orders"
  }
}]
```

### 4. Run a job
From an IDE: pick one of the shipped run configurations in `.run/`.

From the command line:
```bash
./run-local.sh datastream-sample
./run-local.sh dynamic-sink-avro-sample
./run-local.sh flink-sql-sample
```

The Web UI port differs per sample so you can run several at once:

| Sample | Port |
|---|---|
| `datastream-sample` | 8081 |
| `dynamic-sink-sample` | 8082 |
| `dynamic-sink-avro-sample` | 8083 |
| `flink-sql-sample` | 8084 |
| `iceberg-source-datastream-sample` | 8085 |
| `hybrid-source-sample` | 8086 |

(`iceberg-source-sql-sample` sets its runtime mode before environment creation and doesn't open the Web UI automatically; pass `-Drest.port=…` as a JVM flag if you need it locally.)

### 5. Generate data

```bash
# JSON events (used by DataStream, SQL, Dynamic)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar \
     iceberg-events-datastream us-east-1 100 60 v1

# Same generator, Avro output with GSR schema registration (used by Dynamic Avro)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar \
     avro iceberg-events-dynamic-avro us-east-1 iceberg-dynamic-avro 100 60
```

Positional arguments:

| Mode | Arguments |
|---|---|
| JSON (default) | `<stream> <region> <events/sec> [duration-s] [v1\|v2]` |
| Avro | `avro <stream> <region> <registry-name> [events/sec] [duration-s]` |

The JSON generator supports `v1` (no optional fields) and `v2` (adds `userAgent`, `scrollDepth`) for schema-evolution testing. The Avro generator registers schemas on first use and writes GSR-wrapped Avro bytes.

---

## Deploy to AWS

### One-time CDK bootstrap
```bash
cd cdk-infrastructure
npm install
npx cdk bootstrap aws://<account-id>/<region>
```

### Deploy a sample
All deployments go through one stack, `IcebergFlinkStack`, parameterized by CDK context keys. Pick the sample with `-c appType=<name>` and the catalog with `-c catalogType=glue|s3tables` (default `glue`). Most samples also accept optional flags.

| Goal | Command |
|---|---|
| DataStream sink to Glue, no maintenance | `npx cdk deploy -c appType=datastream` |
| DataStream sink to Glue, with in-job maintenance (JDBC lock) | `npx cdk deploy -c appType=datastream -c enableMaintenance=true` |
| DataStream sink to S3 Tables (managed maintenance) | `npx cdk deploy -c appType=datastream -c catalogType=s3tables` |
| Flink SQL multi-table routing | `npx cdk deploy -c appType=sql` |
| Dynamic sink (JSON), Glue | `npx cdk deploy -c appType=dynamic` |
| Dynamic sink (Avro + GSR), Glue | `npx cdk deploy -c appType=dynamic-avro` |
| Dynamic sink (Avro + GSR), S3 Tables | `npx cdk deploy -c appType=dynamic-avro -c catalogType=s3tables` |
| Iceberg source → Kinesis (DataStream) | `npx cdk deploy -c appType=iceberg-source` |
| Iceberg source → Kinesis (SQL) | `npx cdk deploy -c appType=iceberg-source-sql` |
| Hybrid source: Iceberg bootstrap + Kinesis streaming | `npx cdk deploy -c appType=hybrid` |

Additional context flags:

| Flag | Applies to | Purpose |
|---|---|---|
| `-c enableMaintenance=true` | `datastream` + `glue` | Provisions RDS and runs `ExpireSnapshots` / `RewriteDataFiles` / `DeleteOrphanFiles` inside the Flink job, coordinated by a JDBC lock |
| `-c catalogType=s3tables` | any write-path sample | Uses S3 Tables instead of Glue Catalog |
| `-c writeMode=upsert\|append` | `datastream` | Sink mode (default `upsert`) |
| `-c tableFormatVersion=2\|3` | `datastream` | Override the table format version at creation time (default `2` — see "Delete files and format versions") |
| `-c sourceDatabase=…`, `-c sourceTable=…`, `-c sourceWarehouse=…`, `-c sourceTableBucketArn=…` | source apps | Point source apps at an existing Iceberg table rather than creating a new empty one |
| `-c stackSuffix=…` | any | Suffix applied to stack-scoped resource names so multiple variants can coexist |
| `-c cdkBootstrapQualifier=…` | any | Override the CDK bootstrap qualifier if you've used a non-default `cdk bootstrap` |

After deployment, note the stack outputs (`ApplicationName`, `KinesisSourceStreamName`, `WarehouseBucket`, `GlueDatabaseName` or `S3TableBucketName`) and start the application:

```bash
aws kinesisanalyticsv2 start-application \
  --application-name iceberg-flink-<appType> \
  --run-configuration 'ApplicationRestoreConfiguration={ApplicationRestoreType=SKIP_RESTORE_FROM_SNAPSHOT}'
```

Then feed it data with the data generator (see above), pointing at the Kinesis stream from the outputs.

---

## Querying the tables

Athena works out of the box with Glue Catalog and supports Iceberg metadata tables:

```sql
-- Snapshot history
SELECT snapshot_id, committed_at, operation
FROM iceberg_datastream.orders$snapshots
ORDER BY committed_at DESC;

-- Data files and their sizes
SELECT content, file_path, record_count, file_size_in_bytes
FROM iceberg_datastream.orders$files;

-- Delete files (content: 0=data, 1=positional delete, 2=equality delete)
SELECT content, file_path, record_count
FROM iceberg_datastream.orders$delete_files;
```

For S3 Tables, use the `s3tables` catalog in Athena or any DV-aware engine (e.g. Spark on EMR 7.12+).

---

## Delete files and format versions

Flink 1.10's Iceberg sink writes **equality delete files** (across checkpoints, keyed on the primary key) and **positional delete files** (within a checkpoint, for rows superseded in the in-progress data file). These are Iceberg v2 artefacts.

In v3, positional delete *files* are replaced by **deletion vectors** (Puffin bitmaps). Equality deletes remain as Parquet files — the v3 spec keeps them because streaming writers that don't read existing data cannot emit positional deletes. In Flink 1.10, the sink has no DV-aware writer, so a Flink upsert job against a v3 table writes v2-style positional delete files and the commit fails with:

```
java.lang.IllegalArgumentException: Must use DVs for position deletes in V3
    at org.apache.iceberg.MergingSnapshotProducer.validateNewDeleteFile(...)
    at org.apache.iceberg.flink.sink.IcebergCommitter.commitDeltaTxn(...)
```

Flink SQL is even stricter and rejects `format-version=3` up front with "UPSERT requires v2 table format". For these reasons **every sample uses v2 by default**. The DataStream sample exposes `-c tableFormatVersion=3` for testing purposes; use it if you want to reproduce the commit failure. Once Flink's Iceberg sink gains a DV writer (already on `iceberg` `main`), v3 will work end-to-end.

You still get DV benefits on the read side when compaction (`RewriteDataFiles`) rewrites accumulated delete files into DVs on a v3 table.

---

## Maintenance options

| Catalog | Maintenance |
|---|---|
| **Amazon S3 Tables** | Automatic — the service handles compaction and snapshot management. `-c enableMaintenance=true` is rejected when `catalogType=s3tables`. |
| **AWS Glue Data Catalog — auto-compaction enabled on the database** | Automatic compaction by Glue. No Flink code required. |
| **AWS Glue Data Catalog — in-job** | `-c appType=datastream -c enableMaintenance=true`. Provisions an RDS PostgreSQL for a JDBC lock and adds `ExpireSnapshots` + `RewriteDataFiles` + `DeleteOrphanFiles` operators to the Flink job. |

The in-job schedule in `datastream-sample`:

```java
.add(ExpireSnapshots.builder()
     .scheduleOnCommitCount(10).maxSnapshotAge(Duration.ofHours(24)).retainLast(5))
.add(RewriteDataFiles.builder()
     .scheduleOnDataFileCount(20)
     .targetFileSizeBytes(256 * 1024 * 1024).minFileSizeBytes(32 * 1024 * 1024)
     .partialProgressEnabled(true))
.add(DeleteOrphanFiles.builder()
     .scheduleOnCommitCount(50).minAge(Duration.ofDays(3)))
```

---

## Testing

```bash
# Compile everything
mvn clean package -DskipTests

# Run unit and property-based tests (jqwik)
mvn test

# Run a single module's tests
mvn -pl datastream-sample test

# Static analysis (SpotBugs + FindSecBugs). May be limited on very recent JDKs.
mvn spotbugs:check
```

The property tests model the upsert, compaction, snapshot-expiration, and orphan-cleanup invariants directly; they run in seconds and don't need AWS credentials.

---

## Operations and troubleshooting

**Start / stop the Flink application**
```bash
aws kinesisanalyticsv2 start-application --application-name iceberg-flink-<type> \
  --run-configuration 'ApplicationRestoreConfiguration={ApplicationRestoreType=SKIP_RESTORE_FROM_SNAPSHOT}'

aws kinesisanalyticsv2 stop-application --application-name iceberg-flink-<type> --force
```

**Watch logs**
```bash
aws logs tail /aws/kinesisanalytics/iceberg-flink-<type> --follow
```

**See what the sink is writing**
```bash
aws s3 ls s3://<warehouse-bucket>/warehouse/<db>/<table>/ --recursive | tail -40
```

Then look at `$files` and `$delete_files` via Athena to see the file mix.

**Tear everything down**
```bash
cd cdk-infrastructure
npx cdk destroy
```

---

## Cost notes

Rough per-day cost in `us-east-1` while running:

| Deployment | Approx cost/day |
|---|---|
| SQL / dynamic / source-only (no VPC, no RDS) | **~$6** |
| DataStream without maintenance | **~$6** |
| DataStream with in-job maintenance (adds RDS t3.micro + NAT Gateway) | **~$8** |
| S3 Tables variants | **~$6** plus S3 Tables storage |

Drivers are Managed Flink (~$5.28/day for 2 KPUs), Kinesis shards (~$0.72/day for 2 shards), NAT Gateway when maintenance is enabled (~$1.08/day), and S3 storage (variable). Stop or destroy the stack when you're not actively testing.

---

## Contributing

The samples are intentionally small and self-contained so each one can be read top-to-bottom. If you're adding a new sample:

1. Inherit from the parent `pom.xml`, depend on `shared-common`, and use the `runtime/` helpers (`FlinkEnvironments`, `AppProperties`, `Checkpointing`, `KinesisSources`) rather than reimplementing bootstrapping.
2. Put any cross-sample code in `shared-common` rather than duplicating it.
3. Give every operator an explicit `uid()` and a human `name()` — the Flink production-readiness guide expects it and it keeps checkpoint state portable across job-graph changes.
4. Add the new sample to `cdk-infrastructure/lib/iceberg-flink-stack.ts` so it shares the CDK infrastructure.

---

## License

MIT. See [LICENSE](LICENSE).
