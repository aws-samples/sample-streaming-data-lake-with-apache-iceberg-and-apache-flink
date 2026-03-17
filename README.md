# Apache Iceberg 1.10 with Apache Flink 1.20 on AWS Managed Flink

This repository contains production-ready sample applications demonstrating Apache Iceberg 1.10 integration with Apache Flink 1.20 on AWS Managed Service for Apache Flink. The samples showcase modern data lakehouse patterns including upsert operations, table maintenance, and multi-table routing.

## Introduction

Apache Iceberg 1.10 introduces significant improvements for streaming workloads, including enhanced delete performance with delete vectors, improved metadata handling, and better support for upsert operations. Combined with Apache Flink 1.20's IcebergSink (SinkV2), these samples demonstrate how to build production-grade streaming data pipelines that write to Iceberg tables on AWS.

This repository provides three distinct patterns:
- **DataStream API**: Production-grade ingestion with automated table maintenance
- **Flink SQL API**: SQL-first approach for multi-table routing
- **Dynamic Sink**: Automatic table creation and schema evolution

## What You'll Deploy

The CDK infrastructure provisions a complete streaming data pipeline:

```
┌─────────────────┐      ┌──────────────────┐      ┌─────────────────────┐
│  Data Generator │─────▶│  Kinesis Stream  │─────▶│  Managed Flink App  │
│    (Local)      │      │   (2 shards)     │      │   (2-4 KPUs)        │
└─────────────────┘      └──────────────────┘      └──────────┬──────────┘
                                                               │
                    ┌──────────────────────────────────────────┼──────────────────┐
                    │                                          ▼                  │
                    │  Option A: Glue Catalog                                     │
                    │  ┌──────────────────┐    ┌──────────────────┐              │
                    │  │  S3 Warehouse    │◀───│  Iceberg Tables  │              │
                    │  │  (Versioned)     │    │  (Glue Catalog)  │              │
                    │  └──────────────────┘    └──────────────────┘              │
                    │                                                             │
                    │  Option B: S3 Tables (Native Iceberg)                       │
                    │  ┌──────────────────────────────────────────┐              │
                    │  │  S3 Table Bucket (Auto-maintained)       │              │
                    │  │  - Automatic compaction                  │              │
                    │  │  - Automatic snapshot management         │              │
                    │  └──────────────────────────────────────────┘              │
                    │                                                             │
                    │  ┌──────────────────┐    ┌──────────────────┐              │
                    │  │  RDS PostgreSQL  │◀───│  Maintenance     │              │
                    │  │  (JDBC Locks)    │    │  Coordinator     │              │
                    │  └──────────────────┘    └──────────────────┘              │
                    │         (Optional - DataStream with Glue + maintenance)     │
                    └─────────────────────────────────────────────────────────────┘
```

### Infrastructure Components

**Core Resources (Glue Catalog - Default):**
- Amazon Kinesis Data Stream (2 shards, 24h retention)
- Amazon S3 bucket (versioned, encrypted)
- AWS Glue database for Iceberg catalog
- AWS Managed Flink application (Flink 1.20)
- CloudWatch Log Groups for monitoring

**Core Resources (S3 Tables Catalog):**
- Amazon Kinesis Data Stream (2 shards, 24h retention)
- S3 Table Bucket (native Iceberg storage with automatic maintenance)
- S3 Tables namespace
- AWS Managed Flink application (Flink 1.20)
- CloudWatch Log Groups for monitoring
- Custom resource Lambda for namespace cleanup on stack deletion

**Additional Resources (DataStream with Glue + Maintenance):**
- Amazon VPC (2 AZs, public/private subnets, NAT Gateway)
- Amazon RDS PostgreSQL (t3.micro, for distributed locking)
- Security Groups for Flink and RDS

## Applications

### 1. DataStream API (`datastream-sample`)
**Best for: Production workloads requiring automated maintenance**

- Uses IcebergSink (SinkV2) with upsert mode and equality deletes
- Automated table maintenance with distributed JDBC locking
- Configurable maintenance tasks: snapshot expiration, compaction, orphan cleanup
- Writes to partitioned `orders` table (by date and region)
- Includes property-based tests for data integrity

**Key Features:**
- Upsert operations using Iceberg's equality deletes
- Coordinated maintenance across distributed Flink tasks
- Production-ready error handling and monitoring
- Configurable maintenance intervals and thresholds

### 2. Flink SQL API (`flink-sql-sample`)
**Best for: SQL-first teams and multi-table scenarios**

- Pure SQL approach using Flink Table API
- StatementSet for efficient multi-table routing
- Dynamic table creation from SQL DDL
- Writes to `sql_orders`, `sql_users`, `sql_clicks` tables
- Demonstrates Kinesis JSON deserialization in SQL

**Key Features:**
- Declarative SQL-based pipeline definition
- Multi-table routing with single source
- Easy to understand and maintain
- Property-based tests for upsert semantics

### 3. Dynamic Sink (`dynamic-sink-sample`)
**Best for: Multi-tenant or dynamic schema scenarios**

- Schema-agnostic event processing with automatic schema inference
- Automatic table routing based on event metadata (e.g., `event_type` field)
- Dynamic table creation with schema inferred from JSON structure
- Schema evolution support as new fields appear
- Routes to tables named `{event_type}_events` (e.g., `order_events`, `user_events`)

**Key Features:**
- Zero-configuration table creation
- Automatic schema detection from JSON
- Event-driven table routing
- Handles ANY JSON structure without code changes
- Configurable routing field and table naming

### 4. Iceberg Source - DataStream (`iceberg-source-datastream-sample`)
**Best for: Reading Iceberg tables and streaming to Kinesis**

- Uses FLIP-27 IcebergSource for streaming/batch reads
- Multiple starting strategies (latest, earliest, snapshot-based)
- Watermark generation from Iceberg column statistics
- Writes to Kinesis Data Stream as JSON

**Key Features:**
- Streaming reads from append-only Iceberg tables
- Configurable monitor interval for new snapshots
- Support for both Glue Catalog and S3 Tables
- JSON serialization for downstream consumers

**Important:** Streaming reads only work for APPEND-ONLY tables. Tables with upserts (equality deletes) are NOT supported for streaming.

### 5. Iceberg Source - SQL (`iceberg-source-sql-sample`)
**Best for: SQL-first approach to reading Iceberg tables**

- Flink SQL for reading Iceberg tables
- SQL hints for streaming/batch configuration
- Branch and tag reading support
- Metadata table queries ($snapshots, $history, $files)
- Writes to Kinesis using SQL connector

**Key Features:**
- Declarative SQL-based pipeline
- Time travel queries (snapshot-id, as-of-timestamp)
- Branch and tag support for data versioning
- Easy integration with existing SQL workflows

### 6. Hybrid Source (`hybrid-source-sample`)
**Best for: Backfilling and migration scenarios**

- Bootstrap from Iceberg historical data (bounded)
- Seamlessly switch to Kinesis real-time streaming (unbounded)
- Single unified pipeline for both historical and real-time data

**Use Cases:**
- Backfilling a new streaming application with historical data
- Recovering from extended downtime without data loss
- Migrating from batch to streaming processing

**Key Features:**
- Flink HybridSource pattern
- Automatic switchover when historical read completes
- Unified output to Kinesis sink

## Prerequisites
### For Local Development
- Java 17 or later
- Apache Maven 3.6+
- Docker and Docker Compose
- AWS CLI configured with credentials
- IntelliJ IDEA (recommended) or your preferred IDE

### For AWS Deployment
- AWS Account with appropriate permissions
- AWS CDK CLI: `npm install -g aws-cdk`
- Node.js 18+ and npm
- Docker (for building application JARs)
- AWS CLI configured with credentials and default region

### Required AWS Permissions
Your AWS credentials need permissions for:
- Amazon Kinesis Data Streams
- Amazon S3
- AWS Glue (Data Catalog)
- AWS Managed Flink (Kinesis Analytics V2)
- Amazon RDS (if deploying with maintenance)
- Amazon VPC (if deploying with maintenance)
- AWS CloudFormation
- IAM (for creating service roles)

## Quick Start - Local Development

### 1. Clone and Build
```bash
git clone <repository-url>
cd iceberg-flink-samples
mvn clean package -DskipTests
```

### 2. Start PostgreSQL (for maintenance)
```bash
docker-compose up -d
```

### 3. Configure Local Properties
Edit `src/main/resources/flink-application-properties-dev.json` in your chosen sample:
```json
{
  "PropertyGroupId": "FlinkApplicationProperties",
  "PropertyMap": {
    "kinesis.stream.arn": "arn:aws:kinesis:REGION:ACCOUNT:stream/YOUR-STREAM",
    "iceberg.warehouse": "s3://YOUR-BUCKET/warehouse",
    "aws.region": "us-east-1",
    "iceberg.database": "iceberg_samples"
  }
}
```

### 4. Run a Flink Application
Open in IntelliJ and select a run configuration:
- `DataStreamIcebergJob` - DataStream API with maintenance
- `FlinkSqlIcebergJob` - SQL API with multi-table routing
- `DynamicSinkJob` - Dynamic routing with schema evolution

Configuration is in `src/main/resources/flink-application-properties-dev.json` for each app.

Or run from command line:
```bash
# DataStream API
java -jar datastream-sample/target/datastream-sample-1.0-SNAPSHOT.jar

# SQL API
java -jar flink-sql-sample/target/flink-sql-sample-1.0-SNAPSHOT.jar

# Dynamic Sink
java -jar dynamic-sink-sample/target/dynamic-sink-sample-1.0-SNAPSHOT.jar
```

### 5. Generate Test Data

The data generator creates realistic e-commerce events and sends them to Kinesis:

```bash
# Build the data generator
mvn clean package -pl data-generator -am -DskipTests

# Run with default settings (V1 schema, continuous mode)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar <stream-name> <region> <events-per-second>

# Run for specific duration with V1 schema (no optional fields)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar iceberg-events-datastream us-east-1 100 60 v1

# Run with V2 schema (includes optional fields like userAgent, scrollDepth)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar iceberg-events-datastream us-east-1 100 60 v2
```

**Arguments:**
| Argument | Required | Description |
|----------|----------|-------------|
| `stream-name` | Yes | Name of the Kinesis stream |
| `region` | Yes | AWS region (e.g., us-east-1) |
| `events-per-second` | Yes | Target event generation rate |
| `duration-seconds` | No | Duration in seconds (-1 for continuous, default: -1) |
| `schema-version` | No | `v1` or `v2` (default: v1) |

**Schema Versions:**
- **v1**: Base schema without optional fields - use for initial data load
- **v2**: Extended schema with optional fields (userAgent for UserEvents, scrollDepth for ClickEvents) - use to test schema evolution

**Event Types Generated:**
- **OrderEvent** (40%): E-commerce orders with amount, currency, status
- **UserEvent** (30%): User actions like login, signup, profile updates
- **ClickEvent** (30%): Clickstream data with page URLs and session info

**Built-in Test Scenarios:**
- 10% duplicate keys (for upsert testing)
- 5% late-arriving events (for watermark testing)
- Schema evolution based on version parameter

### 6. Access Flink UI
- DataStream: http://localhost:8081
- SQL: http://localhost:8082
- Dynamic: http://localhost:8083

Monitor job progress, checkpoints, and backpressure in the Flink Web UI.

### 7. Query Data with Athena
```sql
-- Query orders table (DataStream)
SELECT * FROM iceberg_samples.orders 
WHERE order_date >= current_date - interval '1' day
LIMIT 10;

-- Query SQL tables
SELECT * FROM iceberg_samples.sql_orders LIMIT 10;
SELECT * FROM iceberg_samples.sql_users LIMIT 10;
SELECT * FROM iceberg_samples.sql_clicks LIMIT 10;

-- Check table metadata
SELECT * FROM iceberg_samples.orders$snapshots;
SELECT * FROM iceberg_samples.orders$files;
```

## Deploy to AWS

### Step 1: Bootstrap CDK (First Time Only)
```bash
cd cdk-infrastructure
npm install
cdk bootstrap aws://ACCOUNT-ID/REGION
```

### Step 2: Choose Your Deployment

#### Option A: DataStream with Maintenance (Recommended for Production)
```bash
cdk deploy -c appType=datastream -c enableMaintenance=true
```

**Deploys:**
- Kinesis Data Stream
- S3 warehouse bucket
- Glue database
- VPC with private subnets and NAT Gateway
- RDS PostgreSQL for distributed locking
- Managed Flink application with VPC connectivity

**Cost:** ~$6-7/day

#### Option B: DataStream without Maintenance
```bash
cdk deploy -c appType=datastream -c enableMaintenance=false
```

**Deploys:**
- Kinesis, S3, Glue, Flink application
- No VPC or RDS

**Cost:** ~$5-6/day

#### Option C: SQL API
```bash
cdk deploy -c appType=sql
```

**Deploys:**
- Kinesis, S3, Glue, Flink application
- Multi-table routing with SQL

**Cost:** ~$5-6/day

#### Option D: Dynamic Sink
```bash
cdk deploy -c appType=dynamic
```

**Deploys:**
- Kinesis, S3, Glue, Flink application
- Dynamic table creation

**Cost:** ~$5-6/day

#### Option E: DataStream with S3 Tables (Native Iceberg)
```bash
cdk deploy -c appType=datastream -c catalogType=s3tables
```

**Deploys:**
- Kinesis Data Stream
- S3 Table Bucket (native Iceberg storage)
- S3 Tables namespace
- Managed Flink application

**Key Benefits:**
- S3 Tables handles compaction and maintenance automatically
- No need for RDS or VPC for maintenance coordination
- Native Iceberg support with automatic optimization
- Simplified operations

**Note:** S3 Tables is not compatible with `enableMaintenance=true` since it handles maintenance automatically.

**Cost:** ~$5-6/day (plus S3 Tables storage costs)

#### Option F: Dynamic Sink with S3 Tables
```bash
cdk deploy -c appType=dynamic -c catalogType=s3tables
```

**Deploys:**
- Kinesis, S3 Table Bucket, Flink application
- Dynamic table creation with S3 Tables

**Cost:** ~$5-6/day

#### Option G: SQL API with S3 Tables
```bash
cdk deploy -c appType=sql -c catalogType=s3tables
```

**Deploys:**
- Kinesis Data Stream
- S3 Table Bucket (native Iceberg storage)
- Managed Flink application with SQL multi-table routing

**Cost:** ~$5-6/day

#### Option H: Iceberg Source (Read from Iceberg, Write to Kinesis)
```bash
cdk deploy -c appType=iceberg-source
```

**Deploys:**
- S3 warehouse bucket and Glue database (or S3 Tables)
- Kinesis Data Stream for output
- Managed Flink application reading from Iceberg

**Note:** Requires an existing Iceberg table with data. Streaming reads only work for append-only tables.

**Cost:** ~$5-6/day

#### Option I: Iceberg Source SQL
```bash
cdk deploy -c appType=iceberg-source-sql
```

**Deploys:**
- S3 warehouse bucket and Glue database (or S3 Tables)
- Kinesis Data Stream for output
- Managed Flink application with SQL-based Iceberg reading

**Cost:** ~$5-6/day

#### Option J: Hybrid Source (Bootstrap + Streaming)
```bash
cdk deploy -c appType=hybrid
```

**Deploys:**
- S3 warehouse bucket and Glue database (or S3 Tables)
- Kinesis Data Stream for source (after bootstrap)
- Kinesis Data Stream for sink output
- Managed Flink application with hybrid source pattern

**Use Case:** Start by reading all historical data from Iceberg, then seamlessly switch to real-time Kinesis streaming.

**Cost:** ~$6-7/day (two Kinesis streams)

### Step 3: Note the Outputs
CDK will output important values:
```
Outputs:
IcebergFlinkStack.ApplicationName = iceberg-flink-datastream
IcebergFlinkStack.KinesisStreamName = iceberg-events-datastream
IcebergFlinkStack.WarehouseBucket = iceberg-warehouse-datastream-123456789
IcebergFlinkStack.GlueDatabaseName = iceberg_datastream
```

### Step 4: Start the Flink Application
```bash
aws kinesisanalyticsv2 start-application \
  --application-name iceberg-flink-datastream \
  --run-configuration '{}'
```

### Step 5: Generate Test Data

Get the Kinesis stream name from CDK outputs and run the data generator:

```bash
# Get stream name from CDK output
STREAM_NAME=$(aws cloudformation describe-stacks \
  --stack-name IcebergFlinkStack \
  --query 'Stacks[0].Outputs[?OutputKey==`KinesisSourceStreamName`].OutputValue' \
  --output text)

# Generate V1 schema events (100 events/sec for 60 seconds)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar $STREAM_NAME us-east-1 100 60 v1

# Or generate V2 schema events (with optional fields for schema evolution testing)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar $STREAM_NAME us-east-1 100 60 v2

# Continuous mode (runs until stopped with Ctrl+C)
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar $STREAM_NAME us-east-1 50 -1 v1
```

**Tip:** Start with V1 schema to establish baseline tables, then switch to V2 to test schema evolution.

### Step 6: Monitor the Application
```bash
# Check application status
aws kinesisanalyticsv2 describe-application \
  --application-name iceberg-flink-datastream

# View CloudWatch logs
aws logs tail /aws/kinesisanalytics/iceberg-flink-datastream --follow
```

### Step 7: Query Data with Athena
```sql
-- Query your data
SELECT COUNT(*) as total_orders,
       SUM(total_amount) as revenue
FROM iceberg_datastream.orders
WHERE order_date >= current_date - interval '1' day;

-- Check snapshots
SELECT snapshot_id, 
       committed_at,
       operation,
       summary
FROM iceberg_datastream.orders$snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

## Deployment Architecture

### DataStream with Maintenance
```
Internet Gateway
       │
       ▼
   NAT Gateway (Public Subnet)
       │
       ▼
Flink Application (Private Subnet)
       │
       ├──▶ Kinesis (Read events)
       ├──▶ S3 (Write Iceberg data)
       ├──▶ Glue (Register tables)
       └──▶ RDS PostgreSQL (Maintenance locks)
```

### SQL/Dynamic (No VPC)
```
Flink Application (Managed)
       │
       ├──▶ Kinesis (Read events)
       ├──▶ S3 (Write Iceberg data)
       └──▶ Glue (Register tables)
```

## Configuration Details

### Application Properties

Each application reads configuration from runtime properties:

**Common Properties:**
```json
{
  "aws.region": "us-east-1",
  "iceberg.warehouse": "s3://bucket/warehouse",
  "iceberg.catalog.name": "glue_catalog",
  "iceberg.catalog.type": "glue",
  "iceberg.database": "iceberg_samples",
  "checkpoint.interval.ms": "60000"
}
```

**Catalog Types:**
- `glue` (default): Uses AWS Glue Data Catalog for metadata, S3 for data storage
- `s3tables`: Uses S3 Tables for native Iceberg storage with automatic maintenance

**S3 Tables Specific:**
```json
{
  "iceberg.catalog.type": "s3tables",
  "iceberg.catalog.name": "s3tables_catalog",
  "s3tables.bucket.arn": "arn:aws:s3tables:region:account:bucket/bucket-name"
}
```

**DataStream Specific:**
```json
{
  "kinesis.stream.arn": "arn:aws:kinesis:...",
  "iceberg.table": "orders",
  "enable.maintenance": "true",
  "rds.jdbc.url": "jdbc:postgresql://host:5432/iceberg_locks",
  "rds.user": "flink",
  "rds.password": "{{resolve:secretsmanager:<secret-arn>:SecretString:password}}"
}
```

> **Note:** The RDS password is auto-generated by AWS Secrets Manager during CDK deployment. You can customize the password configuration in `cdk-infrastructure/lib/iceberg-flink-stack.ts`.

**SQL Specific:**
```json
{
  "kinesis.stream.name": "iceberg-events-sql",
  "table.prefix": "sql_"
}
```

### Maintenance Configuration (DataStream)

When `enable.maintenance=true`, the application runs coordinated maintenance:

**Snapshot Expiration:**
- Trigger: Every 10 commits
- Retention: 5 snapshots minimum
- Max age: 24 hours
- Removes old metadata and data files

**Data File Compaction:**
- Trigger: Every 20 small files
- Target size: 256 MB
- Combines small files for better query performance

**Orphan File Cleanup:**
- Trigger: Every 50 commits
- Min age: 3 days
- Removes unreferenced data files

**Locking:**
- Uses JDBC-based distributed locks
- Prevents concurrent maintenance conflicts
- Configurable lock timeout and retry

## Project Structure

```
iceberg-flink-samples/
├── datastream-sample/              # DataStream API with IcebergSink
│   ├── src/main/java/             # Application code
│   ├── src/test/java/             # Property-based tests
│   └── src/main/resources/        # Configuration files
├── flink-sql-sample/              # Flink SQL API
│   ├── src/main/java/             # SQL application code
│   ├── src/test/java/             # Upsert semantics tests
│   └── sql/                       # DDL statements
├── dynamic-sink-sample/           # Dynamic Iceberg Sink (Schema-Agnostic)
│   └── src/main/java/             # Dynamic routing logic
├── iceberg-source-datastream-sample/     # Iceberg Source (DataStream API)
│   └── src/main/java/             # FLIP-27 IcebergSource to Kinesis
├── iceberg-source-sql-sample/            # Iceberg Source (SQL API)
│   └── src/main/java/             # SQL-based Iceberg reading
├── hybrid-source-sample/          # Hybrid Source (Iceberg + Kinesis)
│   └── src/main/java/             # Bootstrap from Iceberg, stream from Kinesis
├── data-generator/                # Test data generator
│   └── src/main/java/             # Event generation
├── shared-common/                 # Shared utilities
│   ├── model/                     # Event POJOs
│   ├── config/                    # Iceberg configuration
│   └── util/                      # Serializers and converters
├── cdk-infrastructure/            # AWS CDK deployment
│   ├── lib/                       # Stack definitions
│   └── bin/                       # CDK app entry point
├── .run/                          # IntelliJ run configurations
├── docker-compose.yml             # Local PostgreSQL
└── pom.xml                        # Parent POM
```

## Key Features Demonstrated

### Iceberg 1.10 Features
- ✅ Table format v2 with delete vectors
- ✅ Equality deletes for upsert operations
- ✅ Partitioned tables (date and region)
- ✅ Schema evolution (Dynamic Sink)
- ✅ Snapshot management and time travel
- ✅ Metadata optimization

### Flink 1.20 Features
- ✅ IcebergSink (SinkV2) for DataStream
- ✅ Iceberg SQL connector for Table API
- ✅ Exactly-once semantics with checkpointing
- ✅ Backpressure handling
- ✅ State management

### AWS Integration
- ✅ Glue Catalog for metadata
- ✅ S3 Tables for native Iceberg storage (alternative to Glue)
- ✅ S3 for data storage
- ✅ Kinesis for event streaming
- ✅ VPC connectivity for RDS
- ✅ CloudWatch for monitoring
- ✅ IAM for security

### Production Patterns
- ✅ Distributed maintenance coordination
- ✅ Error handling and retry logic
- ✅ Monitoring and observability
- ✅ Property-based testing
- ✅ Configuration management
- ✅ Infrastructure as Code (CDK)

## Testing

### Unit and Property-Based Tests
```bash
# Run all tests
mvn test

# Run specific module tests
mvn test -pl datastream-sample

# Run with coverage
mvn test jacoco:report
```

### Property-Based Tests
The samples include property-based tests using jqwik:
- **Data Integrity**: Verifies upsert semantics
- **Compaction**: Tests file consolidation
- **Snapshot Expiration**: Validates retention policies
- **Orphan Cleanup**: Ensures no data loss

### Integration Testing
```bash
# Start local environment
docker-compose up -d

# Run application locally
java -jar datastream-sample/target/datastream-sample-1.0-SNAPSHOT.jar

# Generate test data
java -jar data-generator/target/data-generator-1.0-SNAPSHOT.jar

# Query results
aws athena start-query-execution \
  --query-string "SELECT COUNT(*) FROM iceberg_samples.orders" \
  --result-configuration "OutputLocation=s3://your-bucket/results/"
```

## Monitoring and Observability

### CloudWatch Metrics
The Flink applications emit metrics to CloudWatch:
- Records processed per second
- Checkpoint duration and size
- Backpressure indicators
- Task manager resource utilization

### CloudWatch Logs
Application logs are streamed to CloudWatch Logs:
```bash
# Tail application logs
aws logs tail /aws/kinesisanalytics/iceberg-flink-datastream --follow

# Filter for errors
aws logs filter-log-events \
  --log-group-name /aws/kinesisanalytics/iceberg-flink-datastream \
  --filter-pattern "ERROR"
```

### Flink Metrics
Access Flink's built-in metrics via the REST API:
```bash
# Get job metrics
curl http://localhost:8081/jobs/<job-id>/metrics

# Get checkpoint statistics
curl http://localhost:8081/jobs/<job-id>/checkpoints
```

### Iceberg Metadata
Query Iceberg metadata tables:
```sql
-- View snapshots
SELECT * FROM iceberg_samples.orders$snapshots;

-- View data files
SELECT * FROM iceberg_samples.orders$files;

-- View manifests
SELECT * FROM iceberg_samples.orders$manifests;

-- View partitions
SELECT * FROM iceberg_samples.orders$partitions;
```

## Troubleshooting

### Common Issues

**Issue: Application fails to start**
```bash
# Check application status
aws kinesisanalyticsv2 describe-application \
  --application-name iceberg-flink-datastream

# Check CloudWatch logs for errors
aws logs tail /aws/kinesisanalytics/iceberg-flink-datastream --since 10m
```

**Issue: No data in Iceberg tables**
```bash
# Verify Kinesis stream has data
aws kinesis get-records \
  --shard-iterator $(aws kinesis get-shard-iterator \
    --stream-name iceberg-events-datastream \
    --shard-id shardId-000000000000 \
    --shard-iterator-type LATEST \
    --query 'ShardIterator' --output text)

# Check Flink job is running
aws kinesisanalyticsv2 describe-application \
  --application-name iceberg-flink-datastream \
  --query 'ApplicationDetail.ApplicationStatus'
```

**Issue: Maintenance tasks failing**
```bash
# Check RDS connectivity
aws rds describe-db-instances \
  --db-instance-identifier <instance-id>

# Verify security group rules
aws ec2 describe-security-groups \
  --group-ids <flink-sg-id> <rds-sg-id>
```

**Issue: High costs**
```bash
# Stop Flink application when not in use
aws kinesisanalyticsv2 stop-application \
  --application-name iceberg-flink-datastream

# Delete stack to remove all resources
cdk destroy
```

## Cost Optimization

### Development/Testing
- Stop Flink applications when not in use
- Use smaller RDS instances (t3.micro)
- Reduce Kinesis shard count to 1
- Set S3 lifecycle policies for old data

### Production
- Enable auto-scaling for Flink (2-8 KPUs)
- Use Reserved Instances for RDS
- Implement data retention policies
- Monitor and optimize checkpoint intervals

### Estimated Costs (us-east-1)

**DataStream with Maintenance:**
- Flink: 2 KPUs × $0.11/hour = $5.28/day
- Kinesis: 2 shards × $0.015/hour = $0.72/day
- RDS t3.micro: $0.017/hour = $0.41/day
- NAT Gateway: $0.045/hour = $1.08/day
- S3: ~$0.10/day (varies with data volume)
- **Total: ~$7.59/day**

**SQL/Dynamic (No VPC):**
- Flink: 2 KPUs × $0.11/hour = $5.28/day
- Kinesis: 2 shards × $0.015/hour = $0.72/day
- S3: ~$0.10/day
- **Total: ~$6.10/day**

## Cleanup

### Stop Application
```bash
aws kinesisanalyticsv2 stop-application \
  --application-name iceberg-flink-datastream
```

### Delete Stack
```bash
cd cdk-infrastructure
cdk destroy
```

### Manual Cleanup (if needed)
```bash
# Delete S3 bucket contents
aws s3 rm s3://iceberg-warehouse-datastream-123456789 --recursive

# Delete Kinesis stream
aws kinesis delete-stream --stream-name iceberg-events-datastream
```

## Configuration

## License

MIT
