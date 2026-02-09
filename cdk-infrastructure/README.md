# Iceberg Flink CDK Infrastructure

CDK infrastructure for deploying Iceberg Flink sample applications to AWS Managed Service for Apache Flink.

## Prerequisites

- AWS CLI configured with credentials
- Node.js 18+ and npm
- Docker (for building Flink JARs)
- AWS CDK CLI: `npm install -g aws-cdk`

## Setup

```bash
cd cdk-infrastructure
npm install
cdk bootstrap  # First time only
```

## Deploy Applications

### Sink Applications (Kinesis → Iceberg)

#### DataStream API (with optional maintenance)

```bash
cdk deploy -c appType=datastream -c enableMaintenance=true
```

#### SQL API

```bash
cdk deploy -c appType=sql
```

#### Dynamic Sink

```bash
cdk deploy -c appType=dynamic
```

### Source Applications (Iceberg → Kinesis)

#### Iceberg Source (DataStream API)

```bash
cdk deploy -c appType=iceberg-source
```

#### Iceberg Source SQL

```bash
cdk deploy -c appType=iceberg-source-sql
```

#### Hybrid Source (Iceberg bootstrap + Kinesis streaming)

```bash
cdk deploy -c appType=hybrid
```

### Using S3 Tables Catalog

All applications support S3 Tables as an alternative to Glue Catalog:

```bash
cdk deploy -c appType=datastream -c catalogType=s3tables
cdk deploy -c appType=iceberg-source -c catalogType=s3tables
```

## Parameters

- `appType`: Choose which Flink application to deploy
  - **Sink applications** (read from Kinesis, write to Iceberg):
    - `datastream` - DataStream API with IcebergSink and optional maintenance
    - `sql` - Flink SQL API with multi-table routing
    - `dynamic` - Dynamic Iceberg Sink with automatic table routing
  - **Source applications** (read from Iceberg, write to Kinesis):
    - `iceberg-source` - FLIP-27 IcebergSource with DataStream API
    - `iceberg-source-sql` - Flink SQL for reading Iceberg tables
    - `hybrid` - Bootstrap from Iceberg, then switch to Kinesis streaming

- `enableMaintenance`: Enable table maintenance (DataStream only)
  - `true` - Deploys with RDS PostgreSQL for distributed locks
  - `false` - Deploys without maintenance (default)

- `catalogType`: Choose the Iceberg catalog implementation
  - `glue` - AWS Glue Data Catalog (default)
  - `s3tables` - Amazon S3 Tables (automatic maintenance, no RDS needed)

- `enableNag`: Enable CDK Nag security checks
  - `true` - Run AWS Solutions security checks (default)
  - `false` - Skip security checks

## What Gets Deployed

### All Deployments
- AWS Managed Flink application (Flink 1.20)
- CloudWatch Log Group
- IAM roles and policies

### Sink Applications (datastream, sql, dynamic)
- Kinesis Data Stream for source events
- S3 bucket for Iceberg warehouse (Glue catalog only)
- Glue Database or S3 Tables namespace

### Source Applications (iceberg-source, iceberg-source-sql)
- Kinesis Data Stream for sink output
- S3 bucket for Iceberg warehouse (Glue catalog only)
- Glue Database or S3 Tables namespace

### Hybrid Application
- Kinesis Data Stream for source events (after bootstrap)
- Kinesis Data Stream for sink output
- S3 bucket for Iceberg warehouse (Glue catalog only)
- Glue Database or S3 Tables namespace

### With Maintenance Enabled (DataStream + Glue only)
- RDS PostgreSQL instance for lock coordination
- VPC with private subnets
- Security groups
- Secrets Manager for database credentials

## Architecture

### Sink Applications
```
Kinesis Stream → Flink Application → Iceberg Tables (S3 + Glue/S3Tables)
                        ↓
                  RDS PostgreSQL (maintenance locks, optional)
```

### Source Applications
```
Iceberg Tables (S3 + Glue/S3Tables) → Flink Application → Kinesis Stream
```

### Hybrid Application
```
Phase 1: Iceberg Tables → Flink Application → Kinesis Sink
Phase 2: Kinesis Source → Flink Application → Kinesis Sink
```

## Post-Deployment

1. **Start the application** in AWS Console
2. **Generate test data** using the data-generator (for sink apps)
3. **Monitor** via CloudWatch Logs and Flink dashboard
4. **Query tables** using Athena (for sink apps)

## Cleanup

```bash
cdk destroy -c appType=<your-app-type>
```

## Examples

```bash
# Deploy DataStream with maintenance (Glue catalog)
cdk deploy -c appType=datastream -c enableMaintenance=true

# Deploy DataStream with S3 Tables (automatic maintenance)
cdk deploy -c appType=datastream -c catalogType=s3tables

# Deploy Iceberg Source (reads from Iceberg, writes to Kinesis)
cdk deploy -c appType=iceberg-source

# Deploy Hybrid Source (bootstrap from Iceberg, then stream from Kinesis)
cdk deploy -c appType=hybrid

# Synthesize CloudFormation template
cdk synth -c appType=datastream

# View differences
cdk diff -c appType=datastream

# Skip CDK Nag checks
cdk deploy -c appType=datastream -c enableNag=false
```

## Cost Optimization

- Flink: ~$0.11/hour per KPU (2 KPUs minimum)
- RDS t3.micro: ~$0.017/hour (only if maintenance enabled)
- Kinesis: $0.015/hour per shard + data charges
- S3: Storage and request charges
- S3 Tables: Storage and request charges (includes automatic maintenance)

## Notes

- The CDK builds the Flink JAR using Maven in Docker during synthesis
- Shared dependencies (shared-common) are automatically included
- Runtime properties are configured based on app type
- S3 Tables handles maintenance automatically - don't enable `enableMaintenance` with `catalogType=s3tables`
- For maintenance with Glue catalog, update database credentials in AWS Console after deployment
- Streaming reads from Iceberg only work for APPEND-ONLY tables (no upsert/CDC support)
