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
```

## Deploy Applications

### Deploy DataStream API (with maintenance)

```bash
cdk deploy -c appType=datastream -c enableMaintenance=true
```

### Deploy SQL API

```bash
cdk deploy -c appType=sql
```

### Deploy Dynamic Sink

```bash
cdk deploy -c appType=dynamic
```

## Parameters

- `appType`: Choose which Flink application to deploy
  - `datastream` - DataStream API with IcebergSink and optional maintenance
  - `sql` - Flink SQL API with multi-table routing
  - `dynamic` - Dynamic Iceberg Sink with automatic table routing

- `enableMaintenance`: Enable table maintenance (DataStream only)
  - `true` - Deploys with RDS PostgreSQL for distributed locks
  - `false` - Deploys without maintenance (default)

## What Gets Deployed

### All Deployments
- AWS Managed Flink application (Flink 1.20)
- Kinesis Data Stream for events
- S3 bucket for Iceberg warehouse
- CloudWatch Log Group
- IAM roles and policies

### With Maintenance Enabled
- RDS PostgreSQL instance for lock coordination
- VPC with private subnets
- Security groups
- Secrets Manager for database credentials

## Architecture

```
Kinesis Stream → Flink Application → Iceberg Tables (S3 + Glue Catalog)
                        ↓
                  RDS PostgreSQL (maintenance locks)
```

## Post-Deployment

1. **Start the application** in AWS Console
2. **Generate test data** using the data-generator
3. **Monitor** via CloudWatch Logs and Flink dashboard
4. **Query tables** using Athena

## Cleanup

```bash
cdk destroy
```

## Examples

```bash
# Deploy DataStream with maintenance
cdk deploy -c appType=datastream -c enableMaintenance=true

# Deploy SQL without maintenance
cdk deploy -c appType=sql

# Deploy Dynamic Sink
cdk deploy -c appType=dynamic

# Synthesize CloudFormation template
cdk synth -c appType=datastream

# View differences
cdk diff -c appType=datastream
```

## Cost Optimization

- Flink: ~$0.11/hour per KPU (2 KPUs minimum)
- RDS t3.micro: ~$0.017/hour (only if maintenance enabled)
- Kinesis: $0.015/hour per shard + data charges
- S3: Storage and request charges

## Notes

- The CDK builds the Flink JAR using Maven in Docker during synthesis
- Shared dependencies (shared-common) are automatically included
- Runtime properties are configured based on app type
- For maintenance, update database credentials in AWS Console after deployment
