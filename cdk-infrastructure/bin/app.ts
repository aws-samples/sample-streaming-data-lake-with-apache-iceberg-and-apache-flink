#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { Aspects } from 'aws-cdk-lib';
import { AwsSolutionsChecks, NagSuppressions } from 'cdk-nag';
import { IcebergFlinkStack } from '../lib/iceberg-flink-stack';

const app = new cdk.App();

// Get context parameters
const appType = app.node.tryGetContext('appType') || 'datastream';
const enableMaintenance = app.node.tryGetContext('enableMaintenance') === 'true';
const catalogType = app.node.tryGetContext('catalogType') || 'glue';
const enableNag = app.node.tryGetContext('enableNag') !== 'false'; // Enable by default

// Source-app specific context (iceberg-source, iceberg-source-sql, hybrid):
// Override to read from an existing Iceberg table instead of the auto-created one.
const sourceDatabase = app.node.tryGetContext('sourceDatabase');
const sourceTable = app.node.tryGetContext('sourceTable');
const sourceWarehouse = app.node.tryGetContext('sourceWarehouse');
const sourceTableBucketArn = app.node.tryGetContext('sourceTableBucketArn');

// Optional: stack name suffix to allow co-existing deployments (e.g., for testing
// source apps that read from another deployment's Iceberg tables)
const stackSuffix = app.node.tryGetContext('stackSuffix') || '';

const stack = new IcebergFlinkStack(app, `IcebergFlinkStack${stackSuffix ? '-' + stackSuffix : ''}`, {
  appType: appType as 'datastream' | 'sql' | 'dynamic' | 'dynamic-avro' | 'iceberg-source' | 'iceberg-source-sql' | 'hybrid' | 'variant' | 'sql-dynamic' | 'pyflink-dynamic',
  nameSuffix: stackSuffix,
  enableMaintenance: enableMaintenance,
  catalogType: catalogType as 'glue' | 's3tables',
  sourceDatabase,
  sourceTable,
  sourceWarehouse,
  sourceTableBucketArn,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: `Iceberg Flink Sample - ${appType} API${enableMaintenance ? ' with maintenance' : ''} (${catalogType} catalog)`,
});

// Apply CDK Nag - AWS Solutions checks
if (enableNag) {
  Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));
  
  // Add suppressions for known acceptable patterns in this sample
  NagSuppressions.addStackSuppressions(stack, [
    {
      id: 'AwsSolutions-IAM4',
      reason: 'AWS managed policies are acceptable for sample applications',
    },
    {
      id: 'AwsSolutions-IAM5',
      reason: 'Wildcard permissions are required for Iceberg table operations (dynamic table names)',
    },
    {
      id: 'AwsSolutions-S1',
      reason: 'S3 access logging is optional for sample applications - enable in production',
    },
    {
      id: 'AwsSolutions-KDS3',
      reason: 'Kinesis server-side encryption uses AWS managed key for simplicity in samples',
    },
    {
      id: 'AwsSolutions-L1',
      reason: 'Lambda runtime version is managed by CDK custom resource provider',
    },
    {
      id: 'AwsSolutions-S10',
      reason: 'SSL enforcement on S3 bucket is optional for sample applications - enable in production',
    },
  ]);
}

app.synth();
