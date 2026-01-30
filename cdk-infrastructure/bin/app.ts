#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { IcebergFlinkStack } from '../lib/iceberg-flink-stack';

const app = new cdk.App();

// Get context parameters
const appType = app.node.tryGetContext('appType') || 'datastream';
const enableMaintenance = app.node.tryGetContext('enableMaintenance') === 'true';

new IcebergFlinkStack(app, 'IcebergFlinkStack', {
  appType: appType as 'datastream' | 'sql' | 'dynamic',
  enableMaintenance: enableMaintenance,
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION || 'us-east-1',
  },
  description: `Iceberg Flink Sample - ${appType} API${enableMaintenance ? ' with maintenance' : ''}`,
});

app.synth();
