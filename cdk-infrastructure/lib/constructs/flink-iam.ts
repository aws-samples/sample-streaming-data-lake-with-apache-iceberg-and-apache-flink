import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';

export interface FlinkIamProps {
  appType: string;
  catalogType: 'glue' | 's3tables';
  databaseName: string;
  logGroup: logs.LogGroup;
  sourceStream?: kinesis.Stream;
  sinkStream?: kinesis.Stream;
  warehouseBucket?: s3.Bucket;
  s3TableBucketName?: string;
  cdkBootstrapQualifier: string;
  account: string;
  region: string;
  // Maintenance-related
  enableMaintenance: boolean;
  vpc?: ec2.IVpc;
  dbSecret?: secretsmanager.ISecret;
  // Source-app overrides (grant read access to external Iceberg source)
  sourceWarehouse?: string;        // S3 warehouse path (e.g., s3://bucket/warehouse)
  sourceTableBucketArn?: string;   // S3 Table Bucket ARN
  sourceDatabase?: string;         // External Glue database name (for glue: permissions)
  // Glue Schema Registry (for dynamic-avro app type)
  schemaRegistryName?: string;
}

export class FlinkIam extends Construct {
  public readonly role: iam.Role;

  constructor(scope: Construct, id: string, props: FlinkIamProps) {
    super(scope, id);

    this.role = new iam.Role(this, 'FlinkRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      description: `Role for Iceberg Flink ${props.appType} application`,
    });

    // CloudWatch Logs
    this.role.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        'logs:DescribeLogGroups', 'logs:DescribeLogStreams',
        'logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents',
      ],
      resources: [props.logGroup.logGroupArn, `${props.logGroup.logGroupArn}:*`],
    }));

    // Kinesis source
    if (props.sourceStream) {
      props.sourceStream.grantRead(this.role);
      this.role.addToPolicy(new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'kinesis:DescribeStream', 'kinesis:DescribeStreamSummary',
          'kinesis:DescribeStreamConsumer', 'kinesis:RegisterStreamConsumer',
          'kinesis:DeregisterStreamConsumer', 'kinesis:ListShards', 'kinesis:SubscribeToShard',
        ],
        resources: [props.sourceStream.streamArn],
      }));
    }

    // Kinesis sink
    if (props.sinkStream) {
      props.sinkStream.grantWrite(this.role);
      this.role.addToPolicy(new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'kinesis:DescribeStream', 'kinesis:DescribeStreamSummary',
          'kinesis:PutRecord', 'kinesis:PutRecords',
        ],
        resources: [props.sinkStream.streamArn],
      }));
    }

    // S3 warehouse bucket (Glue only)
    if (props.catalogType === 'glue' && props.warehouseBucket) {
      props.warehouseBucket.grantReadWrite(this.role);
    }

    // External source warehouse (for source apps pointing at existing tables)
    if (props.catalogType === 'glue' && props.sourceWarehouse) {
      // Parse bucket name from s3://bucket/path
      const match = props.sourceWarehouse.match(/^s3:\/\/([^/]+)/);
      if (match) {
        const sourceBucketArn = `arn:aws:s3:::${match[1]}`;
        this.role.addToPolicy(new iam.PolicyStatement({
          actions: ['s3:GetObject', 's3:GetObjectVersion', 's3:ListBucket', 's3:GetBucketLocation'],
          resources: [sourceBucketArn, `${sourceBucketArn}/*`],
        }));
      }
    }
    if (props.catalogType === 's3tables' && props.sourceTableBucketArn) {
      this.role.addToPolicy(new iam.PolicyStatement({
        actions: [
          's3tables:GetTableBucket', 's3tables:GetNamespace', 's3tables:ListNamespaces',
          's3tables:ListTables', 's3tables:GetTable', 's3tables:GetTableMetadataLocation',
          's3tables:GetTableData',
        ],
        resources: [props.sourceTableBucketArn, `${props.sourceTableBucketArn}/table/*`],
      }));
    }

    // CDK assets bucket
    this.role.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: [
        's3:GetObject', 's3:GetObjectVersion', 's3:ListBucket',
        's3:GetBucketLocation', 's3:GetBucketVersioning',
      ],
      resources: [
        `arn:aws:s3:::cdk-${props.cdkBootstrapQualifier}-assets-${props.account}-${props.region}`,
        `arn:aws:s3:::cdk-${props.cdkBootstrapQualifier}-assets-${props.account}-${props.region}/*`,
      ],
    }));

    // Catalog-specific permissions
    if (props.catalogType === 'glue') {
      this.role.addToPolicy(new iam.PolicyStatement({
        actions: [
          'glue:GetDatabase', 'glue:GetDatabases', 'glue:GetTable', 'glue:GetTables',
          'glue:GetPartition', 'glue:GetPartitions', 'glue:BatchGetPartition',
        ],
        resources: [
          `arn:aws:glue:${props.region}:${props.account}:catalog`,
          `arn:aws:glue:${props.region}:${props.account}:database/*`,
          `arn:aws:glue:${props.region}:${props.account}:table/*/*`,
        ],
      }));
      this.role.addToPolicy(new iam.PolicyStatement({
        actions: [
          'glue:CreateDatabase', 'glue:UpdateDatabase', 'glue:CreateTable', 'glue:UpdateTable',
          'glue:DeleteTable', 'glue:CreatePartition', 'glue:BatchCreatePartition',
          'glue:UpdatePartition', 'glue:DeletePartition', 'glue:BatchDeletePartition',
        ],
        resources: [
          `arn:aws:glue:${props.region}:${props.account}:catalog`,
          `arn:aws:glue:${props.region}:${props.account}:database/${props.databaseName}`,
          `arn:aws:glue:${props.region}:${props.account}:table/${props.databaseName}/*`,
        ],
      }));
    } else {
      this.role.addToPolicy(new iam.PolicyStatement({
        sid: 'S3TablesTableBucketAccess',
        actions: [
          's3tables:GetTableBucket', 's3tables:ListTableBuckets',
          's3tables:GetTableBucketMaintenanceConfiguration',
          's3tables:CreateNamespace', 's3tables:GetNamespace', 's3tables:ListNamespaces',
          's3tables:DeleteNamespace', 's3tables:CreateTable', 's3tables:ListTables',
        ],
        resources: [`arn:aws:s3tables:${props.region}:${props.account}:bucket/${props.s3TableBucketName}`],
      }));
      this.role.addToPolicy(new iam.PolicyStatement({
        sid: 'S3TablesTableAccess',
        actions: [
          's3tables:GetTable', 's3tables:DeleteTable', 's3tables:RenameTable',
          's3tables:GetTableMetadataLocation', 's3tables:UpdateTableMetadataLocation',
          's3tables:GetTableMaintenanceConfiguration', 's3tables:PutTableMaintenanceConfiguration',
          's3tables:GetTableData', 's3tables:PutTableData',
          's3tables:GetTablePolicy', 's3tables:PutTablePolicy',
        ],
        resources: [`arn:aws:s3tables:${props.region}:${props.account}:bucket/${props.s3TableBucketName}/table/*`],
      }));
    }

    // Glue Schema Registry permissions (for dynamic-avro app type)
    if (props.schemaRegistryName) {
      this.role.addToPolicy(new iam.PolicyStatement({
        sid: 'GlueSchemaRegistryAccess',
        actions: [
          'glue:GetRegistry', 'glue:ListRegistries',
          'glue:CreateSchema', 'glue:DeleteSchema', 'glue:UpdateSchema',
          'glue:GetSchema', 'glue:ListSchemas',
          'glue:RegisterSchemaVersion', 'glue:GetSchemaVersion', 'glue:ListSchemaVersions',
          'glue:GetSchemaByDefinition',
          'glue:QuerySchemaVersionMetadata', 'glue:PutSchemaVersionMetadata',
          'glue:CheckSchemaVersionValidity',
        ],
        resources: [
          `arn:aws:glue:${props.region}:${props.account}:registry/${props.schemaRegistryName}`,
          `arn:aws:glue:${props.region}:${props.account}:schema/${props.schemaRegistryName}/*`,
        ],
      }));
    }

    // CloudWatch Metrics
    this.role.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['cloudwatch:PutMetricData'],
      resources: ['*'],
      conditions: { StringEquals: { 'cloudwatch:namespace': 'AWS/KinesisAnalytics' } },
    }));

    // Self-describe
    this.role.addToPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['kinesisanalytics:DescribeApplication', 'kinesisanalytics:ListApplicationSnapshots'],
      resources: [`arn:aws:kinesisanalytics:${props.region}:${props.account}:application/iceberg-flink-${props.appType}*`],
    }));

    // VPC + RDS permissions for maintenance
    if (props.enableMaintenance && props.vpc && props.dbSecret) {
      this.role.addToPolicy(new iam.PolicyStatement({
        actions: [
          'ec2:DescribeVpcs', 'ec2:DescribeSubnets', 'ec2:DescribeSecurityGroups',
          'ec2:DescribeDhcpOptions', 'ec2:DescribeNetworkInterfaces',
          'ec2:CreateNetworkInterface', 'ec2:CreateNetworkInterfacePermission',
          'ec2:DeleteNetworkInterface',
        ],
        resources: ['*'],
      }));
      props.dbSecret.grantRead(this.role);
    }
  }
}
