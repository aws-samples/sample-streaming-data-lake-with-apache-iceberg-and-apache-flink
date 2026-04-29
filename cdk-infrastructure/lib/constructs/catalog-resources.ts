import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3tables from 'aws-cdk-lib/aws-s3tables';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';

export interface CatalogResourcesProps {
  appType: string;
  catalogType: 'glue' | 's3tables';
  databaseName: string;
  account: string;
  region: string;
  // When true, skip creating a local warehouse/namespace (used when source app reads
  // from an external Iceberg table provided via context overrides).
  skipCatalogCreation?: boolean;
}

export class CatalogResources extends Construct {
  public readonly warehouseBucket?: s3.Bucket;
  public readonly glueDatabase?: glue.CfnDatabase;
  public readonly s3TableBucketArn?: string;
  public readonly s3TableBucketName?: string;

  constructor(scope: Construct, id: string, props: CatalogResourcesProps) {
    super(scope, id);

    if (props.skipCatalogCreation) {
      return;
    }

    if (props.catalogType === 'glue') {
      this.warehouseBucket = new s3.Bucket(this, 'IcebergWarehouse', {
        bucketName: `iceberg-warehouse-${props.appType}-${props.account}`,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
        versioned: true,
        encryption: s3.BucketEncryption.S3_MANAGED,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });

      this.glueDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
        catalogId: props.account,
        databaseInput: {
          name: props.databaseName,
          description: `Iceberg tables for ${props.appType} Flink sample`,
          locationUri: `s3://${this.warehouseBucket.bucketName}/warehouse/${props.appType}`,
        },
      });
      this.glueDatabase.node.addDependency(this.warehouseBucket);
    } else {
      this.s3TableBucketName = `iceberg-tables-${props.appType}-${props.account}`;

      const tableBucket = new s3tables.CfnTableBucket(this, 'S3TableBucket', {
        tableBucketName: this.s3TableBucketName,
      });
      tableBucket.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

      this.s3TableBucketArn = tableBucket.attrTableBucketArn;

      const namespace = new s3tables.CfnNamespace(this, 'S3TablesNamespace', {
        namespace: props.databaseName,
        tableBucketArn: this.s3TableBucketArn,
      });
      namespace.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
      namespace.node.addDependency(tableBucket);

      // Custom resource to clean up tables before namespace deletion
      const cleanupRole = new iam.Role(this, 'S3TablesCleanupRole', {
        assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        ],
      });

      cleanupRole.addToPolicy(new iam.PolicyStatement({
        actions: ['s3tables:ListTables', 's3tables:DeleteTable', 's3tables:GetTable'],
        resources: [
          `arn:aws:s3tables:${props.region}:${props.account}:bucket/${this.s3TableBucketName}`,
          `arn:aws:s3tables:${props.region}:${props.account}:bucket/${this.s3TableBucketName}/table/*`,
        ],
      }));

      const cleanupProvider = new cdk.custom_resources.Provider(this, 'S3TablesCleanupProvider', {
        onEventHandler: new cdk.aws_lambda.Function(this, 'S3TablesCleanupFunction', {
          runtime: cdk.aws_lambda.Runtime.PYTHON_3_12,
          handler: 'index.handler',
          role: cleanupRole,
          timeout: cdk.Duration.minutes(5),
          code: cdk.aws_lambda.Code.fromInline(`
import boto3
import cfnresponse

def handler(event, context):
    print(f"Event: {event}")
    
    if event['RequestType'] != 'Delete':
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
        return
    
    try:
        table_bucket_arn = event['ResourceProperties']['TableBucketArn']
        namespace = event['ResourceProperties']['Namespace']
        
        client = boto3.client('s3tables')
        paginator = client.get_paginator('list_tables')
        tables_to_delete = []
        
        for page in paginator.paginate(tableBucketARN=table_bucket_arn, namespace=namespace):
            for table in page.get('tables', []):
                tables_to_delete.append(table['name'])
        
        print(f"Found {len(tables_to_delete)} tables to delete: {tables_to_delete}")
        
        for table_name in tables_to_delete:
            print(f"Deleting table: {table_name}")
            try:
                client.delete_table(tableBucketARN=table_bucket_arn, namespace=namespace, name=table_name)
                print(f"Deleted table: {table_name}")
            except Exception as e:
                print(f"Error deleting table {table_name}: {e}")
        
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {'TablesDeleted': len(tables_to_delete)})
    except Exception as e:
        print(f"Error: {e}")
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {'Error': str(e)})
`),
        }),
      });

      const cleanup = new cdk.CustomResource(this, 'S3TablesCleanup', {
        serviceToken: cleanupProvider.serviceToken,
        properties: {
          TableBucketArn: this.s3TableBucketArn,
          Namespace: props.databaseName,
        },
      });
      cleanup.node.addDependency(namespace);
    }
  }
}
