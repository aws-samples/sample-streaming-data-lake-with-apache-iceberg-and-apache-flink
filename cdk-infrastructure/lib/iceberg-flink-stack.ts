import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as s3tables from 'aws-cdk-lib/aws-s3tables';
import * as kinesisanalytics from 'aws-cdk-lib/aws-kinesisanalyticsv2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import * as rds from 'aws-cdk-lib/aws-rds';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { Construct } from 'constructs';

export interface IcebergFlinkStackProps extends cdk.StackProps {
  appType: 'datastream' | 'sql' | 'dynamic' | 'iceberg-source' | 'iceberg-source-sql' | 'hybrid';
  enableMaintenance: boolean;
  catalogType?: 'glue' | 's3tables';  // Default: 'glue'
}

export class IcebergFlinkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: IcebergFlinkStackProps) {
    super(scope, id, props);

    const { appType, enableMaintenance, catalogType = 'glue' } = props;
    
    // Validate: S3 Tables handles maintenance automatically, don't allow enableMaintenance with s3tables
    if (catalogType === 's3tables' && enableMaintenance) {
      throw new Error('S3 Tables handles maintenance automatically. Do not enable maintenance when using S3 Tables catalog.');
    }

    // App configuration mapping
    const appConfig = {
      datastream: {
        modulePath: '../datastream-sample',
        jarName: 'datastream-sample-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.datastream.DataStreamIcebergJob',
        description: 'DataStream API with Iceberg Sink and optional maintenance',
        needsSourceStream: true,
        needsSinkStream: false,
      },
      sql: {
        modulePath: '../flink-sql-sample',
        jarName: 'flink-sql-sample-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.sql.FlinkSqlIcebergJob',
        description: 'Flink SQL API with multi-table routing',
        needsSourceStream: true,
        needsSinkStream: false,
      },
      dynamic: {
        modulePath: '../dynamic-sink-sample',
        jarName: 'dynamic-sink-sample-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.dynamic.DynamicSinkJob',
        description: 'Dynamic Iceberg Sink with automatic table routing',
        needsSourceStream: true,
        needsSinkStream: false,
      },
      'iceberg-source': {
        modulePath: '../iceberg-source-datastream',
        jarName: 'iceberg-source-datastream-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.source.IcebergSourceJob',
        description: 'Read from Iceberg tables using DataStream API, write to Kinesis',
        needsSourceStream: false,  // Reads from Iceberg, not Kinesis
        needsSinkStream: true,     // Writes to Kinesis
      },
      'iceberg-source-sql': {
        modulePath: '../iceberg-source-sql',
        jarName: 'iceberg-source-sql-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.source.sql.IcebergSourceSqlJob',
        description: 'Read from Iceberg tables using SQL API, write to Kinesis',
        needsSourceStream: false,  // Reads from Iceberg, not Kinesis
        needsSinkStream: true,     // Writes to Kinesis
      },
      hybrid: {
        modulePath: '../hybrid-source-sample',
        jarName: 'hybrid-source-sample-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.hybrid.HybridSourceJob',
        description: 'Bootstrap from Iceberg then switch to Kinesis streaming',
        needsSourceStream: true,   // Reads from Kinesis (after Iceberg bootstrap)
        needsSinkStream: true,     // Writes to Kinesis
      },
    };

    const config = appConfig[appType];

    // Kinesis stream for events (source - unique per app type)
    // Only create if the app needs a source stream
    let eventStream: kinesis.Stream | undefined;
    if (config.needsSourceStream) {
      eventStream = new kinesis.Stream(this, 'EventStream', {
        streamName: `iceberg-events-${appType}`,
        shardCount: 2,
        retentionPeriod: cdk.Duration.hours(24),
        streamMode: kinesis.StreamMode.PROVISIONED,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      
      // Override CFN resource to ensure deletion on stack failure
      const cfnStream = eventStream.node.defaultChild as kinesis.CfnStream;
      cfnStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    }
    
    // Kinesis stream for output (sink - for Iceberg source apps)
    let sinkStream: kinesis.Stream | undefined;
    if (config.needsSinkStream) {
      sinkStream = new kinesis.Stream(this, 'SinkStream', {
        streamName: `iceberg-output-${appType}`,
        shardCount: 2,
        retentionPeriod: cdk.Duration.hours(24),
        streamMode: kinesis.StreamMode.PROVISIONED,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      
      const cfnSinkStream = sinkStream.node.defaultChild as kinesis.CfnStream;
      cfnSinkStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    }

    // Database/namespace name (unique per app type)
    const glueDatabaseName = `iceberg_${appType}`;
    
    // Resources that differ based on catalog type
    let warehouseBucket: s3.Bucket | undefined;
    let glueDatabase: glue.CfnDatabase | undefined;
    let s3TableBucketArn: string | undefined;
    let s3TableBucketName: string | undefined;
    
    if (catalogType === 'glue') {
      // S3 bucket for Iceberg warehouse (only needed for Glue catalog)
      warehouseBucket = new s3.Bucket(this, 'IcebergWarehouse', {
        bucketName: `iceberg-warehouse-${appType}-${this.account}`,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        autoDeleteObjects: true,
        versioned: true,
        encryption: s3.BucketEncryption.S3_MANAGED,
        blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
      });

      // Glue database for Iceberg catalog
      glueDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
        catalogId: this.account,
        databaseInput: {
          name: glueDatabaseName,
          description: `Iceberg tables for ${appType} Flink sample`,
          locationUri: `s3://${warehouseBucket.bucketName}/warehouse/${appType}`,
        },
      });
      glueDatabase.node.addDependency(warehouseBucket);
    } else {
      // S3 Tables catalog - create S3 Table Bucket (manages storage internally)
      s3TableBucketName = `iceberg-tables-${appType}-${this.account}`;
      
      const tableBucket = new s3tables.CfnTableBucket(this, 'S3TableBucket', {
        tableBucketName: s3TableBucketName,
      });
      tableBucket.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
      
      s3TableBucketArn = tableBucket.attrTableBucketArn;
      
      // Create namespace in S3 Tables (equivalent to database)
      const namespace = new s3tables.CfnNamespace(this, 'S3TablesNamespace', {
        namespace: glueDatabaseName,
        tableBucketArn: s3TableBucketArn,
      });
      namespace.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
      namespace.node.addDependency(tableBucket);
      
      // Custom resource to clean up tables before namespace deletion
      // S3 Tables namespaces cannot be deleted if they contain tables
      const cleanupRole = new iam.Role(this, 'S3TablesCleanupRole', {
        assumedBy: new iam.ServicePrincipal('lambda.amazonaws.com'),
        managedPolicies: [
          iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSLambdaBasicExecutionRole'),
        ],
      });
      
      cleanupRole.addToPolicy(new iam.PolicyStatement({
        actions: [
          's3tables:ListTables',
          's3tables:DeleteTable',
          's3tables:GetTable',
        ],
        resources: [
          `arn:aws:s3tables:${this.region}:${this.account}:bucket/${s3TableBucketName}`,
          `arn:aws:s3tables:${this.region}:${this.account}:bucket/${s3TableBucketName}/table/*`,
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
    
    # Only run cleanup on Delete
    if event['RequestType'] != 'Delete':
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {})
        return
    
    try:
        table_bucket_arn = event['ResourceProperties']['TableBucketArn']
        namespace = event['ResourceProperties']['Namespace']
        
        client = boto3.client('s3tables')
        
        # List all tables in the namespace
        paginator = client.get_paginator('list_tables')
        tables_to_delete = []
        
        for page in paginator.paginate(tableBucketARN=table_bucket_arn, namespace=namespace):
            for table in page.get('tables', []):
                tables_to_delete.append(table['name'])
        
        print(f"Found {len(tables_to_delete)} tables to delete: {tables_to_delete}")
        
        # Delete each table
        for table_name in tables_to_delete:
            print(f"Deleting table: {table_name}")
            try:
                client.delete_table(
                    tableBucketARN=table_bucket_arn,
                    namespace=namespace,
                    name=table_name
                )
                print(f"Deleted table: {table_name}")
            except Exception as e:
                print(f"Error deleting table {table_name}: {e}")
        
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {'TablesDeleted': len(tables_to_delete)})
        
    except Exception as e:
        print(f"Error: {e}")
        # Still return success to allow stack deletion to proceed
        cfnresponse.send(event, context, cfnresponse.SUCCESS, {'Error': str(e)})
`),
        }),
      });
      
      const cleanup = new cdk.CustomResource(this, 'S3TablesCleanup', {
        serviceToken: cleanupProvider.serviceToken,
        properties: {
          TableBucketArn: s3TableBucketArn,
          Namespace: glueDatabaseName,
        },
      });
      
      // IMPORTANT: Dependency ordering for proper deletion sequence
      // The custom resource cleanup must be deleted BEFORE the namespace
      // In CloudFormation, deletion order is REVERSE of creation order
      // So if namespace depends on cleanup:
      // - CREATE: cleanup first, then namespace
      // - DELETE: namespace first, then cleanup (WRONG - we need cleanup first!)
      // 
      // The trick: make cleanup depend on namespace, so:
      // - CREATE: namespace first, then cleanup (cleanup does nothing on create)
      // - DELETE: cleanup first (deletes tables), then namespace (now empty, can be deleted)
      cleanup.node.addDependency(namespace);
    }

    // Build Flink JAR using Maven in Docker and upload to our own bucket
    // Copies entire project to include parent POM and shared-common module
    const flinkJarAsset = new s3assets.Asset(this, 'FlinkJarAsset', {
      path: '..',  // Parent directory containing all modules
      exclude: ['cdk-infrastructure', 'cdk.out', '.git', '.idea', '.kiro', 'target', 'node_modules'],
      bundling: {
        image: cdk.DockerImage.fromRegistry('maven:3.9-eclipse-temurin-11'),
        command: [
          'bash',
          '-c',
          [
            'mkdir -p /tmp/.m2',
            'cp -r /asset-input/* /tmp/',
            'cd /tmp',
            `mvn clean package -DskipTests -pl ${config.modulePath.replace('../', '')} -am -Dmaven.repo.local=/tmp/.m2 -q`,
            `mkdir -p /asset-output`,
            `cp /tmp/${config.modulePath.replace('../', '')}/target/${config.jarName} /asset-output/app.jar`,
          ].join(' && '),
        ],
        user: 'root',
        outputType: cdk.BundlingOutput.SINGLE_FILE,
      },
    });
    
    // VPC and RDS for maintenance (DataStream with maintenance only)
    let vpc: ec2.IVpc | undefined;
    let database: rds.DatabaseInstance | undefined;
    let dbSecret: secretsmanager.ISecret | undefined;
    let flinkSecurityGroup: ec2.ISecurityGroup | undefined;

    if (appType === 'datastream' && enableMaintenance) {
      // Create VPC for RDS and Flink
      vpc = new ec2.Vpc(this, 'FlinkVpc', {
        maxAzs: 2,
        natGateways: 1,
        subnetConfiguration: [
          {
            name: 'Public',
            subnetType: ec2.SubnetType.PUBLIC,
            cidrMask: 24,
          },
          {
            name: 'Private',
            subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS,
            cidrMask: 24,
          },
        ],
      });

      // Security group for Flink application
      flinkSecurityGroup = new ec2.SecurityGroup(this, 'FlinkSecurityGroup', {
        vpc,
        description: 'Security group for Flink application',
        allowAllOutbound: true,
      });

      // Security group for RDS
      const dbSecurityGroup = new ec2.SecurityGroup(this, 'DbSecurityGroup', {
        vpc,
        description: 'Security group for Iceberg maintenance database',
        allowAllOutbound: false,
      });

      // Allow Flink to connect to RDS on PostgreSQL port
      dbSecurityGroup.addIngressRule(
        flinkSecurityGroup,
        ec2.Port.tcp(5432),
        'Allow Flink to connect to PostgreSQL'
      );

      // Create database credentials
      dbSecret = new secretsmanager.Secret(this, 'DbSecret', {
        generateSecretString: {
          secretStringTemplate: JSON.stringify({ username: 'flink' }),
          generateStringKey: 'password',
          excludePunctuation: true,
          passwordLength: 32,
        },
      });

      // Create RDS PostgreSQL for maintenance locks
      database = new rds.DatabaseInstance(this, 'MaintenanceDb', {
        engine: rds.DatabaseInstanceEngine.postgres({
          version: rds.PostgresEngineVersion.VER_15,
        }),
        instanceType: ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MICRO),
        vpc,
        vpcSubnets: { subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS },
        securityGroups: [dbSecurityGroup],
        databaseName: 'iceberg_locks',
        credentials: rds.Credentials.fromSecret(dbSecret),
        allocatedStorage: 20,
        maxAllocatedStorage: 100,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
        deletionProtection: false,
        backupRetention: cdk.Duration.days(7),
      });

      new cdk.CfnOutput(this, 'DatabaseEndpoint', {
        value: database.dbInstanceEndpointAddress,
        description: 'RDS PostgreSQL endpoint for maintenance locks',
      });

      new cdk.CfnOutput(this, 'DatabaseSecretArn', {
        value: dbSecret.secretArn,
        description: 'Secrets Manager ARN for database credentials',
      });
    }

    // CloudWatch log group
    const logGroup = new logs.LogGroup(this, 'FlinkLogGroup', {
      logGroupName: `/aws/kinesisanalytics/iceberg-flink-${appType}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // Create log stream for Flink application
    // Managed Flink requires the log stream to exist before it can write to it
    const logStream = new logs.LogStream(this, 'FlinkLogStream', {
      logGroup: logGroup,
      logStreamName: 'kinesis-analytics-log-stream',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // IAM role for Flink application
    const flinkRole = new iam.Role(this, 'FlinkRole', {
      assumedBy: new iam.ServicePrincipal('kinesisanalytics.amazonaws.com'),
      description: `Role for Iceberg Flink ${appType} application`,
    });

    // Explicit CloudWatch Logs permissions for Managed Flink
    flinkRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'logs:DescribeLogGroups',
          'logs:DescribeLogStreams',
          'logs:CreateLogGroup',
          'logs:CreateLogStream',
          'logs:PutLogEvents',
        ],
        resources: [
          logGroup.logGroupArn,
          `${logGroup.logGroupArn}:*`,
        ],
      })
    );

    // Grant Kinesis permissions based on stream configuration
    if (eventStream) {
      eventStream.grantRead(flinkRole);
      
      // Kinesis permissions - grantRead() doesn't include DescribeStream
      flinkRole.addToPolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'kinesis:DescribeStream',
            'kinesis:DescribeStreamSummary',
            'kinesis:DescribeStreamConsumer',
            'kinesis:RegisterStreamConsumer',
            'kinesis:DeregisterStreamConsumer',
            'kinesis:ListShards',
            'kinesis:SubscribeToShard',
          ],
          resources: [eventStream.streamArn],
        })
      );
    }
    
    if (sinkStream) {
      sinkStream.grantWrite(flinkRole);
      
      // Kinesis sink permissions
      flinkRole.addToPolicy(
        new iam.PolicyStatement({
          effect: iam.Effect.ALLOW,
          actions: [
            'kinesis:DescribeStream',
            'kinesis:DescribeStreamSummary',
            'kinesis:PutRecord',
            'kinesis:PutRecords',
          ],
          resources: [sinkStream.streamArn],
        })
      );
    }
    
    // S3 warehouse bucket permissions (only for Glue catalog)
    if (catalogType === 'glue' && warehouseBucket) {
      warehouseBucket.grantReadWrite(flinkRole);
    }
    
    // CRITICAL: S3 permissions for the specific CDK assets bucket
    flinkRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          's3:GetObject',
          's3:GetObjectVersion',
          's3:ListBucket',
          's3:GetBucketLocation',
          's3:GetBucketVersioning',
        ],
        resources: [
          `arn:aws:s3:::cdk-hnb659fds-assets-${this.account}-${this.region}`,
          `arn:aws:s3:::cdk-hnb659fds-assets-${this.account}-${this.region}/*`,
        ],
      })
    );

    // Grant Glue permissions for Iceberg catalog (only when using Glue)
    if (catalogType === 'glue') {
      // Read permissions on all databases (Glue catalog validation requires this)
      flinkRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            'glue:GetDatabase',
            'glue:GetDatabases',
            'glue:GetTable',
            'glue:GetTables',
            'glue:GetPartition',
            'glue:GetPartitions',
            'glue:BatchGetPartition',
          ],
          resources: [
            `arn:aws:glue:${this.region}:${this.account}:catalog`,
            `arn:aws:glue:${this.region}:${this.account}:database/*`,
            `arn:aws:glue:${this.region}:${this.account}:table/*/*`,
          ],
        })
      );

      // Write permissions scoped to our specific database
      flinkRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            'glue:CreateDatabase',
            'glue:UpdateDatabase',
            'glue:CreateTable',
            'glue:UpdateTable',
            'glue:DeleteTable',
            'glue:CreatePartition',
            'glue:BatchCreatePartition',
            'glue:UpdatePartition',
            'glue:DeletePartition',
            'glue:BatchDeletePartition',
          ],
          resources: [
            `arn:aws:glue:${this.region}:${this.account}:catalog`,
            `arn:aws:glue:${this.region}:${this.account}:database/${glueDatabaseName}`,
            `arn:aws:glue:${this.region}:${this.account}:table/${glueDatabaseName}/*`,
          ],
        })
      );
    } else {
      // S3 Tables permissions - complete API access
      // Reference: https://docs.aws.amazon.com/service-authorization/latest/reference/list_amazons3tables.html
      flinkRole.addToPolicy(
        new iam.PolicyStatement({
          sid: 'S3TablesTableBucketAccess',
          actions: [
            // Table Bucket operations
            's3tables:GetTableBucket',
            's3tables:ListTableBuckets',
            's3tables:GetTableBucketMaintenanceConfiguration',
            // Namespace operations
            's3tables:CreateNamespace',
            's3tables:GetNamespace',
            's3tables:ListNamespaces',
            's3tables:DeleteNamespace',
            // Table operations that require bucket-level permission
            's3tables:CreateTable',  // CreateTable is a bucket-level action
            's3tables:ListTables',
          ],
          resources: [
            `arn:aws:s3tables:${this.region}:${this.account}:bucket/${s3TableBucketName}`,
          ],
        })
      );
      
      flinkRole.addToPolicy(
        new iam.PolicyStatement({
          sid: 'S3TablesTableAccess',
          actions: [
            // Table metadata operations (require table-level permission)
            's3tables:GetTable',
            's3tables:DeleteTable',
            's3tables:RenameTable',
            's3tables:GetTableMetadataLocation',
            's3tables:UpdateTableMetadataLocation',
            's3tables:GetTableMaintenanceConfiguration',
            's3tables:PutTableMaintenanceConfiguration',
            // Table data operations (CRITICAL for reading/writing data)
            's3tables:GetTableData',
            's3tables:PutTableData',
            // Table policy operations
            's3tables:GetTablePolicy',
            's3tables:PutTablePolicy',
          ],
          resources: [
            `arn:aws:s3tables:${this.region}:${this.account}:bucket/${s3TableBucketName}/table/*`,
          ],
        })
      );
    }

    // CloudWatch Metrics - Flink publishes application metrics
    flinkRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['cloudwatch:PutMetricData'],
        resources: ['*'],
        conditions: {
          StringEquals: {
            'cloudwatch:namespace': 'AWS/KinesisAnalytics',
          },
        },
      })
    );

    // Kinesis Analytics permissions - app needs to describe itself
    flinkRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          'kinesisanalytics:DescribeApplication',
          'kinesisanalytics:ListApplicationSnapshots',
        ],
        resources: [
          `arn:aws:kinesisanalytics:${this.region}:${this.account}:application/iceberg-flink-${appType}*`,
        ],
      })
    );

    // Grant VPC and RDS permissions if maintenance is enabled
    if (appType === 'datastream' && enableMaintenance && vpc && dbSecret) {
      // VPC permissions required by Managed Flink for VPC connectivity
      flinkRole.addToPolicy(
        new iam.PolicyStatement({
          actions: [
            'ec2:DescribeVpcs',
            'ec2:DescribeSubnets',
            'ec2:DescribeSecurityGroups',
            'ec2:DescribeDhcpOptions',
            'ec2:DescribeNetworkInterfaces',
            'ec2:CreateNetworkInterface',
            'ec2:CreateNetworkInterfacePermission',
            'ec2:DeleteNetworkInterface',
          ],
          resources: ['*'],
        })
      );
      dbSecret.grantRead(flinkRole);
    }

    // Build runtime properties based on app type and maintenance setting
    const runtimeProperties = this.buildRuntimeProperties(appType, enableMaintenance, catalogType, {
      kinesisSourceStreamArn: eventStream?.streamArn,
      kinesisSinkStreamArn: sinkStream?.streamArn,
      kinesisSinkStreamName: sinkStream?.streamName,
      warehousePath: warehouseBucket ? `s3://${warehouseBucket.bucketName}/warehouse` : '',
      region: this.region,
      dbEndpoint: database?.dbInstanceEndpointAddress,
      dbSecretArn: dbSecret?.secretArn,
      s3TableBucketArn: s3TableBucketArn,
    });

    // // Create Flink application
    const flinkApp = new kinesisanalytics.CfnApplication(this, 'FlinkApplication', {
      applicationName: `iceberg-flink-${appType}${enableMaintenance ? '-maintenance' : ''}`,
      runtimeEnvironment: 'FLINK-1_20',
      serviceExecutionRole: flinkRole.roleArn,
      applicationConfiguration: {
        applicationCodeConfiguration: {
          codeContent: {
            s3ContentLocation: {
              bucketArn: flinkJarAsset.bucket.bucketArn,
              fileKey: flinkJarAsset.s3ObjectKey,
            },
          },
          codeContentType: 'ZIPFILE',
        },
        environmentProperties: {
          propertyGroups: [
            {
              propertyGroupId: 'FlinkApplicationProperties',
              propertyMap: runtimeProperties,
            },
          ],
        },
        flinkApplicationConfiguration: {
          monitoringConfiguration: {
            configurationType: 'CUSTOM',
            metricsLevel: 'APPLICATION',
            logLevel: 'INFO',
          },
          parallelismConfiguration: {
            configurationType: 'CUSTOM',
            parallelism: 2,
            parallelismPerKpu: 1,
            autoScalingEnabled: true,
          },
          checkpointConfiguration: {
            configurationType: 'CUSTOM',
            checkpointingEnabled: true,
            checkpointInterval: 60000,
            minPauseBetweenCheckpoints: 30000,
          },
        },
        applicationSnapshotConfiguration: {
          snapshotsEnabled: true,
        },
        vpcConfigurations: appType === 'datastream' && enableMaintenance && vpc && flinkSecurityGroup
          ? [
              {
                securityGroupIds: [flinkSecurityGroup.securityGroupId],
                subnetIds: vpc.privateSubnets.map((subnet) => subnet.subnetId),
              },
            ]
          : undefined,
      },
      applicationDescription: config.description,
      // CRITICAL: Specify the main class to run
      applicationMode: 'STREAMING',
    });

    // Ensure Flink app is created after JAR is uploaded and role is ready
    flinkApp.node.addDependency(flinkJarAsset);
    flinkApp.node.addDependency(flinkRole);
    if (eventStream) {
      flinkApp.node.addDependency(eventStream);
    }
    if (sinkStream) {
      flinkApp.node.addDependency(sinkStream);
    }
    if (warehouseBucket) {
      flinkApp.node.addDependency(warehouseBucket);
    }
    if (glueDatabase) {
      flinkApp.node.addDependency(glueDatabase);
    }
    flinkApp.node.addDependency(logGroup);
    flinkApp.node.addDependency(logStream);

    // CloudWatch logging for Flink
    const flinkLogging = new kinesisanalytics.CfnApplicationCloudWatchLoggingOption(this, 'FlinkLogging', {
      applicationName: flinkApp.ref,
      cloudWatchLoggingOption: {
        logStreamArn: `arn:aws:logs:${this.region}:${this.account}:log-group:${logGroup.logGroupName}:log-stream:kinesis-analytics-log-stream`,
      },
    });
    flinkLogging.addDependency(flinkApp);

    // Outputs
    new cdk.CfnOutput(this, 'ApplicationName', {
      value: flinkApp.applicationName!,
      description: 'Flink application name',
    });

    if (eventStream) {
      new cdk.CfnOutput(this, 'KinesisSourceStreamName', {
        value: eventStream.streamName,
        description: 'Kinesis stream name for source events',
      });
    }
    
    if (sinkStream) {
      new cdk.CfnOutput(this, 'KinesisSinkStreamName', {
        value: sinkStream.streamName,
        description: 'Kinesis stream name for sink output',
      });
      
      new cdk.CfnOutput(this, 'KinesisSinkStreamArn', {
        value: sinkStream.streamArn,
        description: 'Kinesis stream ARN for sink output',
      });
    }

    if (warehouseBucket) {
      new cdk.CfnOutput(this, 'WarehouseBucket', {
        value: warehouseBucket.bucketName,
        description: 'S3 bucket for Iceberg warehouse',
      });
    }

    new cdk.CfnOutput(this, 'GlueDatabaseName', {
      value: glueDatabaseName,
      description: 'Database name for Iceberg tables',
    });
    
    new cdk.CfnOutput(this, 'CatalogType', {
      value: catalogType,
      description: 'Catalog type (glue or s3tables)',
    });
    
    if (s3TableBucketArn) {
      new cdk.CfnOutput(this, 'S3TableBucketArn', {
        value: s3TableBucketArn,
        description: 'S3 Table Bucket ARN for Iceberg tables',
      });
    }

    new cdk.CfnOutput(this, 'AppType', {
      value: appType,
      description: 'Deployed application type',
    });

    // new cdk.CfnOutput(this, 'FlinkRoleArn', {
    //   value: flinkRole.roleArn,
    //   description: 'Flink application IAM role ARN',
    // });
  }

  private buildRuntimeProperties(
    appType: string,
    enableMaintenance: boolean,
    catalogType: string,
    resources: {
      kinesisSourceStreamArn?: string;
      kinesisSinkStreamArn?: string;
      kinesisSinkStreamName?: string;
      warehousePath: string;
      region: string;
      dbEndpoint?: string;
      dbSecretArn?: string;
      s3TableBucketArn?: string;
    }
  ): { [key: string]: string } {
    // Base properties common to all configurations
    const baseProps: { [key: string]: string } = {
      'aws.region': resources.region,
      'iceberg.catalog.name': catalogType === 's3tables' ? 's3tables_catalog' : 'glue_catalog',
      'iceberg.catalog.type': catalogType,
      'iceberg.database': `iceberg_${appType}`,
      'checkpoint.interval.ms': '60000',
    };
    
    // Add warehouse path only for Glue catalog (S3 Tables manages storage internally)
    if (catalogType === 'glue' && resources.warehousePath) {
      baseProps['iceberg.warehouse'] = resources.warehousePath;
    }
    
    // Add S3 Tables specific properties
    if (catalogType === 's3tables' && resources.s3TableBucketArn) {
      baseProps['s3tables.bucket.arn'] = resources.s3TableBucketArn;
    }

    if (appType === 'datastream') {
      const props = {
        ...baseProps,
        'kinesis.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.region': resources.region,
        'iceberg.table': 'orders',
        'enable.maintenance': enableMaintenance.toString(),
        'write.mode': 'upsert',  // Default to upsert for backward compatibility
        'primary.key.columns': 'event_id,event_date,region',
      };

      if (enableMaintenance && resources.dbEndpoint && resources.dbSecretArn) {
        return {
          ...props,
          'rds.jdbc.url': `jdbc:postgresql://${resources.dbEndpoint}:5432/iceberg_locks`,
          'rds.user': 'flink',
          'rds.password': `{{resolve:secretsmanager:${resources.dbSecretArn}:SecretString:password}}`,
        };
      }
      return props;
    } else if (appType === 'sql') {
      const sqlProps: { [key: string]: string } = {
        ...baseProps,
        'kinesis.stream.name': `iceberg-events-${appType}`,
        'glue.database': `iceberg_${appType}`,  // SQL job expects this property name
        'table.prefix': 'sql_',
        'write.mode': 'append',  // Default to append for SQL (simpler)
        'primary.key.columns': 'event_id,event_date,region',
      };
      
      // Add warehouse path only for Glue catalog
      if (catalogType === 'glue' && resources.warehousePath) {
        sqlProps['s3.warehouse.path'] = resources.warehousePath;
      }
      
      return sqlProps;
    } else if (appType === 'iceberg-source') {
      // Iceberg Source (DataStream API) - reads from Iceberg, writes to Kinesis
      return {
        ...baseProps,
        'iceberg.table': 'orders',  // Default table to read from
        'iceberg.source.streaming': 'true',
        'iceberg.source.starting-strategy': 'INCREMENTAL_FROM_LATEST_SNAPSHOT',
        'iceberg.source.monitor-interval': '60s',
        'kinesis.sink.stream.arn': resources.kinesisSinkStreamArn!,
      };
    } else if (appType === 'iceberg-source-sql') {
      // Iceberg Source SQL - reads from Iceberg using SQL, writes to Kinesis
      return {
        ...baseProps,
        'iceberg.table': 'orders',  // Default table to read from
        'iceberg.source.streaming': 'true',
        'iceberg.source.monitor-interval': '60s',
        'kinesis.sink.stream.name': resources.kinesisSinkStreamName!,
      };
    } else if (appType === 'hybrid') {
      // Hybrid Source - bootstrap from Iceberg, then switch to Kinesis streaming
      return {
        ...baseProps,
        'iceberg.table': 'orders',  // Table to bootstrap from
        'kinesis.source.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.sink.stream.arn': resources.kinesisSinkStreamArn!,
      };
    } else {
      // dynamic
      return {
        ...baseProps,
        'kinesis.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.region': resources.region,
        'write.mode': 'append',  // Default to append for dynamic sink
        'primary.key.columns': 'event_id,event_date,region',
      };
    }
  }
}
