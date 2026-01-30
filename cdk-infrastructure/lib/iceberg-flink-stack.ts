import * as cdk from 'aws-cdk-lib';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
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
  appType: 'datastream' | 'sql' | 'dynamic';
  enableMaintenance: boolean;
}

export class IcebergFlinkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: IcebergFlinkStackProps) {
    super(scope, id, props);

    const { appType, enableMaintenance } = props;

    // App configuration mapping
    const appConfig = {
      datastream: {
        modulePath: '../datastream-sample',
        jarName: 'datastream-sample-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.datastream.DataStreamIcebergJob',
        description: 'DataStream API with Iceberg Sink and optional maintenance',
      },
      sql: {
        modulePath: '../flink-sql-sample',
        jarName: 'flink-sql-sample-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.sql.FlinkSqlIcebergJob',
        description: 'Flink SQL API with multi-table routing',
      },
      dynamic: {
        modulePath: '../dynamic-sink-sample',
        jarName: 'dynamic-sink-sample-1.0-SNAPSHOT.jar',
        mainClass: 'com.aws.samples.iceberg.dynamic.DynamicSinkJob',
        description: 'Dynamic Iceberg Sink with automatic table routing',
      },
    };

    const config = appConfig[appType];

    // S3 bucket for Iceberg warehouse (unique per app type)
    const warehouseBucket = new s3.Bucket(this, 'IcebergWarehouse', {
      bucketName: `iceberg-warehouse-${appType}-${this.account}`,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      autoDeleteObjects: true,  // Automatically delete objects when stack is destroyed
      versioned: true,
      encryption: s3.BucketEncryption.S3_MANAGED,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    });

    // Kinesis stream for events (unique per app type)
    const eventStream = new kinesis.Stream(this, 'EventStream', {
      streamName: `iceberg-events-${appType}`,
      shardCount: 2,
      retentionPeriod: cdk.Duration.hours(24),
      streamMode: kinesis.StreamMode.PROVISIONED,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    
    // Override CFN resource to ensure deletion on stack failure
    const cfnStream = eventStream.node.defaultChild as kinesis.CfnStream;
    cfnStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);

    // Glue database for Iceberg catalog (unique per app type)
    const glueDatabaseName = `iceberg_${appType}`;
    const glueDatabase = new glue.CfnDatabase(this, 'GlueDatabase', {
      catalogId: this.account,
      databaseInput: {
        name: glueDatabaseName,
        description: `Iceberg tables for ${appType} Flink sample`,
        locationUri: `s3://${warehouseBucket.bucketName}/warehouse/${appType}`,
      },
    });

    // Ensure database is created after bucket
    glueDatabase.node.addDependency(warehouseBucket);

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

    // Grant permissions
    warehouseBucket.grantReadWrite(flinkRole);
    eventStream.grantRead(flinkRole);
    
    // CRITICAL: S3 permissions for the specific CDK assets bucket
    // Cannot use wildcards in the middle of ARN, must be exact bucket name
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

    // Grant Glue permissions for Iceberg catalog
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

    // // Build runtime properties based on app type and maintenance setting
    const runtimeProperties = this.buildRuntimeProperties(appType, enableMaintenance, {
      kinesisStreamArn: eventStream.streamArn,
      warehousePath: `s3://${warehouseBucket.bucketName}/warehouse`,
      region: this.region,
      dbEndpoint: database?.dbInstanceEndpointAddress,
      dbSecretArn: dbSecret?.secretArn,
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
    flinkApp.node.addDependency(warehouseBucket);
    flinkApp.node.addDependency(eventStream);
    flinkApp.node.addDependency(glueDatabase);
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

    new cdk.CfnOutput(this, 'KinesisStreamName', {
      value: eventStream.streamName,
      description: 'Kinesis stream name for events',
    });

    new cdk.CfnOutput(this, 'WarehouseBucket', {
      value: warehouseBucket.bucketName,
      description: 'S3 bucket for Iceberg warehouse',
    });

    new cdk.CfnOutput(this, 'GlueDatabaseName', {
      value: glueDatabaseName,
      description: 'Glue database for Iceberg tables',
    });

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
    resources: {
      kinesisStreamArn: string;
      warehousePath: string;
      region: string;
      dbEndpoint?: string;
      dbSecretArn?: string;
    }
  ): { [key: string]: string } {
    const baseProps: { [key: string]: string } = {
      'aws.region': resources.region,
      'iceberg.warehouse': resources.warehousePath,
      'iceberg.catalog.name': 'glue_catalog',
      'iceberg.database': `iceberg_${appType}`,
      'checkpoint.interval.ms': '60000',
    };

    if (appType === 'datastream') {
      const props = {
        ...baseProps,
        'kinesis.stream.arn': resources.kinesisStreamArn,
        'kinesis.region': resources.region,
        'iceberg.table': 'orders',
        'enable.maintenance': enableMaintenance.toString(),
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
      return {
        ...baseProps,
        'kinesis.stream.name': `iceberg-events-${appType}`,
        's3.warehouse.path': resources.warehousePath,  // SQL job expects this property name
        'glue.database': `iceberg_${appType}`,  // SQL job expects this property name
        'table.prefix': 'sql_',
      };
    } else {
      // dynamic
      return {
        ...baseProps,
        'kinesis.stream.arn': resources.kinesisStreamArn,
        'kinesis.region': resources.region,
      };
    }
  }
}
