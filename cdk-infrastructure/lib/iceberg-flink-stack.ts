import * as cdk from 'aws-cdk-lib';
import * as s3assets from 'aws-cdk-lib/aws-s3-assets';
import * as kinesisanalytics from 'aws-cdk-lib/aws-kinesisanalyticsv2';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { KinesisStreams } from './constructs/kinesis-streams';
import { CatalogResources } from './constructs/catalog-resources';
import { MaintenanceResources } from './constructs/maintenance-resources';
import { FlinkIam } from './constructs/flink-iam';

export interface IcebergFlinkStackProps extends cdk.StackProps {
  appType: 'datastream' | 'sql' | 'dynamic' | 'dynamic-avro' | 'iceberg-source' | 'iceberg-source-sql' | 'hybrid';
  enableMaintenance: boolean;
  catalogType?: 'glue' | 's3tables';
  // Source-app overrides: when set, source apps (iceberg-source, iceberg-source-sql,
  // hybrid) will read from this existing Iceberg table instead of the auto-created one.
  sourceDatabase?: string;
  sourceTable?: string;
  sourceWarehouse?: string;        // Required with sourceDatabase for Glue catalog
  sourceTableBucketArn?: string;   // Required with sourceDatabase for S3 Tables
}

// App configuration mapping
const APP_CONFIG = {
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
  'dynamic-avro': {
    modulePath: '../dynamic-sink-avro-sample',
    jarName: 'dynamic-sink-avro-sample-1.0-SNAPSHOT.jar',
    mainClass: 'com.aws.samples.iceberg.dynamic.avro.DynamicAvroSinkJob',
    description: 'Dynamic Iceberg Sink driven by AWS Glue Schema Registry (Avro)',
    needsSourceStream: true,
    needsSinkStream: false,
  },
  'iceberg-source': {
    modulePath: '../iceberg-source-datastream-sample',
    jarName: 'iceberg-source-datastream-1.0-SNAPSHOT.jar',
    mainClass: 'com.aws.samples.iceberg.source.IcebergSourceJob',
    description: 'Read from Iceberg tables using DataStream API, write to Kinesis',
    needsSourceStream: false,
    needsSinkStream: true,
  },
  'iceberg-source-sql': {
    modulePath: '../iceberg-source-sql-sample',
    jarName: 'iceberg-source-sql-1.0-SNAPSHOT.jar',
    mainClass: 'com.aws.samples.iceberg.source.sql.IcebergSourceSqlJob',
    description: 'Read from Iceberg tables using SQL API, write to Kinesis',
    needsSourceStream: false,
    needsSinkStream: true,
  },
  hybrid: {
    modulePath: '../hybrid-source-sample',
    jarName: 'hybrid-source-sample-1.0-SNAPSHOT.jar',
    mainClass: 'com.aws.samples.iceberg.hybrid.HybridSourceJob',
    description: 'Bootstrap from Iceberg then switch to Kinesis streaming',
    needsSourceStream: true,
    needsSinkStream: true,
  },
};

export class IcebergFlinkStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: IcebergFlinkStackProps) {
    super(scope, id, props);

    const { appType, enableMaintenance, catalogType = 'glue' } = props;

    if (catalogType === 's3tables' && enableMaintenance) {
      throw new Error('S3 Tables handles maintenance automatically. Do not enable maintenance when using S3 Tables catalog.');
    }

    const config = APP_CONFIG[appType];
    const databaseName = `iceberg_${appType.replace(/-/g, '_')}`;
    const cdkBootstrapQualifier = this.node.tryGetContext('cdkBootstrapQualifier') || 'hnb659fds';

    // --- Kinesis Streams ---
    const streams = new KinesisStreams(this, 'Streams', {
      appType,
      needsSourceStream: config.needsSourceStream,
      needsSinkStream: config.needsSinkStream,
    });

    // --- Glue Schema Registry (dynamic-avro only) ---
    let schemaRegistry: cdk.aws_glue.CfnRegistry | undefined;
    if (appType === 'dynamic-avro') {
      schemaRegistry = new cdk.aws_glue.CfnRegistry(this, 'SchemaRegistry', {
        name: `iceberg-${appType}`,
        description: 'Schema registry for Avro-encoded events consumed by the Flink dynamic sink sample',
      });
      schemaRegistry.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    }

    // --- Catalog Resources (Glue or S3 Tables) ---
    // If source overrides are provided, skip creating a local catalog (source apps
    // will read from an external Iceberg table).
    const hasSourceOverride = !!(props.sourceDatabase || props.sourceWarehouse || props.sourceTableBucketArn);
    const catalog = new CatalogResources(this, 'Catalog', {
      appType,
      catalogType,
      databaseName,
      account: this.account,
      region: this.region,
      skipCatalogCreation: hasSourceOverride,
    });

    // --- Maintenance Resources (VPC + RDS, datastream only) ---
    let maintenance: MaintenanceResources | undefined;
    if (appType === 'datastream' && enableMaintenance) {
      maintenance = new MaintenanceResources(this, 'Maintenance', {});
    }

    // --- CloudWatch Logs ---
    const logGroup = new logs.LogGroup(this, 'FlinkLogGroup', {
      logGroupName: `/aws/kinesisanalytics/iceberg-flink-${appType}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const logStream = new logs.LogStream(this, 'FlinkLogStream', {
      logGroup,
      logStreamName: 'kinesis-analytics-log-stream',
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // --- IAM Role ---
    const flinkIam = new FlinkIam(this, 'Iam', {
      appType,
      catalogType,
      databaseName,
      logGroup,
      sourceStream: streams.sourceStream,
      sinkStream: streams.sinkStream,
      warehouseBucket: catalog.warehouseBucket,
      s3TableBucketName: catalog.s3TableBucketName,
      cdkBootstrapQualifier,
      account: this.account,
      region: this.region,
      sourceWarehouse: props.sourceWarehouse,
      sourceTableBucketArn: props.sourceTableBucketArn,
      sourceDatabase: props.sourceDatabase,
      enableMaintenance: appType === 'datastream' && enableMaintenance,
      vpc: maintenance?.vpc,
      dbSecret: maintenance?.dbSecret,
      schemaRegistryName: schemaRegistry?.name,
    });

    // --- Flink JAR Asset ---
    const flinkJarAsset = new s3assets.Asset(this, 'FlinkJarAsset', {
      path: '..',
      exclude: ['cdk-infrastructure', 'cdk.out', '.git', '.idea', '.kiro', 'target', 'node_modules'],
      bundling: {
        image: cdk.DockerImage.fromRegistry('maven:3.9-eclipse-temurin-17'),
        command: [
          'bash', '-c',
          [
            'mkdir -p /tmp/.m2',
            'cp -r /asset-input/* /tmp/',
            'cd /tmp',
            `mvn clean package -DskipTests -pl ${config.modulePath.replace('../', '')} -am -Dmaven.repo.local=/tmp/.m2 -q`,
            'mkdir -p /asset-output',
            `cp /tmp/${config.modulePath.replace('../', '')}/target/${config.jarName} /asset-output/app.jar`,
          ].join(' && '),
        ],
        user: 'root',
        outputType: cdk.BundlingOutput.SINGLE_FILE,
      },
    });

    // --- Runtime Properties ---
    const runtimeProperties = this.buildRuntimeProperties(appType, enableMaintenance, catalogType, {
      kinesisSourceStreamArn: streams.sourceStream?.streamArn,
      kinesisSinkStreamArn: streams.sinkStream?.streamArn,
      kinesisSinkStreamName: streams.sinkStream?.streamName,
      warehousePath: catalog.warehouseBucket ? `s3://${catalog.warehouseBucket.bucketName}/warehouse` : '',
      region: this.region,
      dbEndpoint: maintenance?.database.dbInstanceEndpointAddress,
      dbSecretArn: maintenance?.dbSecret.secretArn,
      s3TableBucketArn: catalog.s3TableBucketArn,
      sourceDatabase: props.sourceDatabase,
      sourceTable: props.sourceTable,
      sourceWarehouse: props.sourceWarehouse,
      sourceTableBucketArn: props.sourceTableBucketArn,
    });

    // --- Flink Application ---
    const flinkApp = new kinesisanalytics.CfnApplication(this, 'FlinkApplication', {
      applicationName: `iceberg-flink-${appType}${enableMaintenance ? '-maintenance' : ''}`,
      runtimeEnvironment: 'FLINK-2_2',
      serviceExecutionRole: flinkIam.role.roleArn,
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
          propertyGroups: [{
            propertyGroupId: 'FlinkApplicationProperties',
            propertyMap: runtimeProperties,
          }],
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
        applicationSnapshotConfiguration: { snapshotsEnabled: true },
        vpcConfigurations: maintenance
          ? [{
              securityGroupIds: [maintenance.flinkSecurityGroup.securityGroupId],
              subnetIds: maintenance.vpc.privateSubnets.map((s) => s.subnetId),
            }]
          : undefined,
      },
      applicationDescription: config.description,
      applicationMode: 'STREAMING',
    });

    flinkApp.node.addDependency(flinkJarAsset);
    flinkApp.node.addDependency(flinkIam.role);
    if (streams.sourceStream) flinkApp.node.addDependency(streams.sourceStream);
    if (streams.sinkStream) flinkApp.node.addDependency(streams.sinkStream);
    if (catalog.warehouseBucket) flinkApp.node.addDependency(catalog.warehouseBucket);
    if (catalog.glueDatabase) flinkApp.node.addDependency(catalog.glueDatabase);
    if (schemaRegistry) flinkApp.node.addDependency(schemaRegistry);
    flinkApp.node.addDependency(logGroup);
    flinkApp.node.addDependency(logStream);

    // CloudWatch logging
    const flinkLogging = new kinesisanalytics.CfnApplicationCloudWatchLoggingOption(this, 'FlinkLogging', {
      applicationName: flinkApp.ref,
      cloudWatchLoggingOption: {
        logStreamArn: `arn:aws:logs:${this.region}:${this.account}:log-group:${logGroup.logGroupName}:log-stream:kinesis-analytics-log-stream`,
      },
    });
    flinkLogging.addDependency(flinkApp);

    // --- Outputs ---
    new cdk.CfnOutput(this, 'ApplicationName', {
      value: flinkApp.applicationName!,
      description: 'Flink application name',
    });
    if (streams.sourceStream) {
      new cdk.CfnOutput(this, 'KinesisSourceStreamName', {
        value: streams.sourceStream.streamName,
        description: 'Kinesis stream name for source events',
      });
    }
    if (streams.sinkStream) {
      new cdk.CfnOutput(this, 'KinesisSinkStreamName', {
        value: streams.sinkStream.streamName,
        description: 'Kinesis stream name for sink output',
      });
      new cdk.CfnOutput(this, 'KinesisSinkStreamArn', {
        value: streams.sinkStream.streamArn,
        description: 'Kinesis stream ARN for sink output',
      });
    }
    if (catalog.warehouseBucket) {
      new cdk.CfnOutput(this, 'WarehouseBucket', {
        value: catalog.warehouseBucket.bucketName,
        description: 'S3 bucket for Iceberg warehouse',
      });
    }
    new cdk.CfnOutput(this, 'GlueDatabaseName', {
      value: databaseName,
      description: 'Database name for Iceberg tables',
    });
    new cdk.CfnOutput(this, 'CatalogType', {
      value: catalogType,
      description: 'Catalog type (glue or s3tables)',
    });
    if (catalog.s3TableBucketArn) {
      new cdk.CfnOutput(this, 'S3TableBucketArn', {
        value: catalog.s3TableBucketArn,
        description: 'S3 Table Bucket ARN for Iceberg tables',
      });
    }
    new cdk.CfnOutput(this, 'AppType', {
      value: appType,
      description: 'Deployed application type',
    });
    if (schemaRegistry) {
      new cdk.CfnOutput(this, 'SchemaRegistryName', {
        value: schemaRegistry.name,
        description: 'Glue Schema Registry name for Avro schemas',
      });
    }
    if (maintenance) {
      new cdk.CfnOutput(this, 'DatabaseEndpoint', {
        value: maintenance.database.dbInstanceEndpointAddress,
        description: 'RDS PostgreSQL endpoint for maintenance locks',
      });
      new cdk.CfnOutput(this, 'DatabaseSecretArn', {
        value: maintenance.dbSecret.secretArn,
        description: 'Secrets Manager ARN for database credentials',
      });
    }
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
      sourceDatabase?: string;
      sourceTable?: string;
      sourceWarehouse?: string;
      sourceTableBucketArn?: string;
    }
  ): { [key: string]: string } {
    const baseProps: { [key: string]: string } = {
      'aws.region': resources.region,
      'iceberg.catalog.name': catalogType === 's3tables' ? 's3tables_catalog' : 'glue_catalog',
      'iceberg.catalog.type': catalogType,
      'iceberg.database': resources.sourceDatabase || `iceberg_${appType.replace(/-/g, '_')}`,
      'checkpoint.interval.ms': '60000',
    };

    // For source apps, allow overriding warehouse to point at an existing Iceberg table
    const effectiveWarehouse = resources.sourceWarehouse || resources.warehousePath;
    const effectiveBucketArn = resources.sourceTableBucketArn || resources.s3TableBucketArn;

    if (catalogType === 'glue' && effectiveWarehouse) {
      baseProps['iceberg.warehouse'] = effectiveWarehouse;
    }
    if (catalogType === 's3tables' && effectiveBucketArn) {
      baseProps['s3tables.bucket.arn'] = effectiveBucketArn;
    }

    if (appType === 'datastream') {
      // write.mode can be overridden via context (default 'upsert')
      // Set to 'append' to produce data readable by iceberg-source streaming mode
      const writeMode = this.node.tryGetContext('writeMode') || 'upsert';
      const tableFormatVersion = this.node.tryGetContext('tableFormatVersion') || '2';
      const props = {
        ...baseProps,
        'kinesis.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.region': resources.region,
        'iceberg.table': 'orders',
        'enable.maintenance': enableMaintenance.toString(),
        'write.mode': writeMode,
        'primary.key.columns': 'event_id,event_date,region',
        'table.format.version': tableFormatVersion,
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
        'kinesis.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.region': resources.region,
        'glue.database': `iceberg_${appType.replace(/-/g, '_')}`,
        'table.prefix': 'sql_',
        'write.mode': 'append',
        'primary.key.columns': 'event_id,event_date,region',
      };
      if (catalogType === 'glue' && resources.warehousePath) {
        sqlProps['s3.warehouse.path'] = resources.warehousePath;
      }
      return sqlProps;
    } else if (appType === 'iceberg-source') {
      return {
        ...baseProps,
        'iceberg.table': resources.sourceTable || 'orders',
        'iceberg.source.streaming': 'true',
        'iceberg.source.starting-strategy': 'INCREMENTAL_FROM_LATEST_SNAPSHOT',
        'iceberg.source.monitor-interval': '60s',
        'kinesis.sink.stream.arn': resources.kinesisSinkStreamArn!,
      };
    } else if (appType === 'iceberg-source-sql') {
      return {
        ...baseProps,
        'iceberg.table': resources.sourceTable || 'orders',
        'iceberg.source.streaming': 'true',
        'iceberg.source.monitor-interval': '60s',
        'kinesis.sink.stream.arn': resources.kinesisSinkStreamArn!,
      };
    } else if (appType === 'hybrid') {
      return {
        ...baseProps,
        'iceberg.table': resources.sourceTable || 'orders',
        'kinesis.source.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.sink.stream.arn': resources.kinesisSinkStreamArn!,
      };
    } else if (appType === 'dynamic-avro') {
      return {
        ...baseProps,
        'kinesis.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.region': resources.region,
        'schema.registry.name': `iceberg-${appType}`,
        'partition.candidates': 'event_date,region',
      };
    } else {
      // dynamic
      return {
        ...baseProps,
        'kinesis.stream.arn': resources.kinesisSourceStreamArn!,
        'kinesis.region': resources.region,
        'write.mode': 'append',
        'primary.key.columns': 'event_id,event_date,region',
      };
    }
  }
}
