import * as cdk from 'aws-cdk-lib';
import * as kinesis from 'aws-cdk-lib/aws-kinesis';
import { Construct } from 'constructs';

export interface KinesisStreamsProps {
  appType: string;
  needsSourceStream: boolean;
  needsSinkStream: boolean;
}

export class KinesisStreams extends Construct {
  public readonly sourceStream?: kinesis.Stream;
  public readonly sinkStream?: kinesis.Stream;

  constructor(scope: Construct, id: string, props: KinesisStreamsProps) {
    super(scope, id);

    if (props.needsSourceStream) {
      this.sourceStream = new kinesis.Stream(this, 'EventStream', {
        streamName: `iceberg-events-${props.appType}`,
        shardCount: 2,
        retentionPeriod: cdk.Duration.hours(24),
        streamMode: kinesis.StreamMode.PROVISIONED,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      const cfnStream = this.sourceStream.node.defaultChild as kinesis.CfnStream;
      cfnStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    }

    if (props.needsSinkStream) {
      this.sinkStream = new kinesis.Stream(this, 'SinkStream', {
        streamName: `iceberg-output-${props.appType}`,
        shardCount: 2,
        retentionPeriod: cdk.Duration.hours(24),
        streamMode: kinesis.StreamMode.PROVISIONED,
        removalPolicy: cdk.RemovalPolicy.DESTROY,
      });
      const cfnSinkStream = this.sinkStream.node.defaultChild as kinesis.CfnStream;
      cfnSinkStream.applyRemovalPolicy(cdk.RemovalPolicy.DESTROY);
    }
  }
}
