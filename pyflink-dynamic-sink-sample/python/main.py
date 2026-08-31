#!/usr/bin/env python3
"""PyFlink job driving Iceberg's DynamicIcebergSink through one compiled-Java
routing generator (com.aws.samples.iceberg.pyflink.FixedSchemaRoutingGenerator).

Two modes:

  MSF mode (default) -- runs on Amazon Managed Service for Apache Flink:
    * reads the FlinkApplicationProperties group from the MSF runtime
    * builds the Java KinesisStreamsSource (SimpleStringSchema -> String) via Py4J
    * wraps it into a Python DataStream (env.from_source)
    * builds a Glue CatalogLoader via Py4J, mirroring shared-common IcebergConfig
    * appends the DynamicIcebergSink with the Java generator (allowlist from props)
    There are NO Python map/transform operators in MSF mode: the record path is
    Java source -> Java sink, so no Python worker processes are spawned.

  LOCAL mode (--local) -- runs on a laptop against a HadoopCatalog at /tmp:
    * from_collection of sample JSON strings (mirrors the standalone prototype)
    * same Java generator + DynamicIcebergSink chain over a file:// warehouse

The schema is fixed and JSON-only; there is no Glue Schema Registry anywhere.

MSF packaging contract (see README): the application zip on S3 contains this
main.py plus the module fat jar; the run options group
`kinesis.analytics.flink.run.options` sets python=main.py and
jarfile=<jar path inside the zip>. On MSF the jarfile option places the fat jar
on the PyFlink gateway classpath, so the Java generator/source/sink classes
resolve without any local symlink into pyflink/lib.
"""
import os
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.typeinfo import Types
import pyflink.java_gateway


# ---- Runtime property keys (mirror the CDK pyflink-dynamic runtime properties) ----
KEY_STREAM_ARN = "kinesis.stream.arn"
KEY_KINESIS_REGION = "kinesis.region"
KEY_AWS_REGION = "aws.region"
KEY_CATALOG_NAME = "iceberg.catalog.name"
KEY_DATABASE = "iceberg.database"
KEY_WAREHOUSE = "iceberg.warehouse"
KEY_ALLOWLIST = "routing.allowlist"
KEY_TABLE_SUFFIX = "routing.table.suffix"

DEFAULT_CATALOG_NAME = "glue_catalog"
DEFAULT_DATABASE = "iceberg_pyflink_dynamic"
DEFAULT_AWS_REGION = "us-east-1"
DEFAULT_ALLOWLIST = "order,user,click"
DEFAULT_TABLE_SUFFIX = "_events"

GENERATOR_CLASS = "com.aws.samples.iceberg.pyflink.FixedSchemaRoutingGenerator"

# Kinesis source defaults (mirror shared-common KinesisSources.defaultSourceConfig).
_SRC_INIT_POSITION = ("flink.stream.initpos", "LATEST")
_SRC_SHARD_DISCOVERY_MS = ("flink.shard.discovery.intervalmillis", "10000")
_SRC_GET_RECORDS_MAX = ("flink.shard.getrecords.maxrecordcount", "10000")

# Local-mode warehouse and sample data.
LOCAL_WAREHOUSE = "/tmp/pyflink-dynamic-sink-warehouse"
LOCAL_DB = "db"
LOCAL_EVENTS = [
    '{"event_id":"ORDER-1","event_type":"ORDER","event_time":"2026-08-31T10:00:00.000Z","region":"us-east-1","event_date":"2026-08-31","amount":100}',
    '{"event_id":"CLICK-1","event_type":"CLICK","event_time":"2026-08-31T10:00:01.000Z","region":"eu-west-1","event_date":"2026-08-31"}',
    '{"event_id":"USER-1","event_type":"USER","event_time":"2026-08-31T10:00:02.000Z","region":"us-west-2","event_date":"2026-08-31"}',
    '{"event_id":"ORDER-2","event_type":"ORDER","event_time":"2026-08-31T10:00:03.000Z","region":"us-east-1","event_date":"2026-08-31","amount":250}',
    '{"event_id":"REFUND-1","event_type":"REFUND","event_time":"2026-08-31T10:00:04.000Z","region":"us-east-1","event_date":"2026-08-31"}',
]
# REFUND is not on the default allowlist, so it must be dropped.
LOCAL_EXPECTED = {"order_events": 2, "click_events": 1, "user_events": 1}


def _load_msf_properties():
    """Read the FlinkApplicationProperties group from the MSF Python runtime.

    Managed Service for Apache Flink exposes runtime properties to Python
    applications as a JSON file at a fixed path (see "Use Python with Managed
    Service for Apache Flink" in the MSF documentation). An environment
    variable override is supported for testing.
    """
    config_path = os.environ.get(
        "APPLICATION_PROPERTIES_FILE_PATH", "/etc/flink/application_properties.json")
    props = {}
    if not os.path.exists(config_path):
        return props
    import json
    with open(config_path) as fh:
        groups = json.load(fh)
    for group in groups:
        if group.get("PropertyGroupId") == "FlinkApplicationProperties":
            props.update(group.get("PropertyMap", {}))
    return props


def _build_kinesis_source(jvm, stream_arn, region):
    """Build a Java KinesisStreamsSource<String> via Py4J (SimpleStringSchema)."""
    source_config = jvm.org.apache.flink.configuration.Configuration()
    source_config.setString(KEY_AWS_REGION, region)
    for key, val in (_SRC_INIT_POSITION, _SRC_SHARD_DISCOVERY_MS, _SRC_GET_RECORDS_MAX):
        source_config.setString(key, val)

    j_schema = jvm.org.apache.flink.api.common.serialization.SimpleStringSchema()
    builder = jvm.org.apache.flink.connector.kinesis.source.KinesisStreamsSource.builder()
    builder.setStreamArn(stream_arn)
    builder.setDeserializationSchema(j_schema)
    builder.setSourceConfig(source_config)
    return builder.build()


def _glue_catalog_loader(jvm, catalog_name, warehouse, region):
    """Build a Glue CatalogLoader via the Java facade (relocation-safe)."""
    return jvm.com.aws.samples.iceberg.pyflink.PyFlinkSupport.glueCatalogLoader(
        catalog_name, warehouse, region)


def _hadoop_catalog_loader(jvm, warehouse):
    """Build a HadoopCatalog CatalogLoader via the Java facade (local mode)."""
    return jvm.com.aws.samples.iceberg.pyflink.PyFlinkSupport.hadoopCatalogLoader(warehouse)


def _append_dynamic_sink(jvm, j_data_stream, generator, catalog_loader):
    """Wire the DynamicIcebergSink builder chain onto the Java DataStream."""
    (jvm.org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink
        .forInput(j_data_stream)
        .generator(generator)
        .catalogLoader(catalog_loader)
        .immediateTableUpdate(True)
        .set("write.format.default", "parquet")
        .append())


def run_msf():
    props = _load_msf_properties()
    stream_arn = props.get(KEY_STREAM_ARN)
    if not stream_arn:
        raise ValueError(f"{KEY_STREAM_ARN} is required in FlinkApplicationProperties")
    region = props.get(KEY_KINESIS_REGION, props.get(KEY_AWS_REGION, DEFAULT_AWS_REGION))
    catalog_name = props.get(KEY_CATALOG_NAME, DEFAULT_CATALOG_NAME)
    database = props.get(KEY_DATABASE, DEFAULT_DATABASE)
    warehouse = props.get(KEY_WAREHOUSE)
    if not warehouse:
        raise ValueError(f"{KEY_WAREHOUSE} is required for the Glue catalog")
    allowlist = props.get(KEY_ALLOWLIST, DEFAULT_ALLOWLIST)
    table_suffix = props.get(KEY_TABLE_SUFFIX, DEFAULT_TABLE_SUFFIX)

    env = StreamExecutionEnvironment.get_execution_environment()
    gateway = pyflink.java_gateway.get_gateway()
    jvm = gateway.jvm

    source = _build_kinesis_source(jvm, stream_arn, region)
    watermark = jvm.org.apache.flink.api.common.eventtime.WatermarkStrategy.noWatermarks()
    string_type = jvm.org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO
    j_env = env._j_stream_execution_environment
    j_data_stream = j_env.fromSource(source, watermark, "Kinesis Source (JSON String)", string_type)

    generator = jvm.com.aws.samples.iceberg.pyflink.FixedSchemaRoutingGenerator(
        database, allowlist, table_suffix)
    catalog_loader = _glue_catalog_loader(jvm, catalog_name, warehouse, region)
    _append_dynamic_sink(jvm, j_data_stream, generator, catalog_loader)

    print(f"[pyflink-dynamic] MSF mode: stream={stream_arn} region={region} "
          f"db={database} allowlist={allowlist} suffix={table_suffix} generator={GENERATOR_CLASS}")
    env.execute("pyflink-dynamic-iceberg-sink")


def run_local():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    stream = env.from_collection(LOCAL_EVENTS, type_info=Types.STRING())
    j_data_stream = stream._j_data_stream

    gateway = pyflink.java_gateway.get_gateway()
    jvm = gateway.jvm

    generator = jvm.com.aws.samples.iceberg.pyflink.FixedSchemaRoutingGenerator(
        "db", DEFAULT_ALLOWLIST, DEFAULT_TABLE_SUFFIX)
    catalog_loader = _hadoop_catalog_loader(jvm, LOCAL_WAREHOUSE)
    _append_dynamic_sink(jvm, j_data_stream, generator, catalog_loader)

    print(f"[pyflink-dynamic] LOCAL mode: warehouse={LOCAL_WAREHOUSE} "
          f"allowlist={DEFAULT_ALLOWLIST} expected={LOCAL_EXPECTED}")
    env.execute("pyflink-dynamic-iceberg-sink-local")
    print("[pyflink-dynamic] execute() returned")

    support = jvm.com.aws.samples.iceberg.pyflink.PyFlinkSupport
    failures = 0
    for table, expected in LOCAL_EXPECTED.items():
        count = support.countRows(LOCAL_WAREHOUSE, LOCAL_DB, table)
        status = "OK" if count == expected else "MISMATCH"
        failures += 0 if count == expected else 1
        print(f"[verify] {LOCAL_DB}.{table}: rows={count} expected={expected} [{status}]")
    print("=== RESULT: SUCCESS ===" if failures == 0 else "=== RESULT: FAIL ===")
    if failures:
        sys.exit(1)


def main():
    if "--local" in sys.argv:
        run_local()
    else:
        run_msf()


if __name__ == "__main__":
    main()
