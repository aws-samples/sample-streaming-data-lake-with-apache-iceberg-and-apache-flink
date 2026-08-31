# PyFlink Dynamic Sink Sample

This module demonstrates driving Apache Iceberg's `DynamicIcebergSink` from a
**PyFlink** job. A PyFlink program builds the source, the catalog, and the sink
builder chain in Python (via Py4J), while exactly **one compiled Java class** —
`FixedSchemaRoutingGenerator` — performs the per-record routing. The pipeline is
**JSON only**; there is no Glue Schema Registry anywhere in this module.

## What it demonstrates

- PyFlink orchestrating a Java-only Iceberg sink: `env`, source wiring, catalog
  construction, and the `DynamicIcebergSink.forInput(...).generator(...).append()`
  chain are all Python calls over Py4J on the Java `DataStream`.
- A single compiled-Java `DynamicRecordGenerator<String>` that parses each JSON
  string against a **fixed** Iceberg schema and routes it to
  `<database>.<event_type>_events`, dropping any event type not on a
  configurable allowlist.
- The same fixed schema the shared `data-generator` module already emits, so the
  existing generator feeds this job unchanged.

### Why the generator must be Java

`DynamicRecordGenerator.generate(...)` runs per record **inside the sink's Java
operator** on Flink task threads. A Py4J Python proxy cannot be serialized into
the Flink job graph, so the routing logic is a compiled Java class. Everything
else — sources, catalog wiring, the sink builder chain — stays Python.

## Fixed schema

Aligned to the common fields every `data-generator` event carries (`BaseEvent`):

| Column | Type | Notes |
|--------|------|-------|
| `event_id` | string | required |
| `event_type` | string | required; drives routing (allowlist, case-insensitive) |
| `event_time` | string | optional (ISO-8601 as emitted) |
| `region` | string | optional |
| `event_date` | string | optional (`yyyy-MM-dd` as emitted) |

Table naming: `event_type.toLowerCase() + routing.table.suffix`
(e.g. `ORDER` → `order_events`). The default allowlist is `order,user,click`, so
the generator routes `ORDER`/`USER`/`CLICK` events and drops everything else.

## Configuration

| Property (`FlinkApplicationProperties`) | Default | Description |
|---|---|---|
| `kinesis.stream.arn` | — | Source Kinesis stream ARN (required) |
| `kinesis.region` / `aws.region` | `us-east-1` | AWS region |
| `iceberg.catalog.name` | `glue_catalog` | Catalog name |
| `iceberg.database` | `iceberg_pyflink_dynamic` | Target namespace |
| `iceberg.warehouse` | — | S3 warehouse path (required for Glue) |
| `routing.allowlist` | `order,user,click` | Comma-separated event-type allowlist |
| `routing.table.suffix` | `_events` | Suffix for generated table names |

## Local run

Uses `from_collection` sample events and a `HadoopCatalog` at
`/tmp/pyflink-dynamic-sink-warehouse` (no AWS needed):

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn -q package -DskipTests -pl pyflink-dynamic-sink-sample -am   # builds the fat jar
python python/main.py --local
```

The compiled Java classes (generator, Iceberg dynamic sink) resolve on the
PyFlink **gateway** classpath. `env.add_jars()` alone does not extend that
classpath, so for a local run make the fat jar
(`target/pyflink-dynamic-sink-sample.jar`) visible to the gateway JVM — e.g. by
placing it on the venv's `pyflink/lib/` directory. On MSF this is handled by the
`jarfile` run option (below), with no local step.

## MSF deploy via CDK

```bash
cd cdk-infrastructure
npx cdk deploy -c appType=pyflink-dynamic -c stackSuffix=pyf23
```

The `pyflink-dynamic` app type provisions an MSF **Python** application
(`FLINK-2_3`, `STREAMING`) with a source Kinesis stream, a Glue catalog + S3
warehouse, and IAM wiring identical to the `dynamic` app type.

## MSF Python packaging contract

The application code uploaded to S3 is a **zip** containing:

```
main.py                            # the PyFlink entry point
pyflink-dynamic-sink-sample.jar    # the module fat jar (generator + Iceberg + Kinesis)
```

and two runtime property groups:

- `FlinkApplicationProperties` — the job configuration above.
- `kinesis.analytics.flink.run.options` — `python=main.py` and
  `jarfile=pyflink-dynamic-sink-sample.jar`. The `jarfile` option puts the fat
  jar on the PyFlink gateway classpath, so the Java generator, Iceberg dynamic
  sink, and Kinesis source classes resolve at runtime.

The CDK stack assembles this zip from `python/main.py` and the built fat jar as a
single S3 asset (`ZIPFILE` code content type).

## Python version note

- **Local:** run local jobs on Python 3.12 with `apache-flink==2.3.0` (matches
  this module's Flink version and the MSF runtime). apache-beam ships no arm64
  macOS wheels, so on Apple Silicon install with
  `pip install 'setuptools==79.*' wheel cython grpcio-tools numpy` first, then
  `pip install --no-build-isolation apache-flink==2.3.0`.
- **MSF:** the MSF 2.x Python runtime is Python 3.12; the `main.py` here targets
  that runtime.

## Variant limitation

`DynamicIcebergSink` rejects any schema containing a `VARIANT` column
(`UnsupportedOperationException: Unsupported type: variant`) on Iceberg 1.11.0,
because the dynamic-sink schema visitors lack a `variant()` case. This module's
fixed schema is primitives only and is unaffected. The variant gap is fixed
upstream in [apache/iceberg PR #17900](https://github.com/apache/iceberg/pull/17900)
(targets Iceberg 1.12).

## Troubleshooting MSF Python deployments

Failures encountered while bringing this sample up on Managed Service for
Apache Flink, with their fixes — they apply to any PyFlink application that
ships a dependency jar:

| Symptom | Cause | Fix in this module |
|---|---|---|
| `LinkageError: loader constraint violation ... org.apache.commons.cli.Options` at start | The `jarfile` from `kinesis.analytics.flink.run.options` loads on the Python driver's parent classpath, where Flink's own commons-cli is already loaded; the fat jar carried an unrelocated copy (hadoop-common transitive) | `org.apache.commons.cli` is shade-relocated in this module's pom |
| `Python process exits with code: 1` with `ValueError: kinesis.stream.arn is required` | Runtime properties were read from an environment variable that MSF does not set | `main.py` reads MSF's fixed path `/etc/flink/application_properties.json` |
| `NoSuchMethodError: org.apache.iceberg.util.ThreadPools.newFixedThreadPool` | A transitive dependency bundled an older iceberg-core whose classes won the shade merge over `iceberg-flink-runtime` | `s3-tables-catalog-for-iceberg` is excluded from `shared-common` in this module |

| Job restart-loops at startup with `Ref main already exists` (escalated as `Partial recovery is not supported` by the Kinesis source) | Passing an explicit `"main"` branch in `DynamicRecord` makes parallel subtasks race to create the ref on freshly created tables | The generator passes a `null` branch (default main) |

Related: writing to a table with an Iceberg `variant` column through the
dynamic sink fails with `Unsupported type: variant` on Iceberg 1.11 — fix
pending upstream in [apache/iceberg#17900](https://github.com/apache/iceberg/pull/17900).
