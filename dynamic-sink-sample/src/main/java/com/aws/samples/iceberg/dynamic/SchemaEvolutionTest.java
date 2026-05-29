package com.aws.samples.iceberg.dynamic;

import com.aws.samples.iceberg.config.IcebergConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;

import java.util.Arrays;
import java.util.Properties;

/**
 * Minimal LOCAL schema-evolution test for DynamicIcebergSink + SchemaAgnosticRoutingGenerator.
 * Record 1 has schema A; record 2 adds a NEW field (loyalty_tier). With immediateTableUpdate,
 * the sink must evolve table {db}.schemaevo_events to add the column live.
 * Config via -D: iceberg.database, iceberg.warehouse, aws.region.
 */
public class SchemaEvolutionTest {
    public static void main(String[] args) throws Exception {
        Properties p = System.getProperties();
        String db = p.getProperty("iceberg.database");
        ObjectMapper om = new ObjectMapper();

        String r1 = "{\"event_type\":\"schemaevo\",\"event_id\":\"e1\",\"event_date\":\"2026-05-29\",\"region\":\"us-east-1\",\"amount\":10}";
        String r2 = "{\"event_type\":\"schemaevo\",\"event_id\":\"e2\",\"event_date\":\"2026-05-29\",\"region\":\"us-east-1\",\"amount\":20,\"loyalty_tier\":\"gold\"}";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStream<JsonNode> events = env.fromElements(r1, r2)
                .map((org.apache.flink.api.common.functions.MapFunction<String, JsonNode>) om::readTree)
                .returns(TypeInformation.of(JsonNode.class));

        SchemaAgnosticRoutingGenerator gen = new SchemaAgnosticRoutingGenerator(
                db, "event_type", null, "_events", Arrays.asList("event_date", "region"));

        DynamicIcebergSink.forInput(events)
                .generator(gen)
                .catalogLoader(IcebergConfig.createCatalogLoader("glue", p))
                .immediateTableUpdate(true)
                .cacheMaxSize(100)
                .cacheRefreshMs(60000)
                .set("format-version", "3")
                .append();

        env.execute("schema-evolution-test");
        System.out.println("SCHEMA_EVO_DONE table=" + db + ".schemaevo_events");
    }
}
