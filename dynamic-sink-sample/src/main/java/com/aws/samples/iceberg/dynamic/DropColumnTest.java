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
 * Drop-COLUMN test (Iceberg 1.11.0 DynamicIcebergSink dropUnusedColumns feature).
 * Record 1 carries loyalty_tier; record 2 omits it. With dropUnusedColumns(true) the sink
 * must call UpdateSchema.deleteColumn and remove loyalty_tier from {db}.dropcol_events.
 * Config via -D: iceberg.database, iceberg.warehouse, aws.region.
 */
public class DropColumnTest {
    public static void main(String[] args) throws Exception {
        Properties p = System.getProperties();
        String db = p.getProperty("iceberg.database");
        ObjectMapper om = new ObjectMapper();

        String r1 = "{\"event_type\":\"dropcol\",\"event_id\":\"e1\",\"event_date\":\"2026-05-29\",\"region\":\"us-east-1\",\"amount\":10,\"loyalty_tier\":\"gold\"}";
        String r2 = "{\"event_type\":\"dropcol\",\"event_id\":\"e2\",\"event_date\":\"2026-05-29\",\"region\":\"us-east-1\",\"amount\":20}";
        // adds signup_source, omits loyalty_tier -> forces SCHEMA_UPDATE_NEEDED (add) so dropUnusedColumns can prune loyalty_tier
        String r3 = "{\"event_type\":\"dropcol\",\"event_id\":\"e3\",\"event_date\":\"2026-05-29\",\"region\":\"us-east-1\",\"amount\":30,\"signup_source\":\"web\"}";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String mode = p.getProperty("mode", "both"); // both | reduced | addomit
        DataStream<JsonNode> events = (mode.equals("reduced") ? env.fromElements(r2)
                : mode.equals("addomit") ? env.fromElements(r3)
                : env.fromElements(r1, r2))
                .map((org.apache.flink.api.common.functions.MapFunction<String, JsonNode>) om::readTree)
                .returns(TypeInformation.of(JsonNode.class));

        SchemaAgnosticRoutingGenerator gen = new SchemaAgnosticRoutingGenerator(
                db, "event_type", null, "_events", Arrays.asList("event_date", "region"));

        DynamicIcebergSink.forInput(events)
                .generator(gen)
                .catalogLoader(IcebergConfig.createCatalogLoader("glue", p))
                .immediateTableUpdate(true)
                .dropUnusedColumns(true)
                .cacheMaxSize(100)
                .cacheRefreshMs(60000)
                .set("format-version", "3")
                .append();

        env.execute("drop-column-test");
        System.out.println("DROP_COLUMN_DONE table=" + db + ".dropcol_events");
    }
}
