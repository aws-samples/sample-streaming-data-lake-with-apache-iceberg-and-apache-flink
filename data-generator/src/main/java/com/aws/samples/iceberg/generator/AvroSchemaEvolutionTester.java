package com.aws.samples.iceberg.generator;

import com.amazonaws.services.schemaregistry.common.AWSSerializerInput;
import com.amazonaws.services.schemaregistry.serializers.GlueSchemaRegistrySerializationFacade;
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.model.DataFormat;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Sends a batch of OrderEvent records using a V2 schema (adds payment_method and
 * promo_code fields). Verifies schema evolution through GSR -> the dynamic sink
 * should add the new columns to the Iceberg table automatically.
 *
 * Usage:
 *   java -cp data-generator.jar com.aws.samples.iceberg.generator.AvroSchemaEvolutionTester <stream> <region> <registry> [count]
 */
public class AvroSchemaEvolutionTester {

    private static final Logger LOG = LoggerFactory.getLogger(AvroSchemaEvolutionTester.class);

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: AvroSchemaEvolutionTester <stream> <region> <registry> [count]");
            System.exit(1);
        }
        String streamName = args[0];
        String regionStr = args[1];
        String registryName = args[2];
        int count = args.length > 3 ? Integer.parseInt(args[3]) : 100;

        // Load V2 schema (adds payment_method and promo_code)
        Schema v2Schema;
        try (java.io.InputStream in = AvroSchemaEvolutionTester.class.getResourceAsStream("/avro/OrderEventV2.avsc")) {
            v2Schema = new Schema.Parser().parse(in);
        }
        LOG.info("Loaded V2 schema: {}", v2Schema.getFullName());

        Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.AWS_REGION, regionStr);
        configs.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true);
        configs.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, "BACKWARD");
        configs.put(AWSSchemaRegistryConstants.REGISTRY_NAME, registryName);

        GlueSchemaRegistrySerializationFacade facade = GlueSchemaRegistrySerializationFacade.builder()
                .credentialProvider(DefaultCredentialsProvider.create())
                .configs(configs)
                .build();

        // Register V2 schema version (BACKWARD compatible because new fields are nullable with defaults)
        AWSSerializerInput input = AWSSerializerInput.builder()
                .schemaDefinition(v2Schema.toString())
                .schemaName("OrderEvent")
                .dataFormat(DataFormat.AVRO.name())
                .transportName(streamName)
                .build();
        UUID versionId = facade.getOrRegisterSchemaVersion(input);
        LOG.info("Registered OrderEvent V2 schema version: {}", versionId);

        KinesisAsyncClient kinesis = KinesisAsyncClient.builder().region(Region.of(regionStr)).build();

        String[] paymentMethods = {"credit_card", "debit_card", "paypal", "apple_pay"};
        String[] promoCodes = {"SAVE10", "NEWUSER", "SUMMER25", null, null};

        java.util.List<PutRecordsRequestEntry> batch = new java.util.ArrayList<>();
        java.util.Random random = new java.util.Random();

        for (int i = 0; i < count; i++) {
            GenericRecord r = new GenericData.Record(v2Schema);
            r.put("event_id", "order-v2-" + UUID.randomUUID());
            r.put("event_time", Instant.now().toEpochMilli() * 1000L);
            r.put("event_type", "ORDER");
            r.put("region", new String[]{"us-east-1", "us-west-2", "eu-west-1"}[random.nextInt(3)]);
            r.put("event_date", (int) LocalDate.now().toEpochDay());
            r.put("order_id", "ORD-V2-" + i);
            r.put("customer_id", "CUST-" + random.nextInt(1000));
            r.put("amount", 100.0 + random.nextDouble() * 500);
            r.put("currency", "USD");
            r.put("status", "COMPLETED");
            r.put("payment_method", paymentMethods[random.nextInt(paymentMethods.length)]);
            r.put("promo_code", promoCodes[random.nextInt(promoCodes.length)]);

            byte[] payload = facade.serialize(DataFormat.AVRO, r, versionId);

            batch.add(PutRecordsRequestEntry.builder()
                    .partitionKey(r.get("event_id").toString())
                    .data(SdkBytes.fromByteArray(payload))
                    .build());

            if (batch.size() >= 500) {
                flush(kinesis, streamName, batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) flush(kinesis, streamName, batch);

        LOG.info("Sent {} V2 OrderEvent records", count);
        kinesis.close();
    }

    private static void flush(KinesisAsyncClient kinesis, String stream, List<PutRecordsRequestEntry> batch) {
        PutRecordsRequest req = PutRecordsRequest.builder().streamName(stream).records(batch).build();
        kinesis.putRecords(req).join();
    }
}
