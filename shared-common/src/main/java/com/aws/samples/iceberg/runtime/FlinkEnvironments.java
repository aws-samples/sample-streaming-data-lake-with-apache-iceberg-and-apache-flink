package com.aws.samples.iceberg.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for constructing a {@link StreamExecutionEnvironment} with or without the
 * Flink Web UI depending on whether the job is running locally or on AWS Managed Flink.
 *
 * <p>On Managed Flink the service creates the environment; {@link #getOrCreateLocal(int)}
 * only needs to take effect when running from an IDE.
 */
public final class FlinkEnvironments {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvironments.class);
    private static final String REST_BIND_ADDRESS_KEY = "rest.bind-address";
    private static final String REST_PORT_KEY = "rest.port";
    private static final String LOCALHOST = "localhost";

    private FlinkEnvironments() {}

    /**
     * @return {@code true} when running inside a Flink local cluster (IDE/tests).
     */
    public static boolean isLocal(StreamExecutionEnvironment env) {
        return env instanceof LocalStreamEnvironment;
    }

    /**
     * Return a {@link StreamExecutionEnvironment} appropriate for the current runtime.
     * When running locally, enable the Web UI on the supplied port so that multiple
     * samples can run in parallel from different IDE runs without port clashes.
     *
     * @param localWebUiPort port the local Web UI should bind to (e.g. 8081)
     */
    public static StreamExecutionEnvironment getOrCreateLocal(int localWebUiPort) {
        try {
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            if (isLocal(env)) {
                Configuration config = new Configuration();
                config.setString(REST_PORT_KEY, Integer.toString(localWebUiPort));
                config.setString(REST_BIND_ADDRESS_KEY, LOCALHOST);
                env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
                LOG.info("Local execution detected — Flink Web UI at http://{}:{}",
                        LOCALHOST, localWebUiPort);
            }
            return env;
        } catch (RuntimeException e) {
            LOG.warn("Could not create local environment with Web UI, falling back", e);
            return StreamExecutionEnvironment.getExecutionEnvironment();
        }
    }
}
