package com.aws.samples.iceberg.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Shared checkpointing defaults for the samples. Only invoked when running locally —
 * AWS Managed Flink configures checkpointing on behalf of the application.
 */
public final class Checkpointing {

    private static final Logger LOG = LoggerFactory.getLogger(Checkpointing.class);

    public static final long DEFAULT_INTERVAL_MS = 60_000L;
    private static final long MIN_PAUSE_BETWEEN_CHECKPOINTS_MS = 30_000L;
    private static final long CHECKPOINT_TIMEOUT_MS = 600_000L;
    private static final int MAX_CONCURRENT_CHECKPOINTS = 1;
    private static final int TOLERABLE_FAILURES = 3;

    private Checkpointing() {}

    /**
     * Configure exactly-once checkpointing with production-friendly defaults.
     */
    public static void configureLocalDefaults(StreamExecutionEnvironment env, long intervalMs) {
        env.enableCheckpointing(intervalMs, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cp = env.getCheckpointConfig();
        cp.setMinPauseBetweenCheckpoints(MIN_PAUSE_BETWEEN_CHECKPOINTS_MS);
        cp.setCheckpointTimeout(CHECKPOINT_TIMEOUT_MS);
        cp.setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS);
        cp.setTolerableCheckpointFailureNumber(TOLERABLE_FAILURES);

        // A transient task/checkpoint failure should retry, not kill the job. (Managed Flink
        // provides its own restart strategy; this only applies to local runs.)
        Configuration restart = new Configuration();
        restart.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        restart.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
        restart.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10));
        env.configure(restart);

        LOG.info("Local checkpointing configured: interval={}ms, mode=EXACTLY_ONCE", intervalMs);
    }
}
