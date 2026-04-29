package com.aws.samples.iceberg.runtime;

import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        LOG.info("Local checkpointing configured: interval={}ms, mode=EXACTLY_ONCE", intervalMs);
    }
}
