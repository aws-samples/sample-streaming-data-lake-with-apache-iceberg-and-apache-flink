package com.aws.samples.iceberg.runtime;

import com.amazonaws.services.kinesisanalytics.runtime.KinesisAnalyticsRuntime;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Load the {@code FlinkApplicationProperties} property group from the runtime.
 *
 * <p>On AWS Managed Flink the property map is delivered via
 * {@link KinesisAnalyticsRuntime#getApplicationProperties()}. For local IDE runs we
 * fall back to a classpath-resident JSON resource (the same format the service consumes).
 */
public final class AppProperties {

    private static final Logger LOG = LoggerFactory.getLogger(AppProperties.class);
    private static final String DEFAULT_LOCAL_RESOURCE = "flink-application-properties-dev.json";
    private static final String FLINK_APPLICATION_PROPERTIES = "FlinkApplicationProperties";

    private AppProperties() {}

    /**
     * Load the {@code FlinkApplicationProperties} group as a {@code Map<String,String>}.
     * Preferred when the caller does not need the raw {@link Properties} type.
     */
    public static Map<String, String> loadAsMap(StreamExecutionEnvironment env) throws IOException {
        Properties props = load(env);
        Map<String, String> map = new HashMap<>();
        for (String key : props.stringPropertyNames()) {
            map.put(key, props.getProperty(key));
        }
        return map;
    }

    /**
     * Load the {@code FlinkApplicationProperties} group as {@link Properties}.
     */
    public static Properties load(StreamExecutionEnvironment env) throws IOException {
        return load(env, DEFAULT_LOCAL_RESOURCE);
    }

    /**
     * As {@link #load(StreamExecutionEnvironment)} but with a configurable local resource path.
     */
    public static Properties load(StreamExecutionEnvironment env, String localResourcePath) throws IOException {
        Map<String, Properties> allProps = loadAllGroups(env, localResourcePath);
        return allProps.getOrDefault(FLINK_APPLICATION_PROPERTIES, new Properties());
    }

    /**
     * Return every property group from the runtime or local resource — useful when a job
     * reads more than one property group (rare for our samples).
     */
    public static Map<String, Properties> loadAllGroups(StreamExecutionEnvironment env) throws IOException {
        return loadAllGroups(env, DEFAULT_LOCAL_RESOURCE);
    }

    private static Map<String, Properties> loadAllGroups(StreamExecutionEnvironment env,
                                                          String localResourcePath) throws IOException {
        if (FlinkEnvironments.isLocal(env)) {
            LOG.info("Loading application properties from local resource: {}", localResourcePath);
            return loadFromLocalResource(localResourcePath);
        }
        LOG.info("Loading application properties from Managed Flink runtime");
        Map<String, Properties> runtime = KinesisAnalyticsRuntime.getApplicationProperties();
        return runtime != null ? runtime : new HashMap<>();
    }

    private static Map<String, Properties> loadFromLocalResource(String resourcePath) throws IOException {
        URL resource = AppProperties.class.getClassLoader().getResource(resourcePath);
        if (resource == null) {
            LOG.warn("Local properties resource {} not found — using empty config", resourcePath);
            return new HashMap<>();
        }
        // KinesisAnalyticsRuntime provides a helper for the JSON format; use it when possible
        // so parsing stays consistent with the Managed Flink runtime.
        try {
            return KinesisAnalyticsRuntime.getApplicationProperties(resource.getPath());
        } catch (RuntimeException primary) {
            LOG.warn("KinesisAnalyticsRuntime could not parse {} — falling back to direct JSON read",
                    resourcePath, primary);
            return parseJsonGroups(resource);
        }
    }

    /**
     * Fallback parser for the Managed Flink properties JSON format. The runtime parser is
     * preferred; this path exists so the samples can still start when the runtime library
     * is missing or the resource URL scheme is unusual (e.g. shaded jars).
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Properties> parseJsonGroups(URL resource) throws IOException {
        Map<String, Properties> result = new HashMap<>();
        try (InputStream input = resource.openStream()) {
            Map<String, Object>[] groups = new ObjectMapper().readValue(input, Map[].class);
            for (Map<String, Object> group : groups) {
                String groupId = (String) group.get("PropertyGroupId");
                Map<String, String> propertyMap = (Map<String, String>) group.get("PropertyMap");
                Properties props = new Properties();
                if (propertyMap != null) {
                    props.putAll(propertyMap);
                }
                if (groupId != null) {
                    result.put(groupId, props);
                }
            }
        }
        return result;
    }
}
