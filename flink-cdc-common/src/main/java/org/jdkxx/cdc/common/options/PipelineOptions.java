package org.jdkxx.cdc.common.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PipelineOptions {
    public static final ConfigOption<Map<String, String>> PROPERTIES = ConfigOptions
            .key("pipeline.properties")
            .mapType()
            .noDefaultValue();

    public static final ConfigOption<Map<String, String>> GLOBAL_CONF = ConfigOptions
            .key("pipeline.global.conf")
            .mapType()
            .noDefaultValue();

    public static final ConfigOption<List<Map<String, String>>> ROUTES = ConfigOptions
            .key("pipeline.routes")
            .mapType()
            .asList()
            .noDefaultValue();

    public static final ConfigOption<Integer> MAX_PARALLELISM = ConfigOptions
            .key("pipeline.max.parallelism")
            .intType()
            .defaultValue(10);

    public static final ConfigOption<Integer> LOCAL_NUMBER_TASK_MANAGER = ConfigOptions
            .key("local.number-taskmanager")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Integer> CHECKPOINTS_INTERVAL = ConfigOptions
            .key("state.checkpoints.interval")
            .intType()
            .defaultValue(300000);

    public static final ConfigOption<Map<String, String>> PARALLELISM_OVERRIDES = ConfigOptions
            .key("pipeline.jobvertex-parallelism-overrides")
            .mapType()
            .defaultValue(Collections.emptyMap())
            .withDescription(
                    "A parallelism override map (jobVertexId -> parallelism) which will be used to update"
                            + " the parallelism of the corresponding job vertices of submitted JobGraphs.");
}
