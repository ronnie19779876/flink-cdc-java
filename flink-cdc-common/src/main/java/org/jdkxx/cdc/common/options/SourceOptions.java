package org.jdkxx.cdc.common.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;
import java.util.Map;

public class SourceOptions {
    public static final ConfigOption<String> NAME = ConfigOptions
            .key("source.name")
            .stringType()
            .defaultValue("Mock StreamJob Source");

    public static final ConfigOption<String> TYPE = ConfigOptions
            .key("source.type")
            .stringType()
            .defaultValue("unknown");

    public static final ConfigOption<String> HOSTNAME = ConfigOptions
            .key("source.hostname")
            .stringType()
            .defaultValue("localhost");

    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("source.port")
            .intType()
            .defaultValue(8080);

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("source.username")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("source.password")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> DATABASE = ConfigOptions
            .key("source.database")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PARALLELISM = ConfigOptions
            .key("source.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<List<String>> TABLE_LIST = ConfigOptions
            .key("source.table.list")
            .stringType()
            .asList()
            .noDefaultValue();

    public static final ConfigOption<List<String>> EXCLUDED_TABLE_LIST = ConfigOptions
            .key("source.excluded.table.list")
            .stringType()
            .asList()
            .noDefaultValue();

    public static final ConfigOption<String> STARTUP_MODE = ConfigOptions
            .key("source.startup.mode")
            .stringType()
            .defaultValue("initial");

    public static final ConfigOption<String> STARTUP_FILE = ConfigOptions
            .key("source.startup.file")
            .stringType()
            .defaultValue(null);

    public static final ConfigOption<Integer> STARTUP_POSITION = ConfigOptions
            .key("source.startup.position")
            .intType()
            .defaultValue(0);

    public static final ConfigOption<Long> STARTUP_TIMESTAMP = ConfigOptions
            .key("source.startup.timestamp")
            .longType()
            .defaultValue(0L);

    public static final ConfigOption<Map<String, String>> PROPERTIES = ConfigOptions
            .key("source.properties")
            .mapType()
            .noDefaultValue();

    public static final ConfigOption<Integer> SERIALIZATION_BUFFER_SIZE = ConfigOptions
            .key("serialization.buffer.size")
            .intType()
            .defaultValue(2 * 1024);
}
