package org.jdkxx.cdc.common.options;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.List;
import java.util.Map;

public class SinkOptions {
    public static final ConfigOption<String> TYPE = ConfigOptions
            .key("sink.type")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> HOSTNAME = ConfigOptions
            .key("sink.hostname")
            .stringType()
            .defaultValue("localhost");

    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("sink.port")
            .intType()
            .defaultValue(8080);

    public static final ConfigOption<String> USERNAME = ConfigOptions
            .key("sink.username")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> PASSWORD = ConfigOptions
            .key("sink.password")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> DATABASE = ConfigOptions
            .key("sink.database")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PARALLELISM = ConfigOptions
            .key("sink.parallelism")
            .intType()
            .defaultValue(1);

    public static final ConfigOption<Map<String, String>> PROPERTIES = ConfigOptions
            .key("sink.properties")
            .mapType()
            .noDefaultValue();

    public static final ConfigOption<Integer> SNAPSHOT_BATCH_SIZE = ConfigOptions
            .key("sink.snapshot.batch.size")
            .intType()
            .defaultValue(1000);

    public static final ConfigOption<Integer> SNAPSHOT_FLUSH_INTERVAL_SECONDS = ConfigOptions
            .key("sink.snapshot.flush.interval.seconds")
            .intType()
            .defaultValue(5);

    public static final ConfigOption<Integer> SNAPSHOT_QUEUE_SIZE = ConfigOptions
            .key("sink.snapshot.queue.size")
            .intType()
            .defaultValue(50000);

    public static final ConfigOption<Boolean> CREATED_SCHEMA = ConfigOptions
            .key("sink.create.schema.allowed")
            .booleanType()
            .defaultValue(false);

    public static final ConfigOption<Boolean> OPERATOR_DELETE = ConfigOptions
            .key("sink.operator.delete.allowed")
            .booleanType()
            .defaultValue(true);

    public static final ConfigOption<List<String>> OPERATOR_DELETE_EXCLUDED = ConfigOptions
            .key("sink.operator.delete.excluded-tables")
            .stringType()
            .asList()
            .noDefaultValue();

    public static final ConfigOption<String> DATA_SAVE_DIRECTORY = ConfigOptions
            .key("sink.data.save.directory")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Long> DATA_FILE_SIZE = ConfigOptions
            .key("sink.data.file.size")
            .longType()
            .defaultValue(0x7FFFFFFFL);

    public static final ConfigOption<String> JDBC_URL = ConfigOptions
            .key("sink.jdbc.url")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<String> JDBC_DRIVER = ConfigOptions
            .key("sink.jdbc.driver")
            .stringType()
            .defaultValue("com.mysql.cj.jdbc.Driver");

    public static final ConfigOption<String> JDBC_POOL_SIZE = ConfigOptions
            .key("sink.jdbc.pool.size")
            .stringType()
            .defaultValue("5");

    public static final ConfigOption<Integer> LOADER_FLUSH_SIZE = ConfigOptions
            .key("sink.loader.flush.size")
            .intType()
            .defaultValue(1000);

    public static final ConfigOption<Integer> LOADER_FLUSH_INTERVAL = ConfigOptions
            .key("sink.loader.flush.interval")
            .intType()
            .defaultValue(10);

    public static final ConfigOption<Integer> LOADER_QUEUE_SIZE = ConfigOptions
            .key("sink.loader.queue.size")
            .intType()
            .defaultValue(50000);

    public static final ConfigOption<Long> LOADER_CACHE_MAX_SIZE = ConfigOptions
            .key("sink.loader.cache.max.size")
            .longType()
            .defaultValue(20L);

    public static final ConfigOption<Integer> LOADER_CACHE_EXPIRE = ConfigOptions
            .key("sink.loader.cache.expire")
            .intType()
            .defaultValue(600);

}
