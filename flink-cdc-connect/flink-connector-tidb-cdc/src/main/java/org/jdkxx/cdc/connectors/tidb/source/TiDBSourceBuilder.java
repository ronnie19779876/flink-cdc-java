package org.jdkxx.cdc.connectors.tidb.source;

import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import org.jdkxx.cdc.common.options.StartupOptions;

import java.util.Map;

public class TiDBSourceBuilder<T> {
    private final TiDBSourceConfigFactory factory = new TiDBSourceConfigFactory();
    private TiKVDeserializationSchema<T> deserializer;

    public TiDBSourceBuilder<T> database(String database) {
        factory.database(database);
        return this;
    }

    public TiDBSourceBuilder<T> tableList(String... tables) {
        factory.tableList(tables);
        return this;
    }

    public TiDBSourceBuilder<T> excludedTables(String... excludedTables) {
        factory.excludes(excludedTables);
        return this;
    }

    public TiDBSourceBuilder<T> startupOptions(StartupOptions startupOptions) {
        factory.startupOptions(startupOptions);
        return this;
    }

    public TiDBSourceBuilder<T> tikvOptions(Map<String, String> options) {
        factory.tikvOptions(options);
        return this;
    }

    public TiDBSourceBuilder<T> deserializer(TiKVDeserializationSchema<T> deserializer) {
        this.deserializer = deserializer;
        return this;
    }

    public TiDBSource<T> build() {
        return new TiDBSource<>(factory.create(), deserializer);
    }
}
