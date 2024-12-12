package com.lakala.cdc.connectors.tidb.source.config;

import com.lakala.cdc.connectors.tidb.schema.TiDBSchema;
import com.lakala.cdc.connectors.tidb.schema.config.TiDBSchemaConfig;
import com.lakala.cdc.connectors.tidb.schema.config.TiDBSchemaConfigFactory;
import org.jdkxx.cdc.common.config.SourceConfig;
import org.jdkxx.cdc.common.options.StartupOptions;

import java.util.Map;

public class TiDBSourceConfigFactory implements SourceConfig.Factory<TiDBSourceConfig> {
    private StartupOptions startupOptions;
    private final TiDBSchemaConfigFactory factory = new TiDBSchemaConfigFactory();

    public void database(String database) {
        factory.database(database);
    }

    public void tableList(String... tables) {
        factory.tableList(tables);
    }

    public void excludes(String... excludes) {
        factory.excludedTables(excludes);
    }

    public void tikvOptions(Map<String, String> options) {
        factory.tikvOptions(options);
    }

    public void startupOptions(StartupOptions startupOptions) {
        this.startupOptions = startupOptions;
    }

    @Override
    public TiDBSourceConfig create(int subtask) {
        TiDBSchemaConfig config = factory.create(subtask);
        TiDBSchema schema = TiDBSchema.builder().build(config);
        return TiDBSourceConfig.builder()
                .database(config.getDatabase())
                .tableList(config.getTableList())
                .excludedTables(config.getExcludedTables())
                .startupOptions(this.startupOptions)
                .schema(schema)
                .build();
    }
}
