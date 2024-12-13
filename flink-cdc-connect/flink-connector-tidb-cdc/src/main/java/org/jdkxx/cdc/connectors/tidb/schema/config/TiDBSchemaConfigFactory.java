package org.jdkxx.cdc.connectors.tidb.schema.config;

import org.apache.flink.configuration.Configuration;
import org.jdkxx.cdc.common.config.SourceConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TiDBSchemaConfigFactory implements SourceConfig.Factory<TiDBSchemaConfig> {
    private String database;
    private List<String> tableList;
    private List<String> excludedTables = new ArrayList<>();
    private Configuration options;

    public void database(String database) {
        this.database = database;
    }

    public void tableList(String... tables) {
        if (tables != null) {
            this.tableList = Arrays.asList(tables);
        } else {
            this.tableList = new ArrayList<>();
        }
    }

    public void excludedTables(String... excludedTables) {
        if (excludedTables != null) {
            this.excludedTables = Arrays.asList(excludedTables);
        }
    }

    public void tikvOptions(Map<String, String> options) {
        if (options != null) {
            this.options = Configuration.fromMap(options);
        } else {
            this.options = new Configuration();
        }
    }

    @Override
    public TiDBSchemaConfig create(int subtask) {
        return TiDBSchemaConfig.builder()
                .database(database)
                .excludedTables(excludedTables)
                .options(options)
                .tableList(tableList)
                .build();
    }
}
