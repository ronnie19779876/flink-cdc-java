package org.jdkxx.cdc.connectors.tidb.source.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.apache.flink.configuration.Configuration;
import org.jdkxx.cdc.connectors.tidb.schema.TiDBSchema;
import org.jdkxx.cdc.common.config.SourceConfig;
import org.jdkxx.cdc.common.options.StartupOptions;
import org.jdkxx.cdc.schema.metadata.table.TablePath;
import org.tikv.common.TiConfiguration;

import java.util.List;

@AllArgsConstructor
@Builder
@Getter
public class TiDBSourceConfig extends Configuration implements SourceConfig {
    private String database;
    private final List<String> tableList;
    private final List<String> excludedTables;
    private final StartupOptions startupOptions;
    private final TiDBSchema schema;

    public boolean isIncluded(TablePath path) {
        return tableList.stream().
                filter(name -> !excludedTables.contains(name))
                .map(name -> TablePath.of(database, name))
                .toList()
                .contains(path);
    }

    public TiConfiguration getTiConfiguration() {
        return schema.getTiConfiguration();
    }
}
