package org.jdkxx.cdc.connectors.tidb.schema.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.jdkxx.cdc.common.config.SourceConfig;

import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
public class TiDBSchemaConfig extends Configuration implements SourceConfig {
    private String database;
    private List<String> tableList;
    private List<String> excludedTables;
    private Configuration options;

}
