package org.jdkxx.cdc.connectors.tidb.schema;

import org.jdkxx.cdc.connectors.tidb.schema.config.TiDBSchemaConfig;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceOptions;
import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.jdkxx.cdc.schema.metadata.GenericSchema;
import org.jdkxx.cdc.schema.metadata.TableMetadata;
import org.jdkxx.cdc.schema.metadata.table.TablePath;
import org.jdkxx.cdc.schema.types.CharacterSet;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.meta.TiDBInfo;
import org.tikv.common.meta.TiPartitionInfo;
import org.tikv.common.meta.TiTableInfo;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

@Slf4j
public class TiDBSchema extends GenericSchema {
    private final TiDBSchemaConfig config;
    private TiDatabaseMetadata database;
    private final TiConfiguration tiConf;
    private transient HikariDataSource dataSource;

    TiDBSchema(final TiDBSchemaConfig config) {
        this.config = config;
        this.tiConf = TiDBSourceOptions.getTiConf(config.getOptions());
    }

    public List<TiTableMetadata> getTableMetadataList() {
        return this.tables.values().stream()
                .map(table -> table.as(TiTableMetadata.class))
                .toList();
    }

    public List<TablePath> listTablePaths() {
        return getTableMetadataList().stream()
                .map(table -> TablePath.of(table.id(), database.name(), table.name()))
                .toList();
    }

    public TiTableMetadata getTable(long tableId) throws TableNotExistException {
        for (Map.Entry<TablePath, TableMetadata> entry : tables.entrySet()) {
            TiTableMetadata table = entry.getValue().as(TiTableMetadata.class);

            if (table.hasPartitions() && table.getPartitions().stream()
                    .map(TiTablePartition::getId)
                    .toList().contains(tableId)) {
                return table;
            } else if (table.getId() == tableId) {
                return table;
            }
        }
        throw new CatalogException("TiDB TableId: {" + tableId + "} not found");
    }

    @Override
    public void open(final Configuration parameters) {
        super.open(parameters);
        try (TiSession session = TiSession.create(this.tiConf)) {
            TiDBInfo tiDBInfo = session.getCatalog().getDatabase(config.getDatabase());
            if (tiDBInfo != null) {
                this.database = TiDatabaseMetadata.builder()
                        .id(tiDBInfo.getId())
                        .name(tiDBInfo.getName())
                        .charset(new CharacterSet(tiDBInfo.getCharset(), tiDBInfo.getCollate()))
                        .build();
                List<TiTableMetadata> tables = listTiTables(tiDBInfo, session);
                tables.forEach(table ->
                        this.tables.put(TablePath.of(table.id(), this.database.name(), table.name()), table));

                //we will copy the schema of Source to Sink.
                //copyToSink(new Configuration(parameters));
            }
        } catch (Exception e) {
            throw new CatalogException(e);
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    public TiConfiguration getTiConfiguration() {
        return tiConf;
    }

    private List<TiTableMetadata> listTiTables(TiDBInfo tiDBInfo, TiSession session) {
        List<String> tableNames = config.getTableList();
        Stream<TiTableInfo> stream;
        if (tableNames == null || tableNames.isEmpty()) {
            stream = session.getCatalog().listTables(tiDBInfo).stream();
        } else {
            stream = tableNames.stream()
                    .map(tableName -> session.getCatalog().getTable(tiDBInfo.getName(), tableName));
        }
        long position = session.getTimestamp().getVersion();
        return stream
                .filter(table -> !config.getExcludedTables().contains(table.getName()))
                .map(table -> toTiTable(table, position))
                .toList();
    }

    private TiTableMetadata toTiTable(TiTableInfo tableInfo, long position) {
        List<ColumnMetadata> columns = tableInfo.getColumns().stream()
                .map(columnInfo -> TiTableColumnBuilder.builder().build(columnInfo))
                .toList();
        return TiTableMetadata.builder()
                .id(tableInfo.getId())
                .name(tableInfo.getName())
                .database(this.database)
                .charset(new CharacterSet(tableInfo.getCharset(), tableInfo.getCollate()))
                .pkHandle(tableInfo.isPkHandle())
                .partitions(listPartitions(tableInfo.getPartitionInfo()))
                .columns(columns)
                .kind(TableMetadata.TableKind.TABLE)
                .position(position)
                .build();
    }

    private List<TiTablePartition> listPartitions(TiPartitionInfo tiPartitionInfo) {
        if (tiPartitionInfo != null && tiPartitionInfo.isEnable()) {
            return tiPartitionInfo.getDefs().stream().map(def ->
                    TiTablePartition.builder()
                            .id(def.getId())
                            .name(def.getName())
                            .type(TiTablePartition.PartitionType.valueOf(tiPartitionInfo.getType().name()))
                            .build()).toList();
        }
        return null;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public TiDBSchema build(TiDBSchemaConfig config) {
            TiDBSchema schema = new TiDBSchema(config);
            schema.open(config.getOptions());
            return schema;
        }
    }
}
