package org.jdkxx.cdc.schema.metadata.mysql;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.jdkxx.cdc.common.options.SourceOptions;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.jdkxx.cdc.schema.metadata.ConstraintsMetadata;
import org.jdkxx.cdc.schema.metadata.GenericSchema;
import org.jdkxx.cdc.schema.metadata.table.IndexConstraints;
import org.jdkxx.cdc.schema.metadata.table.KeyConstraints;
import org.jdkxx.cdc.schema.metadata.table.PhysicalColumn;
import org.jdkxx.cdc.schema.metadata.table.TablePath;
import org.jdkxx.cdc.schema.types.CharacterSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.jdkxx.cdc.schema.metadata.ConstraintsMetadata.ConstraintsType;

@Slf4j
public class MySqlSchema extends GenericSchema {
    private static final Set<String> builtinDatabases =
            new HashSet<>() {
                {
                    add("information_schema");
                    add("mysql");
                    add("performance_schema");
                    add("sys");
                }
            };

    private MySqlSchema() {
    }

    @Override
    public void open(Configuration parameters) {
        super.open(parameters);
        try {
            log.info("Start reading MySQL schema, database:{}, tables:{}",
                    configuration.get(SourceOptions.DATABASE),
                    configuration.get(SourceOptions.TABLE_LIST));
            MySqlDatabaseMetadata database = loadDatabase();
            loadTables(database);
        } catch (DatabaseNotExistException | TableNotExistException e) {
            throw new CatalogException(e);
        } finally {
            if (this.dataSource != null) {
                this.dataSource.close();
            }
        }
    }

    private MySqlDatabaseMetadata loadDatabase() throws DatabaseNotExistException {
        String sql = "SELECT `CATALOG_NAME`,`SCHEMA_NAME`,`DEFAULT_CHARACTER_SET_NAME`,`DEFAULT_COLLATION_NAME` " +
                "FROM `INFORMATION_SCHEMA`.`SCHEMATA` WHERE `SCHEMA_NAME`=?";
        String databaseName = configuration.get(SourceOptions.DATABASE);
        if (!builtinDatabases.contains(databaseName)) {
            try (Connection conn = dataSource.getConnection()) {
                PreparedStatement ps = conn.prepareStatement(sql);
                ps.setObject(1, databaseName);
                ResultSet rs = ps.executeQuery();
                if (rs.next()) {
                    return MySqlDatabaseMetadata.builder()
                            .catalogName(rs.getString("CATALOG_NAME"))
                            .name(rs.getString("SCHEMA_NAME"))
                            .charset(new CharacterSet(
                                    rs.getString("DEFAULT_CHARACTER_SET_NAME"),
                                    rs.getString("DEFAULT_COLLATION_NAME")
                            ))
                            .build();
                } else {
                    throw new DatabaseNotExistException(databaseName, databaseName);
                }
            } catch (SQLException e) {
                throw new CatalogException(
                        String.format(
                                "The following SQL query could not be executed: %s", sql),
                        e);
            }
        } else {
            throw new DatabaseNotExistException(databaseName, databaseName);
        }
    }

    private void loadTables(MySqlDatabaseMetadata database) throws TableNotExistException {
        List<String> excludedTables = configuration.get(SourceOptions.EXCLUDED_TABLE_LIST);
        if (excludedTables == null) {
            excludedTables = new ArrayList<>();
        }
        List<String> tableNames = configuration.get(SourceOptions.TABLE_LIST);
        for (String tableName : tableNames) {
            if (!builtinDatabases.contains(database.name())
                    && !excludedTables.contains(tableName)) {
                loadTable(database, tableName);
            }
        }
    }

    private void loadTable(MySqlDatabaseMetadata database, String tableName) throws TableNotExistException {
        TablePath path = TablePath.of(database.name(), tableName);
        String sql = "SELECT * FROM `INFORMATION_SCHEMA`.`TABLES` WHERE TABLE_SCHEMA=? AND TABLE_NAME=?";
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setObject(1, path.getDatabaseName());
            ps.setObject(2, path.getTableName());
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                CharacterSet charset = getDefaultCharacterSet(rs.getString("TABLE_COLLATION"));
                MySqlTableMetadata table = MySqlTableMetadata.builder()
                        .name(path.getTableName())
                        .database(database)
                        .columns(getMySqlColumns(path))
                        .constraints(getKeyConstraints(path))
                        .charset(charset)
                        .comment(rs.getString("TABLE_COMMENT"))
                        .build();
                this.tables.put(path, table);
            } else {
                throw new TableNotExistException(database.name(), path.toObjectPath());
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "The following SQL query could not be executed: %s", sql),
                    e);
        }
    }

    private List<ColumnMetadata> getMySqlColumns(TablePath path) {
        List<ColumnMetadata> columns = new ArrayList<>();
        String sql = "SELECT * FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=? " +
                "ORDER BY `ORDINAL_POSITION`";
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setObject(1, path.getDatabaseName());
            ps.setObject(2, path.getTableName());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                PhysicalColumn column = MySqlColumnBuilder.builder()
                        .name(rs.getString("COLUMN_NAME"))
                        .code(rs.getString("DATA_TYPE"))
                        .autoIncrement(StringUtils.equalsIgnoreCase("auto_increment", rs.getString("EXTRA")))
                        .nullable(StringUtils.equalsIgnoreCase("YES", rs.getString("IS_NULLABLE")))
                        .precision(rs.getInt("CHARACTER_MAXIMUM_LENGTH") > 0 ?
                                rs.getInt("CHARACTER_MAXIMUM_LENGTH") :
                                rs.getInt("NUMERIC_PRECISION"))
                        .scale(rs.getInt("NUMERIC_SCALE"))
                        .unsigned(StringUtils.indexOf(rs.getString("COLUMN_TYPE"), "unsigned") >= 0)
                        .charset(new CharacterSet(rs.getString("CHARACTER_SET_NAME"),
                                rs.getString("COLLATION_NAME")))
                        .defaultValue(rs.getString("COLUMN_DEFAULT"))
                        .comment(rs.getString("COLUMN_COMMENT"))
                        .build();
                columns.add(column);
            }
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "The following SQL query could not be executed: %s", sql),
                    e);
        }
        return columns;
    }

    private List<ConstraintsMetadata> getKeyConstraints(TablePath path) {
        Map<String, ConstraintsMetadata> map = new HashMap<>(5);
        String sql = "SELECT T1.`CONSTRAINT_NAME`,`CONSTRAINT_TYPE`,`COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`TABLE_CONSTRAINTS` T1 " +
                "INNER JOIN `INFORMATION_SCHEMA`.`KEY_COLUMN_USAGE` T2 ON T1.TABLE_SCHEMA = T2.TABLE_SCHEMA " +
                "AND T1.TABLE_NAME = T2.TABLE_NAME AND T1.CONSTRAINT_NAME = T2.CONSTRAINT_NAME " +
                "WHERE T1.`TABLE_SCHEMA`=? AND T1.TABLE_NAME = ?";
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setObject(1, path.getDatabaseName());
            ps.setObject(2, path.getTableName());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String name = rs.getString("CONSTRAINT_NAME");
                ConstraintsMetadata constraints = map.get(name);
                if (constraints == null) {
                    constraints = KeyConstraints.builder()
                            .constraintName(name)
                            .type(ConstraintsType.of(rs.getString("CONSTRAINT_TYPE")))
                            .build();
                    map.put(name, constraints);
                }
                constraints.addColumn(rs.getString("COLUMN_NAME"));
            }
            return getIndexConstraints(path, map);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "The following SQL query could not be executed: %s", sql),
                    e);
        }
    }

    private List<ConstraintsMetadata> getIndexConstraints(TablePath path,
                                                          Map<String, ConstraintsMetadata> constraints) throws SQLException {
        Map<String, ConstraintsMetadata> indexes = new HashMap<>(5);
        String sql = "SELECT `INDEX_NAME`,`INDEX_TYPE`,`COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`statistics` WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME` = ?";
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setObject(1, path.getDatabaseName());
            ps.setObject(2, path.getTableName());
            ResultSet rs = ps.executeQuery();
            while (rs.next()) {
                String name = rs.getString("INDEX_NAME");
                if (!constraints.containsKey(name)) {
                    ConstraintsMetadata index = indexes.get(name);
                    if (index == null) {
                        index = IndexConstraints.builder()
                                .indexName(name)
                                .indexType(rs.getString("INDEX_TYPE"))
                                .build();
                        indexes.put(name, index);
                    }
                    index.addColumn(rs.getString("COLUMN_NAME"));
                }
            }
        }
        constraints.putAll(indexes);
        return new ArrayList<>(constraints.values());
    }

    private CharacterSet getDefaultCharacterSet(String collation) {
        String sql = "SELECT `CHARACTER_SET_NAME` FROM `INFORMATION_SCHEMA`.`COLLATIONS` WHERE `COLLATION_NAME` = ?";
        try (Connection conn = dataSource.getConnection()) {
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setObject(1, collation);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return new CharacterSet(rs.getString("CHARACTER_SET_NAME"), collation);
            }
            return new CharacterSet("", collation);
        } catch (SQLException e) {
            throw new CatalogException(
                    String.format(
                            "The following SQL query could not be executed: %s", sql),
                    e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends GenericSchema.Builder<MySqlSchema> {
        @Override
        public MySqlSchema build() {
            MySqlSchema schema = new MySqlSchema();
            schema.open(configuration);
            return schema;
        }
    }
}
