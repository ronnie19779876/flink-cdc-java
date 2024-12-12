package org.jdkxx.cdc.schema.metadata.table;

import lombok.Data;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

@Data
public class TablePath implements Serializable {
    private long tableId;
    private String databaseName;
    private String tableName;

    public TablePath(String databaseName, String tableName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(databaseName),
                "databaseName cannot be null or empty");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(tableName),
                "tableName cannot be null or empty");

        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    public TablePath(long tableId, String databaseName, String tableName) {
        this(databaseName, tableName);
        this.tableId = tableId;
    }

    public String getFullName() {
        return String.format("%s.%s", databaseName, tableName);
    }

    public ObjectPath toObjectPath() {
        return new ObjectPath(databaseName, tableName);
    }

    public static TablePath fromString(String fullName) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(fullName), "fullName cannot be null or empty");

        String[] paths = fullName.split("\\.");

        if (paths.length != 2) {
            throw new IllegalArgumentException(
                    String.format(
                            "Cannot get split '%s' to get databaseName and objectName", fullName));
        }

        return new TablePath(paths[0], paths[1]);
    }

    public static TablePath of(long tableId, String fullName) {
        TablePath path = fromString(fullName);
        path.tableId = tableId;
        return path;
    }

    public static TablePath of(String databaseName, String tableName) {
        return fromString(databaseName + "." + tableName);
    }

    public static TablePath of(long tableId, String databaseName, String tableName) {
        TablePath path = fromString(databaseName + "." + tableName);
        path.tableId = tableId;
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (!(o instanceof TablePath that)) {
            return false;
        }
        return Objects.equals(databaseName, that.databaseName)
                && Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(databaseName, tableName);
    }

    @Override
    public String toString() {
        return String.format("%s.%s.{%s}", databaseName, tableName, tableId);
    }
}
