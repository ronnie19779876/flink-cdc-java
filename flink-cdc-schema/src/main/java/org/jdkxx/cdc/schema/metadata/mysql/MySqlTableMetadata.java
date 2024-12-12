package org.jdkxx.cdc.schema.metadata.mysql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.jdkxx.cdc.schema.metadata.ConstraintsMetadata;
import org.jdkxx.cdc.schema.metadata.TableMetadata;
import org.jdkxx.cdc.schema.types.CharacterSet;

import java.util.List;

@Builder
@AllArgsConstructor
public class MySqlTableMetadata implements TableMetadata {
    private String name;
    private MySqlDatabaseMetadata database;
    private CharacterSet charset;
    private List<ColumnMetadata> columns;
    private List<ConstraintsMetadata> constraints;
    private String comment;

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public boolean hasPartitions() {
        return false;
    }

    @Override
    public List<ColumnMetadata> columns() {
        return columns;
    }

    @Override
    public List<ConstraintsMetadata> constraints() {
        return constraints;
    }

    @Override
    public long id() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CharacterSet charset() {
        return charset;
    }
}
