package org.jdkxx.cdc.connectors.tidb.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.jdkxx.cdc.schema.metadata.ConstraintsMetadata;
import org.jdkxx.cdc.schema.metadata.TableMetadata;
import org.jdkxx.cdc.schema.metadata.table.TablePath;
import org.jdkxx.cdc.schema.types.CharacterSet;

import java.util.List;

@Getter
@Builder
@AllArgsConstructor
public class TiTableMetadata implements TableMetadata {
    private long id;
    private String name;
    private TiDatabaseMetadata database;
    private CharacterSet charset;
    private boolean pkHandle;
    private String comment;
    private List<TiTablePartition> partitions;
    private List<ColumnMetadata> columns;
    private List<ConstraintsMetadata> constraints;
    private TableKind kind;
    private long position;

    public TablePath path() {
        return new TablePath(id, database.name(), name);
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public boolean hasPartitions() {
        return partitions != null && !partitions.isEmpty();
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
    public CharacterSet charset() {
        return charset;
    }
}
