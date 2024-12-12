package org.jdkxx.cdc.schema.metadata;

import java.util.List;

public interface TableMetadata extends Metadata {
    enum TableKind {
        TABLE,
        VIEW
    }

    String comment();

    boolean hasPartitions();

    List<ColumnMetadata> columns();

    List<ConstraintsMetadata> constraints();

    default <T extends TableMetadata> T as(Class<T> clazz) {
        return clazz.cast(this);
    }
}
