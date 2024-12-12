package org.jdkxx.cdc.schema.metadata.mysql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.jdkxx.cdc.schema.metadata.DatabaseMetadata;
import org.jdkxx.cdc.schema.types.CharacterSet;

@Builder
@AllArgsConstructor
public class MySqlDatabaseMetadata implements DatabaseMetadata {
    private String name;
    private CharacterSet charset;
    private String catalogName;

    @Override
    public String catalogName() {
        return catalogName;
    }

    @Override
    public long id() {
        return 0;
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
