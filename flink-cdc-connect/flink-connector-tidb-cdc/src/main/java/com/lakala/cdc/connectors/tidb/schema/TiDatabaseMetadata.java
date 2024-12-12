package com.lakala.cdc.connectors.tidb.schema;


import lombok.AllArgsConstructor;
import lombok.Builder;
import org.jdkxx.cdc.schema.metadata.DatabaseMetadata;
import org.jdkxx.cdc.schema.types.CharacterSet;

@Builder
@AllArgsConstructor
public class TiDatabaseMetadata implements DatabaseMetadata {
    private long id;
    private String name;
    private CharacterSet charset;

    @Override
    public long id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CharacterSet charset() {
        return charset;
    }

    @Override
    public String catalogName() {
        throw new UnsupportedOperationException();
    }
}
