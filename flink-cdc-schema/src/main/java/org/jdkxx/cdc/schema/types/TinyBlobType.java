package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;

public class TinyBlobType extends BlobType {
    private static final String FORMAT = "TINYBLOB";

    public TinyBlobType(boolean isNullable) {
        super(isNullable);
    }

    public TinyBlobType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TinyBlobType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }
}
