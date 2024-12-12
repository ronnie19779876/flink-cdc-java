package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;

public class LongBlobType extends BlobType {
    private static final String FORMAT = "LONGBLOB";

    public LongBlobType(boolean isNullable) {
        super(isNullable);
    }

    public LongBlobType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new LongBlobType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }
}
