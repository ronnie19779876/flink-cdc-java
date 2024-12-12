package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;

public class MediumBlobType extends BlobType {
    private static final String FORMAT = "MEDIUMBLOB";

    public MediumBlobType(boolean isNullable) {
        super(isNullable);
    }

    public MediumBlobType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new MediumBlobType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }
}
