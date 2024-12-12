package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;

public class MediumTextType extends TextType {
    private static final String FORMAT = "MEDIUMTEXT";

    public MediumTextType(boolean isNullable, int length) {
        super(isNullable, length);
    }

    public MediumTextType(int length) {
        this(true, length);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new MediumTextType(isNullable, length());
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }
}
