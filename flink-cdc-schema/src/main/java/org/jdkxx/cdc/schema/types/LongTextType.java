package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;

public class LongTextType extends TextType {
    private static final String FORMAT = "LONGTEXT";

    public LongTextType(boolean isNullable, int length) {
        super(isNullable, length);
    }

    public LongTextType(int length) {
        this(true, length);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new LongTextType(isNullable, length());
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }
}
