package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;

public class TinyTextType extends TextType {
    private static final String FORMAT = "TINYTEXT";

    public TinyTextType(boolean isNullable, int length) {
        super(isNullable, length);
    }

    public TinyTextType(int length) {
        this(true, length);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TinyTextType(isNullable, length());
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }
}
