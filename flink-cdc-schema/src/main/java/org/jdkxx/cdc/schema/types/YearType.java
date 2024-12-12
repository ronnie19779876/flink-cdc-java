package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;

public class YearType extends DateTimeType {
    private static final String FORMAT = "YEAR";

    public YearType() {
        super();
    }

    public YearType(boolean isNullable) {
        super(isNullable);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new YearType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }
}
