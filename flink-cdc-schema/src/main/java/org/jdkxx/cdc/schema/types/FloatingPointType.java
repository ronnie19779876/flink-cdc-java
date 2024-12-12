package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalTypeRoot;

public abstract class FloatingPointType extends NumericType {
    public FloatingPointType(boolean isNullable, LogicalTypeRoot typeRoot, boolean unsigned, int size) {
        super(isNullable, typeRoot, unsigned, size);
    }
}
