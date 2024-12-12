package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.util.Collections;
import java.util.List;

public abstract class NumericType extends LogicalType {
    private boolean unsigned;
    private int size;

    public NumericType(boolean isNullable, LogicalTypeRoot typeRoot, boolean unsigned, int size) {
        super(isNullable, typeRoot);
        this.unsigned = unsigned;
        this.size = size;
    }

    public boolean unsigned() {
        return unsigned;
    }

    public void unsigned(boolean unsigned) {
        this.unsigned = unsigned;
    }

    public int size() {
        return size;
    }

    public void size(int size) {
        this.size = size;
    }

    @Override
    public String asSerializableString() {
        String format = unsigned() ? format() + " UNSIGNED" : format();
        return withNullability(format);
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }

    abstract String format();
}
