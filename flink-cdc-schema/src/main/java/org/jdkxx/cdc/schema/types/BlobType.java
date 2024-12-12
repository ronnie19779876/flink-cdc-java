package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class BlobType extends LogicalType {
    private static final String FORMAT = "BLOB";

    private static final Set<String> INPUT_OUTPUT_CONVERSION = conversionSet(byte[].class.getName());

    private static final Class<?> DEFAULT_CONVERSION = byte[].class;

    public BlobType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.BINARY);
    }

    public BlobType() {
        this(true);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new BlobType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        return INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public Class<?> getDefaultConversion() {
        return DEFAULT_CONVERSION;
    }

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
