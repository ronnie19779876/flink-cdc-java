package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DateTimeType extends LogicalType {
    private static final String FORMAT = "DATETIME";

    private static final Set<String> NULL_OUTPUT_CONVERSION =
            conversionSet(
                    java.sql.Date.class.getName(),
                    java.time.LocalDateTime.class.getName(),
                    Integer.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(
                    java.sql.Date.class.getName(),
                    java.time.LocalDateTime.class.getName(),
                    Integer.class.getName(),
                    int.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = java.time.LocalDateTime.class;

    public DateTimeType() {
        super(true, LogicalTypeRoot.DATE);
    }

    protected DateTimeType(boolean isNullable) {
        super(isNullable, LogicalTypeRoot.DATE);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new DateTimeType(isNullable);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
    }

    @Override
    public boolean supportsInputConversion(Class<?> clazz) {
        return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
    }

    @Override
    public boolean supportsOutputConversion(Class<?> clazz) {
        if (isNullable()) {
            return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
        }
        return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
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
