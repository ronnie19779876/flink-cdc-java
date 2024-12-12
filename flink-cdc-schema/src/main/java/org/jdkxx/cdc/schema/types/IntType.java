package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Set;

public class IntType extends NumericType {
    public static final int PRECISION = 10;
    private static final String FORMAT = "INT";

    private static final Set<String> NULL_OUTPUT_CONVERSION =
            conversionSet(Integer.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(Integer.class.getName(), int.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = Integer.class;

    public IntType(boolean isNullable, boolean unsigned, int size) {
        super(isNullable, LogicalTypeRoot.INTEGER, unsigned, size);
    }

    public IntType(boolean isNullable, boolean unsigned) {
        this(isNullable, unsigned, Integer.BYTES);
    }

    public IntType(boolean unsigned) {
        this(true, unsigned);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new IntType(isNullable, unsigned());
    }

    @Override
    String format() {
        return FORMAT;
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
}
