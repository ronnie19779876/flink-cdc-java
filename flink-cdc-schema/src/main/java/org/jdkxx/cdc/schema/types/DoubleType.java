package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Set;

public class DoubleType extends FloatingPointType {
    public static final int PRECISION = 15; // adopted from Calcite

    private static final String FORMAT = "DOUBLE";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(Double.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(Double.class.getName(), double.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = Double.class;

    public DoubleType(boolean isNullable, boolean unsigned) {
        super(isNullable, LogicalTypeRoot.DOUBLE, unsigned, Double.BYTES);
    }

    public DoubleType(boolean isNullable) {
        this(isNullable, false);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new DoubleType(isNullable, unsigned());
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
