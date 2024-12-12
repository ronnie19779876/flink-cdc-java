package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Set;

public class BigIntType extends NumericType {
    public static final int PRECISION = 19;

    private static final String FORMAT = "BIGINT";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(Long.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(Long.class.getName(), long.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = Long.class;

    public BigIntType(boolean isNullable, boolean unsigned) {
        super(isNullable, LogicalTypeRoot.BIGINT, unsigned, Long.BYTES);
    }

    public BigIntType(boolean unsigned) {
        this(true, unsigned);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new BigIntType(isNullable, unsigned());
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
