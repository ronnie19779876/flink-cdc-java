package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Set;

public class TinyIntType extends NumericType {
    public static final int PRECISION = 3;
    public static final int BYTES = 2;
    private static final String FORMAT = "TINYINT";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(Byte.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(Byte.class.getName(), byte.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = Byte.class;

    public TinyIntType(boolean isNullable, boolean unsigned, int size) {
        super(isNullable, LogicalTypeRoot.TINYINT, unsigned, size);
    }

    public TinyIntType(boolean isNullable, boolean unsigned) {
        this(isNullable, unsigned, BYTES);
    }

    public TinyIntType(boolean unsigned) {
        this(true, unsigned, BYTES);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TinyIntType(isNullable, unsigned());
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
