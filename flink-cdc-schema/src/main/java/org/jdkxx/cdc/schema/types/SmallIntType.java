package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Set;

public class SmallIntType extends NumericType {
    public static final int PRECISION = 5;
    public static final int BYTES = 2;
    private static final String FORMAT = "SMALLINT";

    private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(Short.class.getName());

    private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION =
            conversionSet(Short.class.getName(), short.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = Short.class;

    public SmallIntType(boolean isNullable, boolean unsigned) {
        super(isNullable, LogicalTypeRoot.SMALLINT, unsigned, BYTES);
    }

    public SmallIntType(boolean unsigned) {
        this(true, unsigned);
    }


    @Override
    public LogicalType copy(boolean isNullable) {
        return new SmallIntType(isNullable, unsigned());
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
