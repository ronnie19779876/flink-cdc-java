package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.math.BigDecimal;
import java.util.Set;

public class DecimalType extends FixedPointType {
    public static final int DEFAULT_PRECISION = 10;
    public static final int DEFAULT_SCALE = 0;
    private static final String FORMAT = "DECIMAL(%d, %d)";

    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(BigDecimal.class.getName(), DecimalData.class.getName());

    private static final Class<?> DEFAULT_CONVERSION = BigDecimal.class;

    public DecimalType(boolean isNullable, boolean unsigned, int precision, int scale) {
        super(isNullable, LogicalTypeRoot.DECIMAL, unsigned, 0, precision, scale);
        size(size(precision, scale));
    }

    public DecimalType(boolean unsigned, int precision, int scale) {
        this(true, unsigned, precision, scale);
    }

    public DecimalType(boolean unsigned, int precision) {
        this(unsigned, precision, DEFAULT_SCALE);
    }

    public DecimalType() {
        this(false, DEFAULT_PRECISION);
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new DecimalType(isNullable, unsigned(), precision(), scale());
    }

    @Override
    String format() {
        return FORMAT;
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

    private int size(int precision, int scale) {
        if (precision > scale) {
            return precision + 2;
        }
        return scale + 2;
    }
}
