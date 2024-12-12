package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

public abstract class FixedPointType extends NumericType {
    public static final int MIN_PRECISION = 1;
    public static final int MAX_PRECISION = 38;
    public static final int MIN_SCALE = 0;
    private final int precision;
    private final int scale;

    public FixedPointType(boolean isNullable, LogicalTypeRoot typeRoot, boolean unsigned, int size, int precision, int scale) {
        super(isNullable, typeRoot, unsigned, size);
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Decimal precision must be between %d and %d (both inclusive).",
                            MIN_PRECISION, MAX_PRECISION));
        }
        if (scale < MIN_SCALE || scale > precision) {
            throw new ValidationException(
                    String.format(
                            "Decimal scale must be between %d and the precision %d (both inclusive).",
                            MIN_SCALE, precision));
        }
        if (unsigned) {
            this.precision = precision + 1;
        } else {
            this.precision = precision;
        }
        this.scale = scale;
    }

    public int precision() {
        return precision;
    }

    public int scale() {
        return scale;
    }

    @Override
    public String asSerializableString() {
        String format = unsigned() ? String.format(format() + " UNSIGNED", precision(), scale())
                : String.format(format(), precision(), scale());
        return withNullability(format);
    }
}
