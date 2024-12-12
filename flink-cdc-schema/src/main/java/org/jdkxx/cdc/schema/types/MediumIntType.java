package org.jdkxx.cdc.schema.types;

public class MediumIntType extends IntType {
    public static final int BYTES = 3;
    private static final String FORMAT = "MEDIUMINT";

    public MediumIntType(boolean unsigned) {
        super(true, unsigned, BYTES);
    }

    @Override
    String format() {
        return FORMAT;
    }
}
