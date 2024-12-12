package org.jdkxx.cdc.schema.types;

public class IntegerType extends IntType {
    private static final String FORMAT = "INTEGER";

    public IntegerType(boolean unsigned) {
        super(unsigned);
    }

    @Override
    String format() {
        return FORMAT;
    }
}
