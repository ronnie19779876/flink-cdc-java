package org.jdkxx.cdc.schema.types;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class TextType extends LogicalType {
    private final int length;
    private static final Set<String> INPUT_OUTPUT_CONVERSION =
            conversionSet(
                    String.class.getName(), byte[].class.getName(), StringData.class.getName());
    private static final Class<?> DEFAULT_CONVERSION = String.class;
    private static final String FORMAT = "TEXT";

    public TextType(boolean isNullable, int length) {
        super(isNullable, LogicalTypeRoot.VARCHAR);
        this.length = length;
    }

    public TextType(int length) {
        this(true, length);
    }

    public int length() {
        return length;
    }

    @Override
    public LogicalType copy(boolean isNullable) {
        return new TextType(isNullable, length);
    }

    @Override
    public String asSerializableString() {
        return withNullability(FORMAT);
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

    @Override
    public List<LogicalType> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <R> R accept(LogicalTypeVisitor<R> visitor) {
        return visitor.visit(this);
    }
}
