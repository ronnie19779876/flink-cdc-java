package org.jdkxx.cdc.schema.metadata.table;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.flink.table.types.DataType;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.jdkxx.cdc.schema.types.CharacterSet;

@Builder
@AllArgsConstructor
public class PhysicalColumn implements ColumnMetadata {
    private long id;
    private String name;
    private String comment;
    private CharacterSet charset;
    private DataType type;
    private Object defaultValue;
    private int code;
    private int flag;

    @Override
    public DataType type() {
        return type;
    }

    @Override
    public int code() {
        return code;
    }

    @Override
    public Object defaultValue() {
        return defaultValue;
    }

    @Override
    public String comment() {
        return comment;
    }

    @Override
    public boolean isAutoIncrement() {
        return (flag & AutoIncrementFlag) > 0;
    }

    @Override
    public boolean isPrimaryKey() {
        return (flag & PriKeyFlag) > 0;
    }

    @Override
    public boolean isNotNull() {
        return (flag & NotNullFlag) > 0;
    }

    @Override
    public boolean isUnsigned() {
        return (flag & UnsignedFlag) > 0;
    }

    @Override
    public long id() {
        return id;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public CharacterSet charset() {
        return charset;
    }
}
