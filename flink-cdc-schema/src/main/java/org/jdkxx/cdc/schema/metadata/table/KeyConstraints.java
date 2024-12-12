package org.jdkxx.cdc.schema.metadata.table;

import org.jdkxx.cdc.schema.metadata.ConstraintsMetadata;
import org.jdkxx.cdc.schema.types.CharacterSet;

import java.util.ArrayList;
import java.util.List;

public class KeyConstraints implements ConstraintsMetadata {
    private ConstraintsType type;
    private final List<String> columns = new ArrayList<>();
    private final String constraintName;

    public KeyConstraints(ConstraintsType type, String constraintName) {
        this.type = type;
        this.constraintName = constraintName;
    }

    @Override
    public void addColumn(String name) {
        columns.add(name);
    }

    @Override
    public boolean contains(String columnName) {
        return columns.contains(columnName);
    }

    @Override
    public ConstraintsType type() {
        return type;
    }

    @Override
    public List<String> columns() {
        return columns;
    }

    @Override
    public long id() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
        return constraintName;
    }

    @Override
    public CharacterSet charset() {
        throw new UnsupportedOperationException();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private ConstraintsType type;
        private String constraintName;

        public Builder type(ConstraintsType type) {
            this.type = type;
            return this;
        }

        public Builder constraintName(String constraintName) {
            this.constraintName = constraintName;
            return this;
        }

        public KeyConstraints build() {
            return new KeyConstraints(type, constraintName);
        }
    }
}
