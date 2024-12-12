package org.jdkxx.cdc.schema.metadata.table;

import java.util.Objects;

public class IndexConstraints extends KeyConstraints {
    private final String indexType;

    public IndexConstraints(String indexType, String constraintName) {
        super(ConstraintsType.NORMAL_KEY, constraintName);
        this.indexType = indexType;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexConstraints that)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        return super.equals(that)
                && Objects.equals(name(), that.name())
                && Objects.equals(indexType, that.indexType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexType, name());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder extends KeyConstraints.Builder {
        private String type;
        private String name;

        public Builder indexType(String type) {
            this.type = type;
            return this;
        }

        public Builder indexName(String name) {
            this.name = name;
            return this;
        }

        public IndexConstraints build() {
            return new IndexConstraints(type, name);
        }
    }
}
