package org.jdkxx.cdc.schema.metadata;

import org.apache.commons.lang3.StringUtils;

import java.util.List;

public interface ConstraintsMetadata extends Metadata {
    void addColumn(String name);

    boolean contains(String columnName);

    ConstraintsType type();

    List<String> columns();

    enum ConstraintsType {
        PRIMARY_KEY("PRIMARY KEY"),
        UNIQUE_KEY("UNIQUE"),
        NORMAL_KEY("INDEX");

        private final String name;

        ConstraintsType(String name) {
            this.name = name;
        }

        public static ConstraintsType of(String name) {
            for (ConstraintsType e : ConstraintsType.values()) {
                if (StringUtils.equalsIgnoreCase(name, e.name)) {
                    return e;
                }
            }
            return ConstraintsType.NORMAL_KEY;
        }
    }
}
