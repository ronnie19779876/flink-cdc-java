package org.jdkxx.cdc.schema.types;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.io.Serializable;
import java.util.Objects;

@Data
@Builder
@AllArgsConstructor
public class CharacterSet implements Serializable {
    private String name;
    private String collation;

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof CharacterSet that)) {
            return false;
        }
        return Objects.equals(name, that.name)
                && Objects.equals(collation, that.collation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, collation);
    }
}
