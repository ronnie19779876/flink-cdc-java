package org.jdkxx.cdc.schema.metadata;

import org.jdkxx.cdc.schema.types.CharacterSet;

import java.io.Serializable;

public interface Metadata extends Serializable {
    long id();

    String name();

    CharacterSet charset();
}
