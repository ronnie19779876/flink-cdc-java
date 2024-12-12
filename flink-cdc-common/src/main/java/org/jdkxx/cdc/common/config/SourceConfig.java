package org.jdkxx.cdc.common.config;

import java.io.Serializable;

public interface SourceConfig extends Serializable {

    @FunctionalInterface
    interface Factory<C extends SourceConfig> extends Serializable {
        C create(int subtask);

        default C create() {
            return create(0);
        }
    }
}
