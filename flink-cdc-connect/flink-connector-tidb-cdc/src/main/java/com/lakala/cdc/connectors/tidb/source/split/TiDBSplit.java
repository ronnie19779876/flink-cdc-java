package com.lakala.cdc.connectors.tidb.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class TiDBSplit implements SourceSplit {
    final String splitId;

    @Nullable
    transient byte[] serializedFormCache;

    TiDBSplit(String splitId) {
        this.splitId = splitId;
    }

    @Override
    public String splitId() {
        return this.splitId;
    }

    /**
     * Checks whether this split is a snapshot split.
     */
    public final boolean isSnapshotSplit() {
        return getClass() == TiDBSnapshotSplit.class;
    }

    /**
     * Checks whether this split is a binlog split.
     */
    public final boolean isBinlogSplit() {
        return getClass() == TiDBBinlogSplit.class;
    }

    /**
     * Casts this split into a {@link TiDBSnapshotSplit}.
     */
    public final TiDBSnapshotSplit asSnapshotSplit() {
        return (TiDBSnapshotSplit) this;
    }

    /**
     * Casts this split into a {@link TiDBBinlogSplit}.
     */
    public final TiDBBinlogSplit asBinlogSplit() {
        return (TiDBBinlogSplit) this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TiDBSplit that)) {
            return false;
        }
        return Objects.equals(splitId, that.splitId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
}
