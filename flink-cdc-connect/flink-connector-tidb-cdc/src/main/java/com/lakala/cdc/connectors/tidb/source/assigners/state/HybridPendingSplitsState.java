package com.lakala.cdc.connectors.tidb.source.assigners.state;


import lombok.Getter;

import java.util.Objects;

/**
 * A {@link PendingSplitsState} for pending hybrid (snapshot & binlog) splits.
 */
@Getter
public class HybridPendingSplitsState extends PendingSplitsState {
    private final SnapshotPendingSplitsState snapshotSourceSplits;
    private final boolean isBinlogSplitAssigned;

    public HybridPendingSplitsState(
            SnapshotPendingSplitsState snapshotSourceSplits, boolean isBinlogSplitAssigned) {
        this.snapshotSourceSplits = snapshotSourceSplits;
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HybridPendingSplitsState that)) {
            return false;
        }
        return isBinlogSplitAssigned == that.isBinlogSplitAssigned
                && Objects.equals(snapshotSourceSplits, that.snapshotSourceSplits);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotSourceSplits, isBinlogSplitAssigned);
    }

    @Override
    public String toString() {
        return "HybridSourceSplitsState{"
                + "snapshotSourceSplits="
                + snapshotSourceSplits
                + ", isBinlogSplitAssigned="
                + isBinlogSplitAssigned
                + '}';
    }
}
