package org.jdkxx.cdc.connectors.tidb.source.split;

import lombok.extern.slf4j.Slf4j;


@Slf4j
public abstract class TiDBSplitState {
    final TiDBSplit split;

    public TiDBSplitState(TiDBSplit split) {
        this.split = split;
    }

    /**
     * Casts this split state into a {@link TiDBSnapshotSplitState}.
     */
    public final TiDBSnapshotSplitState asSnapshotSplitState() {
        return (TiDBSnapshotSplitState) this;
    }

    /**
     * Casts this split state into a {@link TiDBBinlogSplitState}.
     */
    public final TiDBBinlogSplitState asBinlogSplitState() {
        return (TiDBBinlogSplitState) this;
    }

    /**
     * Use the current split state to create a new TiDBSplit.
     */
    public abstract TiDBSplit toTiDBSplit();
}
