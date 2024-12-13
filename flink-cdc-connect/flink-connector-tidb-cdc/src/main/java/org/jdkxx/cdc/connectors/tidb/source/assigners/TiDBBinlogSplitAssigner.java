package org.jdkxx.cdc.connectors.tidb.source.assigners;

import org.jdkxx.cdc.connectors.tidb.source.assigners.state.BinlogPendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.PendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.jdkxx.cdc.connectors.tidb.source.offset.TiKVOffset;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBBinlogSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplit;
import org.apache.flink.util.CollectionUtil;
import org.tikv.common.key.Key;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceOptions.BINLOG_SPLIT_ID;

/**
 * A {@link TiDBBinlogSplitAssigner} which only read binlog from current binlog position.
 */
public class TiDBBinlogSplitAssigner implements TiDBSplitAssigner {
    private boolean isBinlogSplitAssigned;
    private final TiDBSourceConfig sourceConfig;

    public TiDBBinlogSplitAssigner(TiDBSourceConfig sourceConfig) {
        this(sourceConfig, false);

    }

    public TiDBBinlogSplitAssigner(TiDBSourceConfig sourceConfig, BinlogPendingSplitsState checkpoint) {
        this(sourceConfig, checkpoint.isBinlogSplitAssigned());

    }

    private TiDBBinlogSplitAssigner(TiDBSourceConfig sourceConfig, boolean isBinlogSplitAssigned) {
        this.isBinlogSplitAssigned = isBinlogSplitAssigned;
        this.sourceConfig = sourceConfig;
    }

    @Override
    public void open() {
    }

    @Override
    public Optional<TiDBSplit> getNext() {
        if (isBinlogSplitAssigned) {
            return Optional.empty();
        } else {
            isBinlogSplitAssigned = true;
            return Optional.of(createBinlogSplit());
        }
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void onFinishedSplits(Map<String, TiKVOffset> splitFinishedOffsets) {
        //do nothing...
    }

    @Override
    public void addSplits(Collection<TiDBSplit> splits) {
        if (!CollectionUtil.isNullOrEmpty(splits)) {
            // we don't store the split, but will re-create binlog split later
            isBinlogSplitAssigned = false;
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return false;
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return AssignerStatus.INITIAL_ASSIGNING_FINISHED;
    }

    @Override
    public void startAssignNewlyAddedTables() {
    }

    @Override
    public void onBinlogSplitUpdated() {
    }

    @Override
    public boolean noMoreSplits() {
        return isBinlogSplitAssigned;
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new BinlogPendingSplitsState(isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        //nothing to do.
    }

    private TiDBBinlogSplit createBinlogSplit() {
        Long position = sourceConfig.getStartupOptions().startupTimestampMillis;
        position = position != null ? position : 0L;
        TiKVOffset offset = TiKVOffset.builder()
                .position(position)
                .nextKey(Key.EMPTY)
                .endKey(Key.EMPTY)
                .build();
        return new TiDBBinlogSplit(BINLOG_SPLIT_ID, offset);
    }
}
