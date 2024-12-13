package org.jdkxx.cdc.connectors.tidb.source.assigners;

import lombok.extern.slf4j.Slf4j;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.HybridPendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.PendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.jdkxx.cdc.connectors.tidb.source.offset.TiKVOffset;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBBinlogSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplit;
import org.tikv.common.key.Key;

import java.util.*;

import static org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceOptions.BINLOG_SPLIT_ID;

@Slf4j
public class TiDBHybridSplitAssigner implements TiDBSplitAssigner {
    private final TiDBSnapshotSplitAssigner snapshotSplitAssigner;
    private boolean isBinlogSplitAssigned;

    public TiDBHybridSplitAssigner(TiDBSourceConfig sourceConfig) {
        this.snapshotSplitAssigner = new TiDBSnapshotSplitAssigner(sourceConfig);
    }

    public TiDBHybridSplitAssigner(TiDBSourceConfig sourceConfig,
                                   HybridPendingSplitsState checkpoint) {
        this.snapshotSplitAssigner = new TiDBSnapshotSplitAssigner(
                sourceConfig,
                checkpoint.getSnapshotSourceSplits());
        this.isBinlogSplitAssigned = checkpoint.isBinlogSplitAssigned();
    }

    @Override
    public void open() {
        snapshotSplitAssigner.open();
    }

    @Override
    public Optional<TiDBSplit> getNext() {
        if (AssignerStatus.isNewlyAddedAssigningSnapshotFinished(getAssignerStatus())) {
            // do not assign split until the adding table process finished
            return Optional.empty();
        }

        if (snapshotSplitAssigner.noMoreSplits()) {
            // binlog split assigning
            if (isBinlogSplitAssigned) {
                // no more splits for the assigner
                return Optional.empty();
            } else if (AssignerStatus.isInitialAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // we need to wait snapshot-assigner to be finished before
                // assigning the binlog split. Otherwise, records emitted from binlog split
                // might be out-of-order in terms of same primary key with snapshot splits.
                isBinlogSplitAssigned = true;
                return Optional.of(createBinlogSplit());
            } else if (AssignerStatus.isNewlyAddedAssigningFinished(
                    snapshotSplitAssigner.getAssignerStatus())) {
                // do not need to create binlog, but send event to wake up the binlog reader
                isBinlogSplitAssigned = true;
                return Optional.empty();
            } else {
                // binlog split is not ready by now
                return Optional.empty();
            }
        } else {
            // snapshot assigner still have remaining splits, assign split from it
            return snapshotSplitAssigner.getNext();
        }
    }

    @Override
    public void close() throws Exception {
        snapshotSplitAssigner.close();
    }

    @Override
    public void onFinishedSplits(Map<String, TiKVOffset> splitFinishedOffsets) {
        snapshotSplitAssigner.onFinishedSplits(splitFinishedOffsets);
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return snapshotSplitAssigner.waitingForFinishedSplits();
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return snapshotSplitAssigner.getAssignerStatus();
    }

    @Override
    public void startAssignNewlyAddedTables() {
        snapshotSplitAssigner.startAssignNewlyAddedTables();
    }

    @Override
    public void onBinlogSplitUpdated() {
        snapshotSplitAssigner.onBinlogSplitUpdated();
    }

    @Override
    public void addSplits(Collection<TiDBSplit> splits) {
        List<TiDBSplit> snapshotSplits = new ArrayList<>();
        for (TiDBSplit split : splits) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split);
            } else {
                // we don't store the split, but will re-create binlog split later
                isBinlogSplitAssigned = false;
            }
        }
        snapshotSplitAssigner.addSplits(snapshotSplits);
    }

    @Override
    public boolean noMoreSplits() {
        return snapshotSplitAssigner.noMoreSplits();
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) {
        return new HybridPendingSplitsState(
                snapshotSplitAssigner.snapshotState(checkpointId), isBinlogSplitAssigned);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotSplitAssigner.notifyCheckpointComplete(checkpointId);
    }

    private TiDBBinlogSplit createBinlogSplit() {
        TiKVOffset offset = snapshotSplitAssigner.getLastFinishedOffset();
        return new TiDBBinlogSplit(BINLOG_SPLIT_ID,
                TiKVOffset.builder()
                        .position(offset == null ? 0 : offset.getPosition())
                        .nextKey(Key.EMPTY)
                        .endKey(Key.EMPTY)
                        .build());
    }
}
