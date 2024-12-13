package org.jdkxx.cdc.connectors.tidb.source.assigners;

import org.jdkxx.cdc.connectors.tidb.schema.TiTableMetadata;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.util.Preconditions;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.SnapshotPendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.jdkxx.cdc.connectors.tidb.source.offset.TiKVOffset;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSnapshotSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplit;
import org.jdkxx.cdc.schema.metadata.table.TablePath;
import org.tikv.common.key.Key;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TiDBSnapshotSplitAssigner implements TiDBSplitAssigner {
    private final TiDBSourceConfig sourceConfig;
    private final List<TablePath> remainingTables;
    private final List<TablePath> alreadyProcessedTables;
    private final Map<String, TiKVOffset> splitFinishedOffsets;
    private AssignerStatus assignerStatus;
    private final AtomicInteger counter = new AtomicInteger(0);
    @Nullable
    private Long checkpointIdToFinish;
    @Getter
    private TiKVOffset lastFinishedOffset;

    TiDBSnapshotSplitAssigner(TiDBSourceConfig sourceConfig,
                              List<TablePath> remainingTables,
                              List<TablePath> alreadyProcessedTables,
                              Map<String, TiKVOffset> splitFinishedOffsets,
                              AssignerStatus assignerStatus) {
        this.sourceConfig = sourceConfig;
        this.remainingTables = remainingTables;
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.splitFinishedOffsets = splitFinishedOffsets;
        this.assignerStatus = assignerStatus;
    }

    public TiDBSnapshotSplitAssigner(TiDBSourceConfig sourceConfig) {
        this(sourceConfig,
                new ArrayList<>(),
                new ArrayList<>(),
                new HashMap<>(),
                AssignerStatus.INITIAL_ASSIGNING);
    }

    public TiDBSnapshotSplitAssigner(
            TiDBSourceConfig sourceConfig,
            SnapshotPendingSplitsState checkpoint) {
        this(sourceConfig,
                new ArrayList<>(),
                checkpoint.getAlreadyProcessedTables(),
                new HashMap<>(),
                checkpoint.getAssignerStatus());
    }

    @Override
    public void open() {
        discoveryCaptureTables();
    }

    @Override
    public Optional<TiDBSplit> getNext() {
        Iterator<TablePath> iterator = this.remainingTables.iterator();
        if (iterator.hasNext()) {
            TablePath path = iterator.next();
            long position = 0;
            try {
                TiTableMetadata table = sourceConfig.getSchema().getTableMetadata(path).as(TiTableMetadata.class);
                position = table.getPosition();
            } catch (TableNotExistException ignore) {
            }

            TiKVOffset offset = getSplitFinishedOffset();
            if (offset == null) {
                offset = TiKVOffset.builder()
                        .position(lastFinishedOffset == null ? position : lastFinishedOffset.getPosition())
                        .nextKey(Key.EMPTY)
                        .endKey(Key.EMPTY)
                        .build();
            }
            return Optional.of(new TiDBSnapshotSplit(
                    path.getFullName() + ":" + counter.getAndIncrement(),
                    path,
                    offset));
        }
        return Optional.empty();
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void onFinishedSplits(Map<String, TiKVOffset> splitFinishedOffsets) {
        log.info("Snapshot split finished. offsets: {}", splitFinishedOffsets);
        Iterator<TiKVOffset> iterator = splitFinishedOffsets.values().iterator();
        if (iterator.hasNext()) {
            TiKVOffset offset = iterator.next();
            if (offset.endOfRows()) {
                Iterator<TablePath> it = this.remainingTables.iterator();
                if (it.hasNext()) {
                    TablePath path = it.next();
                    if (this.remainingTables.remove(path)) {
                        alreadyProcessedTables.add(path);
                    }
                }
                this.counter.set(0);
                this.lastFinishedOffset = offset;
            } else {
                this.splitFinishedOffsets.putAll(splitFinishedOffsets);
            }
        }

        if (noMoreSplits()
                && AssignerStatus.isAssigningSnapshotSplits(assignerStatus)) {
            assignerStatus = assignerStatus.onFinish();
        }
    }

    @Override
    public boolean noMoreSplits() {
        return remainingTables.isEmpty();
    }

    @Override
    public SnapshotPendingSplitsState snapshotState(long checkpointId) {
        SnapshotPendingSplitsState state = new SnapshotPendingSplitsState(
                alreadyProcessedTables,
                assignerStatus);
        if (checkpointIdToFinish == null
                && AssignerStatus.isAssigningSnapshotSplits(assignerStatus)
                && noMoreSplits()) {
            checkpointIdToFinish = checkpointId;
        }
        return state;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (checkpointIdToFinish != null
                && AssignerStatus.isAssigningSnapshotSplits(assignerStatus)
                && noMoreSplits()) {
            if (checkpointId >= checkpointIdToFinish) {
                assignerStatus = assignerStatus.onFinish();
            }
            log.info("Snapshot split assigner is turn into finished status.");
        }
    }

    @Override
    public void addSplits(Collection<TiDBSplit> splits) {
        for (TiDBSplit split : splits) {
            splitFinishedOffsets.remove(split.splitId());
        }
    }

    @Override
    public boolean waitingForFinishedSplits() {
        return !noMoreSplits();
    }

    @Override
    public AssignerStatus getAssignerStatus() {
        return assignerStatus;
    }

    @Override
    public void startAssignNewlyAddedTables() {
        Preconditions.checkState(
                AssignerStatus.isAssigningFinished(assignerStatus),
                "Invalid assigner status {}",
                assignerStatus);
        assignerStatus = assignerStatus.startAssignNewlyTables();
    }

    @Override
    public void onBinlogSplitUpdated() {
        Preconditions.checkState(
                AssignerStatus.isNewlyAddedAssigningSnapshotFinished(assignerStatus),
                "Invalid assigner status {}",
                assignerStatus);
        assignerStatus = assignerStatus.onBinlogSplitUpdated();
    }

    private void discoveryCaptureTables() {
        List<TablePath> captureTables = sourceConfig.getTableList().stream()
                .filter(name -> !sourceConfig.getExcludedTables().contains(name))
                .map(name -> new TablePath(sourceConfig.getDatabase(), name))
                .toList();
        List<TablePath> newlyAddedTables = new ArrayList<>(captureTables);
        newlyAddedTables.removeAll(this.alreadyProcessedTables);
        //we need to remove some tables that didn't be captured anymore.
        this.alreadyProcessedTables.removeIf(path -> !captureTables.contains(path));
        if (!newlyAddedTables.isEmpty()) {
            log.info("Found newly added tables, start capture newly added tables process");
            if (AssignerStatus.isAssigningFinished(assignerStatus)) {
                // start the newly added tables process under binlog reading phase
                log.info("Found newly added tables, start capture newly added tables process under binlog reading phase");
                this.startAssignNewlyAddedTables();
            }
        }
        this.remainingTables.addAll(newlyAddedTables);
    }

    private TiKVOffset getSplitFinishedOffset() {
        log.info("obtain split offsets: {}", this.splitFinishedOffsets);
        TiKVOffset offset = null;
        List<String> keys = this.splitFinishedOffsets.keySet().stream().sorted().toList();
        for (String key : keys) {
            offset = this.splitFinishedOffsets.remove(key);
        }
        return offset;
    }
}
