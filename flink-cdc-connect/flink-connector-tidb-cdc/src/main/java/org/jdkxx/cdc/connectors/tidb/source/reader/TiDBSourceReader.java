package org.jdkxx.cdc.connectors.tidb.source.reader;

import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import com.lakala.cdc.connectors.tidb.source.events.*;
import org.jdkxx.cdc.connectors.tidb.source.events.*;
import org.jdkxx.cdc.connectors.tidb.source.offset.TiKVOffset;
import com.lakala.cdc.connectors.tidb.source.split.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.util.Preconditions;
import org.jdkxx.cdc.connectors.tidb.source.split.*;

import java.util.*;
import java.util.function.Supplier;

import static org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceOptions.BINLOG_SPLIT_ID;

@Slf4j
public class TiDBSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<TiKVRecords, T, TiDBSplit, TiDBSplitState> {
    private final TiDBSourceConfig sourceConfig;
    private final Map<String, TiDBSnapshotSplit> finishedUnackedSplits;
    private final int subtaskId;
    private final TiDBSourceReaderContext tidbSourceReaderContext;
    private volatile TiDBBinlogSplit suspendedBinlogSplit;

    @Override
    public void close() throws Exception {
        super.close();
        tidbSourceReaderContext.close();
    }

    public TiDBSourceReader(
            TiDBSourceConfig sourceConfig,
            Supplier<TiDBSplitReader> splitReaderSupplier,
            RecordEmitter<TiKVRecords, T, TiDBSplitState> recordEmitter,
            Configuration config,
            TiDBSourceReaderContext context) {
        super(new SingleThreadFetcherManager<>(splitReaderSupplier::get, config),
                recordEmitter,
                config,
                context.getSourceReaderContext());
        this.sourceConfig = sourceConfig;
        this.finishedUnackedSplits = new HashMap<>();
        this.subtaskId = context.getSourceReaderContext().getIndexOfSubtask();
        this.tidbSourceReaderContext = context;
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() <= 1) {
            context.sendSplitRequest();
        }
    }

    @Override
    public void addSplits(List<TiDBSplit> splits) {
        List<TiDBSplit> unfinishedSplits = new ArrayList<>();
        for (TiDBSplit split : splits) {
            log.info("Source reader {} adds split {}", subtaskId, split);
            if (split.isSnapshotSplit()) {
                TiDBSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                if (snapshotSplit.isSnapshotReadFinished()) {
                    finishedUnackedSplits.put(snapshotSplit.splitId(), snapshotSplit);
                } else if (sourceConfig.isIncluded(snapshotSplit.getPath())) {
                    unfinishedSplits.add(snapshotSplit);
                } else {
                    log.warn("The subtask {} is skipping split {} because it does not match new table.",
                            subtaskId,
                            split.splitId());
                }
            } else {
                TiDBBinlogSplit binlogSplit = split.asBinlogSplit();
                tidbSourceReaderContext.setHasAssignedBinlogSplit(true);
                if (binlogSplit.isSuspended()) {
                    suspendedBinlogSplit = binlogSplit;
                } else {
                    unfinishedSplits.add(binlogSplit);
                }
                log.info("Source reader {} received the binlog split : {}.", subtaskId, binlogSplit);
                context.sendSourceEventToCoordinator(new BinlogSplitAssignedEvent());
            }
        }

        reportFinishedSnapshotSplitsIfNeed();

        if (!unfinishedSplits.isEmpty()) {
            super.addSplits(unfinishedSplits);
        } else if (suspendedBinlogSplit != null || getNumberOfCurrentlyAssignedSplits() <= 1) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, TiDBSplitState> finishedSplitIds) {
        boolean requestNextSplit = true;

        if (isNewlyAddedTableSplitAndBinlogSplit(finishedSplitIds)) {
            TiDBSplitState binlogSplitState = finishedSplitIds.remove(BINLOG_SPLIT_ID);
            finishedSplitIds
                    .values()
                    .forEach(
                            newAddedSplitState ->
                                    finishedUnackedSplits.put(
                                            newAddedSplitState.toTiDBSplit().splitId(),
                                            newAddedSplitState.toTiDBSplit().asSnapshotSplit()));
            Preconditions.checkState(finishedSplitIds.size() == 1);
            log.info("Source reader {} finished binlog split and snapshot split {}",
                    subtaskId,
                    finishedSplitIds.values().iterator().next().toTiDBSplit().splitId());
            this.addSplits(Collections.singletonList(binlogSplitState.toTiDBSplit()));
        } else {
            Preconditions.checkState(finishedSplitIds.size() == 1);
            for (TiDBSplitState splitState : finishedSplitIds.values()) {
                TiDBSplit split = splitState.toTiDBSplit();
                if (split.isBinlogSplit()) {
                    if (tidbSourceReaderContext.isBinlogSplitReaderSuspended()) {
                        suspendedBinlogSplit = TiDBBinlogSplit.toSuspendedBinlogSplit(split.asBinlogSplit());
                        log.info("Source reader {} suspended binlog split reader success after the newly added table process, current offset {}",
                                subtaskId,
                                suspendedBinlogSplit.getOffset());
                        context.sendSourceEventToCoordinator(new LatestFinishedSplitsRequestEvent());
                        // do not request next split when the reader is suspended
                        requestNextSplit = false;
                    }
                } else {
                    finishedUnackedSplits.put(split.splitId(), split.asSnapshotSplit());
                }
            }

            reportFinishedSnapshotSplitsIfNeed();
        }

        if (requestNextSplit) {
            context.sendSplitRequest();
        }
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitAckEvent event) {
            log.debug("Source reader {} receives ack event for {} from enumerator.",
                    subtaskId,
                    event.finishedSplits());
            for (String splitId : event.finishedSplits()) {
                this.finishedUnackedSplits.remove(splitId);
            }
        } else if (sourceEvent instanceof FinishedSnapshotSplitsRequestEvent) {
            log.debug("Source reader {} receives request to report finished snapshot splits.",
                    subtaskId);
            reportFinishedSnapshotSplitsIfNeed();
        } else if (sourceEvent instanceof BinlogSplitUpdateRequestEvent) {
            log.info("Source reader {} receives binlog split update event.", subtaskId);
            handleBinlogSplitUpdateRequest();
        } else if (sourceEvent instanceof LatestFinishedSplitsEvent event) {
            updateBinlogSplit(event);
        }
    }

    @Override
    protected TiDBSplitState initializedState(TiDBSplit split) {
        if (split.isSnapshotSplit()) {
            return new TiDBSnapshotSplitState(split.asSnapshotSplit());
        }
        return new TiDBBinlogSplitState(split.asBinlogSplit());
    }

    @Override
    protected TiDBSplit toSplitType(String splitId, TiDBSplitState splitState) {
        return splitState.toTiDBSplit();
    }

    private void reportFinishedSnapshotSplitsIfNeed() {
        if (!finishedUnackedSplits.isEmpty()) {
            final Map<String, TiKVOffset> finishedOffsets = new HashMap<>();
            for (Map.Entry<String, TiDBSnapshotSplit> entry : finishedUnackedSplits.entrySet()) {
                finishedOffsets.put(entry.getKey(), entry.getValue().getOffset());
            }
            FinishedSnapshotSplitReportEvent event = new FinishedSnapshotSplitReportEvent(finishedOffsets);
            context.sendSourceEventToCoordinator(event);
            log.debug("Source reader {} reports offsets of finished snapshot splits {}.",
                    subtaskId,
                    finishedOffsets);
        }
    }

    private void handleBinlogSplitUpdateRequest() {
        tidbSourceReaderContext.suspendBinlogSplitReader();
    }

    private void updateBinlogSplit(LatestFinishedSplitsEvent sourceEvent) {
        if (suspendedBinlogSplit != null) {
            final TiDBBinlogSplit binlogSplit =
                    TiDBBinlogSplit.toNormalBinlogSplit(suspendedBinlogSplit);
            suspendedBinlogSplit = null;
            this.addSplits(Collections.singletonList(binlogSplit));

            context.sendSourceEventToCoordinator(new BinlogSplitUpdateAckEvent());
            log.info("Source reader {} notifies enumerator that binlog split has been updated.",
                    subtaskId);

            tidbSourceReaderContext.wakeupSuspendedBinlogSplitReader();
            log.info("Source reader {} wakes up suspended binlog reader as binlog split has been updated.",
                    subtaskId);
        }
    }

    /**
     * During the newly added table process, for the source reader who holds the binlog split, we
     * return the latest finished snapshot split. Binlog split as well, this design lets us have
     * an opportunity to exchange binlog reading and snapshot reading, we put the binlog split back.
     */
    private boolean isNewlyAddedTableSplitAndBinlogSplit(
            Map<String, TiDBSplitState> finishedSplitIds) {
        return finishedSplitIds.containsKey(BINLOG_SPLIT_ID) && finishedSplitIds.size() == 2;
    }
}
