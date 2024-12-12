package com.lakala.cdc.connectors.tidb.source.enumerator;

import com.lakala.cdc.connectors.tidb.source.assigners.AssignerStatus;
import com.lakala.cdc.connectors.tidb.source.assigners.TiDBHybridSplitAssigner;
import com.lakala.cdc.connectors.tidb.source.assigners.TiDBSplitAssigner;
import com.lakala.cdc.connectors.tidb.source.assigners.state.PendingSplitsState;
import com.lakala.cdc.connectors.tidb.source.events.*;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;
import com.lakala.cdc.connectors.tidb.source.split.TiDBBinlogSplit;
import com.lakala.cdc.connectors.tidb.source.split.TiDBSplit;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

@Slf4j
public class TiDBSourceEnumerator implements SplitEnumerator<TiDBSplit, PendingSplitsState> {
    private static final long CHECK_EVENT_INTERVAL = 30_000L;
    private final SplitEnumeratorContext<TiDBSplit> context;
    // using TreeSet to prefer assigning binlog split to task-0 for easier debug
    private final TreeSet<Integer> readersAwaitingSplit;
    private final TiDBSplitAssigner splitAssigner;
    private final Boundedness boundedness;
    @Nullable
    private Integer binlogSplitTaskId;

    public TiDBSourceEnumerator(
            SplitEnumeratorContext<TiDBSplit> context,
            TiDBSplitAssigner splitAssigner,
            Boundedness boundedness) {
        this.context = context;
        this.splitAssigner = splitAssigner;
        this.readersAwaitingSplit = new TreeSet<>();
        this.boundedness = boundedness;
    }

    @Override
    public void start() {
        this.splitAssigner.open();
        requestBinlogSplitUpdateIfNeed();
        this.context.callAsync(
                this::getRegisteredReader,
                this::syncWithReaders,
                CHECK_EVENT_INTERVAL,
                CHECK_EVENT_INTERVAL);
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof FinishedSnapshotSplitReportEvent event) {
            log.info("The enumerator under {} receives finished split offsets {} from subtask {}.",
                    splitAssigner.getAssignerStatus(),
                    sourceEvent,
                    subtaskId);
            Map<String, TiKVOffset> finishedOffsets = event.offsets();

            splitAssigner.onFinishedSplits(finishedOffsets);

            requestBinlogSplitUpdateIfNeed();

            // send acknowledge event
            FinishedSnapshotSplitAckEvent ackEvent =
                    new FinishedSnapshotSplitAckEvent(new ArrayList<>(finishedOffsets.keySet()));
            context.sendEventToSourceReader(subtaskId, ackEvent);
        } else if (sourceEvent instanceof BinlogSplitAssignedEvent) {
            log.info("The enumerator receives notice from subtask {} for the binlog split assignment. ",
                    subtaskId);
            binlogSplitTaskId = subtaskId;
        } else if (sourceEvent instanceof LatestFinishedSplitsRequestEvent) {
            log.info("The enumerator receives request from subtask {} for the latest finished splits number after added newly tables. ",
                    subtaskId);
            handleLatestFinishedSplitNumberRequest(subtaskId);
        } else if (sourceEvent instanceof BinlogSplitUpdateAckEvent) {
            log.info("The enumerator receives event that the binlog split has been updated from subtask {}. ",
                    subtaskId);
            splitAssigner.onBinlogSplitUpdated();
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!context.registeredReaders().containsKey(subtaskId)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        readersAwaitingSplit.add(subtaskId);
        assignSplits();
    }

    @Override
    public void addSplitsBack(List<TiDBSplit> splits, int subtaskId) {
        log.debug("The enumerator adds splits back: {}", splits);
        Optional<TiDBSplit> binlogSplit =
                splits.stream().filter(TiDBSplit::isBinlogSplit).findAny();
        if (binlogSplit.isPresent()) {
            log.info("The enumerator adds add binlog split back: {}", binlogSplit);
            this.binlogSplitTaskId = null;
        }
        splitAssigner.addSplits(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // send BinlogSplitUpdateRequestEvent to source reader after newly added table
        // snapshot splits finished.
        if (AssignerStatus.isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
        }
    }

    @Override
    public PendingSplitsState snapshotState(long checkpointId) throws Exception {
        return this.splitAssigner.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        this.splitAssigner.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void close() throws IOException {
        if (this.splitAssigner != null) {
            try {
                this.splitAssigner.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }

    private void assignSplits() {
        final Iterator<Integer> awaitingReader = readersAwaitingSplit.iterator();

        while (awaitingReader.hasNext()) {
            int nextAwaiting = awaitingReader.next();
            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting)) {
                awaitingReader.remove();
                continue;
            }

            if (shouldCloseIdleReader(nextAwaiting)) {
                // close idle readers when snapshot phase finished.
                context.signalNoMoreSplits(nextAwaiting);
                awaitingReader.remove();
                log.info("Close idle reader of subtask {}", nextAwaiting);
                continue;
            }

            Optional<TiDBSplit> optional = splitAssigner.getNext();
            if (optional.isPresent()) {
                final TiDBSplit split = optional.get();
                context.assignSplit(split, nextAwaiting);
                if (split instanceof TiDBBinlogSplit) {
                    this.binlogSplitTaskId = nextAwaiting;
                }

                awaitingReader.remove();
                log.info("The enumerator assigns split {} to subtask {}", split, nextAwaiting);
            } else {
                // there is no available splits by now, skip assigning
                requestBinlogSplitUpdateIfNeed();
                break;
            }
        }
    }

    private boolean shouldCloseIdleReader(int nextAwaiting) {
        // When no unassigned split anymore, Signal NoMoreSplitsEvent to awaiting reader in two
        // situations:
        // 1. When Set StartupMode = snapshot mode(also bounded), there's no more splits in the
        // assigner.
        // 2. When set scan.incremental.close-idle-reader.enabled = true, there's no more splits in
        // the assigner.
        return splitAssigner.noMoreSplits()
                && boundedness == Boundedness.BOUNDED
                && (binlogSplitTaskId != null
                && !binlogSplitTaskId.equals(nextAwaiting));
    }

    private int[] getRegisteredReader() {
        return this.context.registeredReaders().keySet().stream()
                .mapToInt(Integer::intValue)
                .toArray();
    }

    private void syncWithReaders(int[] subtaskIds, Throwable t) {
        if (t != null) {
            throw new FlinkRuntimeException("Failed to list obtain registered readers due to:", t);
        }
        // when the SourceEnumerator restores or the communication failed between
        // SourceEnumerator and SourceReader, it may miss some notification event.
        // tell all SourceReader(s) to report there finished but acknowledged splits.
        if (splitAssigner.waitingForFinishedSplits()) {
            for (int subtaskId : subtaskIds) {
                context.sendEventToSourceReader(subtaskId, new FinishedSnapshotSplitsRequestEvent());
            }
        }
        requestBinlogSplitUpdateIfNeed();
    }

    private void requestBinlogSplitUpdateIfNeed() {
        if (AssignerStatus.isNewlyAddedAssigningSnapshotFinished(splitAssigner.getAssignerStatus())) {
            for (int subtaskId : getRegisteredReader()) {
                log.info("The enumerator requests subtask {} to update the binlog split after newly added table.",
                        subtaskId);
                context.sendEventToSourceReader(subtaskId, new BinlogSplitUpdateRequestEvent());
            }
        }
    }

    private void handleLatestFinishedSplitNumberRequest(int subTask) {
        if (splitAssigner instanceof TiDBHybridSplitAssigner) {
            context.sendEventToSourceReader(
                    subTask,
                    new LatestFinishedSplitsEvent());
        }
    }
}
