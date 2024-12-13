package org.jdkxx.cdc.connectors.tidb.source.reader;

import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import com.lakala.cdc.connectors.tidb.source.split.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.jdkxx.cdc.connectors.tidb.source.split.*;
import org.tikv.cdc.reader.TiKVReader;
import org.tikv.common.TiSession;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceOptions.BINLOG_SPLIT_ID;

@Slf4j
public class TiDBSplitReader implements SplitReader<TiKVRecords, TiDBSplit> {
    private final TiDBSourceConfig sourceConfig;
    private final TiDBSourceReaderContext context;
    private final ArrayDeque<TiDBSnapshotSplit> snapshotSplits;
    private final ArrayDeque<TiDBBinlogSplit> binlogSplits;
    @Nullable
    private String currentSplitId;
    @Nullable
    private TiKVReader<TiKVRecords, TiDBSplit> currentReader;
    @Nullable
    private TiKVSnapshotSplitReader reusedSnapshotReader;
    @Nullable
    private TiKVBinlogSplitReader reusedBinlogReader;
    private final TiSession session;

    public TiDBSplitReader(TiDBSourceConfig sourceConfig,
                           TiDBSourceReaderContext context) {
        this.sourceConfig = sourceConfig;
        this.context = context;
        this.snapshotSplits = new ArrayDeque<>();
        this.binlogSplits = new ArrayDeque<>(1);
        this.session = context.getSession();
    }

    @Override
    public RecordsWithSplitIds<TiKVRecords> fetch() throws IOException {
        try {
            suspendBinlogReaderIfNeed();
            return pollSplitRecords();
        } catch (InterruptedException e) {
            log.warn("fetch data failed.", e);
            throw new IOException(e);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TiDBSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }
        for (TiDBSplit split : splitsChanges.splits()) {
            if (split.isSnapshotSplit()) {
                snapshotSplits.add(split.asSnapshotSplit());
            } else {
                binlogSplits.add(split.asBinlogSplit());
            }
        }
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public void close() throws Exception {
        closeSnapshotReader();
        closeBinlogReader();
    }

    private TiDBRecords pollSplitRecords() throws InterruptedException {
        return switch (currentReader) {
            case null -> {
                if (!binlogSplits.isEmpty()) {
                    TiDBSplit nextSplit = binlogSplits.poll();
                    currentSplitId = nextSplit.splitId();
                    currentReader = getBinlogSplitReader();
                    currentReader.submitSplit(nextSplit);
                } else if (!this.snapshotSplits.isEmpty()) {
                    TiDBSnapshotSplit nextSplit = this.snapshotSplits.poll();
                    currentSplitId = nextSplit.splitId();
                    currentReader = getSnapshotSplitReader();
                    currentReader.submitSplit(nextSplit);
                } else {
                    log.info("No available split to read.");
                }
                Iterator<TiKVRecords> dataIt = currentReader.pollSplitRecords();
                yield dataIt == null ? finishedSplit() : forRecords(dataIt);
            }
            case TiKVSnapshotSplitReader ignored -> {
                Iterator<TiKVRecords> dataIt = currentReader.pollSplitRecords();
                if (dataIt != null) {
                    TiDBRecords records;
                    if (context.isHasAssignedBinlogSplit()) {
                        records = forNewAddedTableFinishedSplit(currentSplitId, dataIt);
                        closeSnapshotReader();
                        closeBinlogReader();
                    } else {
                        records = forRecords(dataIt);
                        TiDBSplit nextSplit = snapshotSplits.poll();
                        if (nextSplit != null) {
                            currentSplitId = nextSplit.splitId();
                            currentReader.submitSplit(nextSplit);
                        } else {
                            closeSnapshotReader();
                        }
                    }
                    yield records;
                } else {
                    yield finishedSplit();
                }
            }
            case TiKVBinlogSplitReader ignored -> {
                Iterator<TiKVRecords> dataIt = currentReader.pollSplitRecords();
                if (dataIt != null) {
                    // try to switch to read snapshot split if there are new added snapshot
                    TiDBSplit nextSplit = snapshotSplits.poll();
                    if (nextSplit != null) {
                        closeBinlogReader();
                        log.info("It's turn to switch next fetch reader to snapshot split reader");
                        currentSplitId = nextSplit.splitId();
                        currentReader = getSnapshotSplitReader();
                        currentReader.submitSplit(nextSplit);
                    }
                    yield TiDBRecords.forBinlogRecords(BINLOG_SPLIT_ID, dataIt);
                } else {
                    // null will be returned after receiving suspend binlog event
                    // finish current binlog split reading
                    closeBinlogReader();
                    yield finishedSplit();
                }
            }
            default -> throw new IllegalStateException("Unsupported reader type.");
        };
    }

    private TiDBRecords finishedSplit() {
        final TiDBRecords finishedRecords = TiDBRecords.forFinishedSplit(currentSplitId);
        currentSplitId = null;
        return finishedRecords;
    }

    /**
     * Suspends binlog reader until updated binlog split join again.
     */
    private void suspendBinlogReaderIfNeed() {
        if (currentReader != null
                && currentReader instanceof TiKVBinlogSplitReader
                && context.isBinlogSplitReaderSuspended()
                && !currentReader.isFinished()) {
            ((TiKVBinlogSplitReader) currentReader).stopBinlogReadTask();
            log.info("Suspend binlog reader to wait the binlog split update.");
        }
    }

    private TiKVBinlogSplitReader getBinlogSplitReader() {
        if (reusedBinlogReader == null) {
            reusedBinlogReader = new TiKVBinlogSplitReader(sourceConfig,
                    context.getSourceReaderContext(),
                    session);
        }
        return reusedBinlogReader;
    }

    private TiKVSnapshotSplitReader getSnapshotSplitReader() {
        if (reusedSnapshotReader == null) {
            reusedSnapshotReader = new TiKVSnapshotSplitReader(sourceConfig,
                    context.getSourceReaderContext(),
                    session);
        }
        return reusedSnapshotReader;
    }

    private TiDBRecords forRecords(Iterator<TiKVRecords> dataIt) {
        if (currentReader instanceof TiKVSnapshotSplitReader) {
            final TiDBRecords finishedRecords = TiDBRecords.forSnapshotRecords(currentSplitId, dataIt);
            closeSnapshotReader();
            return finishedRecords;
        } else {
            return TiDBRecords.forBinlogRecords(currentSplitId, dataIt);
        }
    }

    private void closeSnapshotReader() {
        if (reusedSnapshotReader != null) {
            log.info("Close TiKV snapshot reader {}", reusedSnapshotReader.getClass().getCanonicalName());
            reusedSnapshotReader.close();
            if (reusedSnapshotReader == currentReader) {
                currentReader = null;
            }
            reusedSnapshotReader = null;
        }
    }

    private void closeBinlogReader() {
        if (reusedBinlogReader != null) {
            log.info("Close TiKV binlog reader {}", reusedBinlogReader);
            reusedBinlogReader.close();
            if (reusedBinlogReader == currentReader) {
                currentReader = null;
            }
            reusedBinlogReader = null;
        }
    }

    /**
     * Finishes new added snapshot split, mark the binlog split as finished too, we will add the
     * binlog split back in {@code TiDBSourceReader}.
     */
    private TiDBRecords forNewAddedTableFinishedSplit(
            final String splitId, final Iterator<TiKVRecords> recordsForSplit) {
        final Set<String> finishedSplits = new HashSet<>();
        finishedSplits.add(splitId);
        finishedSplits.add(BINLOG_SPLIT_ID);
        currentSplitId = null;
        return TiDBRecords.of(splitId, recordsForSplit, finishedSplits);
    }
}
