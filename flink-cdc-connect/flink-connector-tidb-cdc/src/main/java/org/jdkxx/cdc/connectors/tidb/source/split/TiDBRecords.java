package org.jdkxx.cdc.connectors.tidb.source.split;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class TiDBRecords implements RecordsWithSplitIds<TiKVRecords> {
    @Nullable
    private String splitId;
    @Nullable
    private Iterator<TiKVRecords> recordsForCurrentSplit;
    @Nullable
    private final Iterator<TiKVRecords> recordsForSplit;
    private final Set<String> finishedSnapshotSplits;

    TiDBRecords(
            @Nullable String splitId,
            @Nullable Iterator<TiKVRecords> recordsForSplit,
            Set<String> finishedSnapshotSplits) {
        this.splitId = splitId;
        this.recordsForSplit = recordsForSplit;
        this.finishedSnapshotSplits = finishedSnapshotSplits;
    }

    @Nullable
    @Override
    public String nextSplit() {
        // move the split one (from current value to null)
        final String nextSplit = this.splitId;
        this.splitId = null;

        // move the iterator, from null to value (if first move) or to null (if second move)
        this.recordsForCurrentSplit = nextSplit != null ? this.recordsForSplit : null;
        return nextSplit;
    }

    @Nullable
    @Override
    public TiKVRecords nextRecordFromSplit() {
        final Iterator<TiKVRecords> recordsForSplit = this.recordsForCurrentSplit;
        if (recordsForSplit != null) {
            if (recordsForSplit.hasNext()) {
                return recordsForSplit.next();
            } else {
                return null;
            }
        } else {
            throw new IllegalStateException();
        }
    }

    @Override
    public Set<String> finishedSplits() {
        return finishedSnapshotSplits;
    }

    public static TiDBRecords forBinlogRecords(
            final String splitId, final Iterator<TiKVRecords> recordsForSplit) {
        return TiDBRecords.of(splitId, recordsForSplit, Collections.emptySet());
    }

    public static TiDBRecords forSnapshotRecords(
            final String splitId, final Iterator<TiKVRecords> recordsForSplit) {
        return TiDBRecords.of(splitId, recordsForSplit, Collections.singleton(splitId));
    }

    public static TiDBRecords forFinishedSplit(final String splitId) {
        return TiDBRecords.of(null, null, Collections.singleton(splitId));
    }

    public static TiDBRecords of(final String splitId,
                                 Iterator<TiKVRecords> recordsForSplit,
                                 Set<String> finishedSnapshotSplits) {
        return new TiDBRecords(splitId, recordsForSplit, finishedSnapshotSplits);
    }
}
