package com.lakala.cdc.connectors.tidb.source.split;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;

import javax.annotation.Nullable;

@Getter
@Setter
@ToString
public class TiDBSnapshotSplitState extends TiDBSplitState {
    @Nullable
    private TiKVOffset offset;

    public TiDBSnapshotSplitState(TiDBSplit split) {
        super(split);
    }

    @Override
    public TiDBSplit toTiDBSplit() {
        final TiDBSnapshotSplit snapshotSplit = split.asSnapshotSplit();
        return new TiDBSnapshotSplit(
                snapshotSplit.splitId,
                snapshotSplit.getPath(),
                getOffset());
    }
}
