package org.jdkxx.cdc.connectors.tidb.source.split;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jdkxx.cdc.connectors.tidb.source.offset.TiKVOffset;

import javax.annotation.Nullable;

@Getter
@Setter
@ToString
public class TiDBBinlogSplitState extends TiDBSplitState {
    @Nullable
    private TiKVOffset offset;

    public TiDBBinlogSplitState(TiDBSplit split) {
        super(split);
    }

    @Override
    public TiDBSplit toTiDBSplit() {
        TiDBBinlogSplit binlogSplit = split.asBinlogSplit();
        return new TiDBBinlogSplit(
                binlogSplit.splitId,
                getOffset());
    }
}
