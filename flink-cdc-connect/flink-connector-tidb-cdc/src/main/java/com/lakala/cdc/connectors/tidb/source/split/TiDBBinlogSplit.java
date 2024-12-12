package com.lakala.cdc.connectors.tidb.source.split;

import lombok.Getter;
import lombok.Setter;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;

import javax.annotation.Nullable;

@Getter
public class TiDBBinlogSplit extends TiDBSplit {
    @Nullable
    @Setter
    private TiKVOffset offset;
    private final boolean isSuspended;

    public TiDBBinlogSplit(String splitId) {
        this(splitId, false);
    }

    public TiDBBinlogSplit(String splitId, boolean isSuspended) {
        super(splitId);
        this.isSuspended = isSuspended;
    }

    public TiDBBinlogSplit(String splitId, TiKVOffset offset, boolean isSuspended) {
        super(splitId);
        this.offset = offset;
        this.isSuspended = isSuspended;
    }

    public TiDBBinlogSplit(String splitId, @Nullable TiKVOffset offset) {
        this(splitId);
        this.offset = offset;
    }

    public static TiDBBinlogSplit toSuspendedBinlogSplit(TiDBBinlogSplit normalBinlogSplit) {
        return new TiDBBinlogSplit(
                normalBinlogSplit.splitId,
                normalBinlogSplit.getOffset(),
                true);
    }

    public static TiDBBinlogSplit toNormalBinlogSplit(TiDBBinlogSplit suspendedBinlogSplit) {
        return new TiDBBinlogSplit(
                suspendedBinlogSplit.splitId,
                suspendedBinlogSplit.getOffset(),
                false);
    }

    @Override
    public String toString() {
        return "TiDBBinlogSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", offset="
                + offset
                + ", isSuspended="
                + isSuspended
                + '}';
    }
}
