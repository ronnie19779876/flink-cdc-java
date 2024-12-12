package com.lakala.cdc.connectors.tidb.source.split;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;
import org.jdkxx.cdc.schema.metadata.table.TablePath;

import javax.annotation.Nullable;

@Getter
public class TiDBSnapshotSplit extends TiDBSplit {
    private final TablePath path;
    @Nullable
    private TiKVOffset offset;

    TiDBSnapshotSplit(String splitId, TablePath path) {
        super(splitId);
        this.path = path;
    }

    public TiDBSnapshotSplit(String splitId, TablePath path, @Nullable TiKVOffset offset) {
        this(splitId, path);
        this.offset = offset;
    }

    public boolean isSnapshotReadFinished() {
        return offset != null && offset.endOfRows() && !isRenewSplit();
    }

    private boolean isRenewSplit() {
        String[] tokens = StringUtils.split(splitId, ':');
        if (tokens.length == 2) {
            return NumberUtils.toInt(tokens[1], -1) == 0;
        }
        return false;
    }

    @Override
    public String toString() {
        return "TiDBSnapshotSplit{"
                + "splitId='"
                + splitId
                + '\''
                + ", offset="
                + offset
                + '}';
    }
}
