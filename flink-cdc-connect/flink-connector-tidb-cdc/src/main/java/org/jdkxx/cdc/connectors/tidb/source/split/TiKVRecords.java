package org.jdkxx.cdc.connectors.tidb.source.split;

import lombok.Getter;
import org.tikv.common.key.Key;

import java.util.ArrayList;
import java.util.List;

import static org.tikv.kvproto.Cdcpb.Event.Row;
import static org.tikv.kvproto.Kvrpcpb.KvPair;

@Getter
public final class TiKVRecords {
    private final List<KvPair> kvPairs;
    private final List<Row> rows;
    private long position;
    private Key nextKey;
    private Key endKey;
    private Type type;

    public TiKVRecords() {
        this.kvPairs = new ArrayList<>();
        this.rows = new ArrayList<>();
    }

    TiKVRecords(Type type) {
        this();
        this.type = type;
    }

    TiKVRecords(long position, Key nextKey, Type type) {
        this(type);
        this.position = position;
        this.nextKey = nextKey;
    }

    TiKVRecords(long position, Key nextKey, Key endKey, Type type) {
        this(position, nextKey, type);
        this.endKey = endKey;
    }

    public boolean isSnapshotEvent() {
        return type == Type.Snapshot_Event;
    }

    public boolean isChangeEvent() {
        return type == Type.Change_Event;
    }

    void addRows(List<Row> rows) {
        this.rows.addAll(rows);
    }

    void addKvPairs(List<KvPair> kvPairs) {
        this.kvPairs.addAll(kvPairs);
    }

    public static TiKVRecords fromRows(List<Row> rows, long version) {
        TiKVRecords records = new TiKVRecords(version, Key.EMPTY, Type.Change_Event);
        records.addRows(rows);
        return records;
    }

    public static TiKVRecords fromKvPairs(List<KvPair> pairs, long version, Key lastKey, Key endKey) {
        TiKVRecords records = new TiKVRecords(version, lastKey, endKey, Type.Snapshot_Event);
        records.addKvPairs(pairs);
        return records;
    }

    enum Type {
        Change_Event, Snapshot_Event;
    }
}
