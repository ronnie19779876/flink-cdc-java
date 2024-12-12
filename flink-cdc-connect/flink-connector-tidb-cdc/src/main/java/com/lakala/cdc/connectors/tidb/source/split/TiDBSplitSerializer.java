package com.lakala.cdc.connectors.tidb.source.split;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffsetSerializer;
import org.jdkxx.cdc.schema.metadata.table.TablePath;

import java.io.IOException;

@Slf4j
public class TiDBSplitSerializer implements SimpleVersionedSerializer<TiDBSplit> {
    public static final TiDBSplitSerializer INSTANCE = new TiDBSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION = 1;
    private static final int SNAPSHOT_SPLIT_FLAG = 1;
    private static final int BINLOG_SPLIT_FLAG = 2;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TiDBSplit split) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        } else {
            final DataOutputSerializer out = SERIALIZER_CACHE.get();
            if (split.isSnapshotSplit()) {
                final TiDBSnapshotSplit snapshotSplit = split.asSnapshotSplit();
                out.writeInt(SNAPSHOT_SPLIT_FLAG);
                out.writeUTF(snapshotSplit.splitId());
                out.writeLong(snapshotSplit.getPath().getTableId());
                out.writeUTF(snapshotSplit.getPath().getFullName());
                out.writeBoolean(snapshotSplit.getOffset() != null);
                if (snapshotSplit.getOffset() != null) {
                    byte[] copy = TiKVOffsetSerializer.INSTANCE.serialize(snapshotSplit.getOffset());
                    out.writeInt(copy.length);
                    out.write(copy);
                }
            } else {
                final TiDBBinlogSplit binlogSplit = split.asBinlogSplit();
                out.writeInt(BINLOG_SPLIT_FLAG);
                out.writeUTF(binlogSplit.splitId());
                out.writeBoolean(binlogSplit.getOffset() != null);
                if (binlogSplit.getOffset() != null) {
                    byte[] copy = TiKVOffsetSerializer.INSTANCE.serialize(binlogSplit.getOffset());
                    out.writeInt(copy.length);
                    out.write(copy);
                }
            }
            final byte[] result = out.getCopyOfBuffer();
            // optimization: cache the serialized from, so we avoid the byte work during repeated
            // serialization
            split.serializedFormCache = result;
            out.clear();
            return result;
        }
    }

    @Override
    public TiDBSplit deserialize(int version, byte[] serialized) throws IOException {
        return switch (version) {
            case 1, 2, 3, 4 -> deserializeSplit(version, serialized);
            default -> throw new IOException("Unknown version: " + version);
        };
    }

    private TiDBSplit deserializeSplit(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        int splitKind = in.readInt();
        String splitId = in.readUTF();
        if (splitKind == SNAPSHOT_SPLIT_FLAG) {
            long tableId = in.readLong();
            String fullName = in.readUTF();
            TiKVOffset offset = TiKVOffset.EMPTY;
            if (in.readBoolean()) {
                int length = in.readInt();
                byte[] copy = new byte[length];
                in.readFully(copy);
                offset = TiKVOffsetSerializer.INSTANCE.deserialize(copy);
            }
            return new TiDBSnapshotSplit(splitId, TablePath.of(tableId, fullName), offset);
        } else if (splitKind == BINLOG_SPLIT_FLAG) {
            TiKVOffset offset = TiKVOffset.EMPTY;
            if (in.readBoolean()) {
                int length = in.readInt();
                byte[] copy = new byte[length];
                in.readFully(copy);
                offset = TiKVOffsetSerializer.INSTANCE.deserialize(copy);
            }
            return new TiDBBinlogSplit(splitId, offset);
        } else {
            throw new IOException("Unknown split kind: " + splitKind);
        }
    }
}
