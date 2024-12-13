package org.jdkxx.cdc.connectors.tidb.source.assigners.state;

import org.jdkxx.cdc.connectors.tidb.source.assigners.AssignerStatus;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplit;
import org.jdkxx.cdc.schema.metadata.table.TablePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The {@link SimpleVersionedSerializer Serializer} for the {@link PendingSplitsState} of TiDB CDC
 * source.
 */
@Slf4j
public class PendingSplitsStateSerializer implements SimpleVersionedSerializer<PendingSplitsState> {
    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int SNAPSHOT_PENDING_SPLITS_STATE_FLAG = 1;
    private static final int BINLOG_PENDING_SPLITS_STATE_FLAG = 2;
    private static final int HYBRID_PENDING_SPLITS_STATE_FLAG = 3;

    private final SimpleVersionedSerializer<TiDBSplit> splitSerializer;

    public PendingSplitsStateSerializer(SimpleVersionedSerializer<TiDBSplit> splitSerializer) {
        this.splitSerializer = splitSerializer;
    }

    @Override
    public int getVersion() {
        return this.splitSerializer.getVersion();
    }

    @Override
    public byte[] serialize(PendingSplitsState state) throws IOException {
        // optimization: the splits lazily cache their own serialized form
        if (state.serializedFormCache != null) {
            return state.serializedFormCache;
        }
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        out.writeInt(splitSerializer.getVersion());
        switch (state) {
            case SnapshotPendingSplitsState snapshotState -> {
                out.writeInt(SNAPSHOT_PENDING_SPLITS_STATE_FLAG);
                writeTables(snapshotState.getAlreadyProcessedTables(), out);
                out.writeInt(snapshotState.getAssignerStatus().getStatusCode());
            }
            case BinlogPendingSplitsState binlogState -> {
                out.writeInt(BINLOG_PENDING_SPLITS_STATE_FLAG);
                out.writeBoolean(binlogState.isBinlogSplitAssigned());
            }
            case HybridPendingSplitsState hybridState -> {
                out.writeInt(HYBRID_PENDING_SPLITS_STATE_FLAG);
                writeTables(hybridState.getSnapshotSourceSplits().getAlreadyProcessedTables(), out);
                out.writeInt(hybridState.getSnapshotSourceSplits().getAssignerStatus().getStatusCode());
                out.writeBoolean(hybridState.isBinlogSplitAssigned());
            }
            default -> throw new IOException(
                    "Unsupported to serialize SourceSplitsState class: "
                            + state.getClass().getName());
        }

        final byte[] result = out.getCopyOfBuffer();
        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        state.serializedFormCache = result;
        out.clear();
        return result;
    }


    @Override
    public PendingSplitsState deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        final int splitVersion = in.readInt();
        final int stateKind = in.readInt();
        return switch (stateKind) {
            case SNAPSHOT_PENDING_SPLITS_STATE_FLAG -> deserializeSnapshotSourceSplitsState(in);
            case BINLOG_PENDING_SPLITS_STATE_FLAG -> deserializeBinlogSourceSplitsState(in);
            case HYBRID_PENDING_SPLITS_STATE_FLAG -> deserializeHybridSourceSplitsState(in);
            default -> throw new IOException("Unsupported to deserialize SourceSplitsState kind: " + stateKind
                    + ", splitVersion: " + splitVersion + ", version: " + version);
        };
    }

    private SnapshotPendingSplitsState deserializeSnapshotSourceSplitsState(DataInputDeserializer in) throws IOException {
        List<TablePath> alreadyProcessedTables = readTables(in);
        AssignerStatus assignerStatus = AssignerStatus.fromStatusCode(in.readInt());
        return new SnapshotPendingSplitsState(alreadyProcessedTables, assignerStatus);
    }

    private BinlogPendingSplitsState deserializeBinlogSourceSplitsState(DataInputDeserializer in)
            throws IOException {
        boolean isBinlogSplitAssigned = in.readBoolean();
        return new BinlogPendingSplitsState(isBinlogSplitAssigned);
    }

    private HybridPendingSplitsState deserializeHybridSourceSplitsState(DataInputDeserializer in) throws IOException {
        SnapshotPendingSplitsState snapshotSourceSplitsState =
                deserializeSnapshotSourceSplitsState(in);
        boolean isBinlogSplitAssigned = in.readBoolean();
        return new HybridPendingSplitsState(snapshotSourceSplitsState, isBinlogSplitAssigned);
    }

    private List<TablePath> readTables(DataInputDeserializer in) throws IOException {
        List<TablePath> tables = new ArrayList<>();
        final int size = in.readInt();
        for (int i = 0; i < size; i++) {
            long tableId = in.readLong();
            String fullName = in.readUTF();
            tables.add(TablePath.of(tableId, fullName));
        }
        return tables;
    }

    private void writeTables(Collection<TablePath> tables, DataOutputSerializer out)
            throws IOException {
        final int size = tables.size();
        out.writeInt(size);
        for (TablePath table : tables) {
            out.writeLong(table.getTableId());
            out.writeUTF(table.getFullName());
        }
    }
}
