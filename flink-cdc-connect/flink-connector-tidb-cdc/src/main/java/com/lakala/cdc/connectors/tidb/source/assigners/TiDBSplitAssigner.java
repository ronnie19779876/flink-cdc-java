package com.lakala.cdc.connectors.tidb.source.assigners;

import org.apache.flink.api.common.state.CheckpointListener;
import com.lakala.cdc.connectors.tidb.source.assigners.state.PendingSplitsState;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;
import com.lakala.cdc.connectors.tidb.source.split.TiDBSplit;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

public interface TiDBSplitAssigner {
    /**
     * Called to open the assigner to acquire any resources, like threads or network connections.
     */
    void open();

    /**
     * Gets the next split.
     *
     * <p>When this method returns an empty {@code Optional}, then the set of splits is assumed to
     * be done and the source will finish once the readers finished their current splits.
     */
    Optional<TiDBSplit> getNext();

    /**
     * Called to close the assigner, in case it holds on to any resources, like threads or network
     * connections.
     */
    void close() throws Exception;

    /**
     * Callback to handle the finished splits with finished binlog offset. This is useful for
     * determine when to generate binlog split and what binlog split to generate.
     */
    void onFinishedSplits(Map<String, TiKVOffset> splitFinishedOffsets);

    /**
     * Indicates there is no more splits available in this assigner.
     */
    boolean noMoreSplits();

    /**
     * Creates a snapshot of the state of this split assigner, to be stored in a checkpoint.
     *
     * <p>The snapshot should contain the latest state of the assigner: It should assume that all
     * operations that happened before the snapshot have successfully completed. For example all
     * splits assigned to readers via {@link #getNext()} don't need to be included in the snapshot
     * anymore.
     *
     * <p>This method takes the ID of the checkpoint for which the state is snapshot. Most
     * implementations should be able to ignore this parameter, because for the contents of the
     * snapshot, it doesn't matter for which checkpoint it gets created. This parameter can be
     * interesting for source connectors with external systems where those systems are themselves
     * aware of checkpoints; for example in cases where the enumerator notifies that system about a
     * specific checkpoint being triggered.
     *
     * @param checkpointId The ID of the checkpoint for which the snapshot is created.
     * @return an object containing the state of the split enumerator.
     */
    PendingSplitsState snapshotState(long checkpointId);

    /**
     * Notifies the listener that the checkpoint with the given {@code checkpointId} completed and
     * was committed.
     *
     * @see CheckpointListener#notifyCheckpointComplete(long)
     */
    void notifyCheckpointComplete(long checkpointId);

    /**
     * Adds a set of splits to this assigner. This happens for example when some split processing
     * failed and the splits need to be re-added.
     */
    void addSplits(Collection<TiDBSplit> splits);

    /**
     * Whether the split assigner is still waiting for callback of finished splits, i.e. {@link
     * #onFinishedSplits(Map)}.
     */
    boolean waitingForFinishedSplits();

    /**
     * Gets the split assigner status, see {@code AssignerStatus}.
     */
    AssignerStatus getAssignerStatus();

    /**
     * Starts assign newly added tables.
     */
    void startAssignNewlyAddedTables();

    /**
     * Callback to handle the binlog split has been updated in the newly added tables process. This
     * is useful to check the newly added tables has been finished or not.
     */
    void onBinlogSplitUpdated();
}
