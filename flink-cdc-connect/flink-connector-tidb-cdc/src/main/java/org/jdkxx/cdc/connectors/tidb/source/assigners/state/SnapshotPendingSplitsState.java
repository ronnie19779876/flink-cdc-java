package org.jdkxx.cdc.connectors.tidb.source.assigners.state;

import org.jdkxx.cdc.connectors.tidb.source.assigners.AssignerStatus;
import lombok.Getter;
import org.jdkxx.cdc.schema.metadata.table.TablePath;

import java.util.List;
import java.util.Objects;

/**
 * A {@link PendingSplitsState} for pending snapshot splits.
 */
@Getter
public class SnapshotPendingSplitsState extends PendingSplitsState {
    /**
     * The paths that are no longer in the enumerator checkpoint, but have been processed before and
     * should this be ignored. Relevant only for sources in continuous monitoring mode.
     */
    private final List<TablePath> alreadyProcessedTables;

    /**
     * The {@link AssignerStatus} that indicates the snapshot assigner status.
     */
    private final AssignerStatus assignerStatus;

    public SnapshotPendingSplitsState(List<TablePath> alreadyProcessedTables, AssignerStatus assignerStatus) {
        this.alreadyProcessedTables = alreadyProcessedTables;
        this.assignerStatus = assignerStatus;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SnapshotPendingSplitsState that)) {
            return false;
        }
        return assignerStatus == that.assignerStatus
                && Objects.equals(alreadyProcessedTables, that.alreadyProcessedTables);
    }

    @Override
    public int hashCode() {
        return Objects.hash(assignerStatus, alreadyProcessedTables);
    }
}
