package org.jdkxx.cdc.common.options;

import org.jdkxx.cdc.common.enums.StartupMode;

import java.io.Serializable;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

public final class StartupOptions implements Serializable {
    public final StartupMode startupMode;
    public final String specificOffsetFile;
    public final Integer specificOffsetPos;
    public final Long startupTimestampMillis;

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and
     * continue to read the latest change log.
     */
    public static StartupOptions initial() {
        return new StartupOptions(StartupMode.INITIAL, null, null, null);
    }

    /**
     * Performs an initial snapshot on the monitored database tables upon first startup, and not
     * read the change log anymore .
     */
    public static StartupOptions snapshot() {
        return new StartupOptions(StartupMode.SNAPSHOT, null, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the beginning of the change log. This should be used with care, as it is only valid when the
     * change log is guaranteed to contain the entire history of the database.
     */
    public static StartupOptions earliest() {
        return new StartupOptions(StartupMode.EARLIEST_OFFSET, null, null, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, just read from
     * the end of the change log which means only have the changes since the connector was started.
     */
    public static StartupOptions latest(long startupTimestampMillis) {
        return new StartupOptions(StartupMode.LATEST_OFFSET, null, null, startupTimestampMillis);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read change log from the specified offset.
     */
    public static StartupOptions specificOffset(String specificOffsetFile, int specificOffsetPos) {
        return new StartupOptions(
                StartupMode.SPECIFIC_OFFSETS, specificOffsetFile, specificOffsetPos, null);
    }

    /**
     * Never to perform snapshot on the monitored database tables upon first startup, and directly
     * read change log from the specified timestamp.
     *
     * <p>The consumer will traverse the change log from the beginning and ignore change events
     * whose timestamp is smaller than the specified timestamp.
     *
     * @param startupTimestampMillis timestamp for the startup offsets, as milliseconds from epoch.
     */
    public static StartupOptions timestamp(long startupTimestampMillis) {
        return new StartupOptions(StartupMode.TIMESTAMP, null, null, startupTimestampMillis);
    }

    private StartupOptions(
            StartupMode startupMode,
            String specificOffsetFile,
            Integer specificOffsetPos,
            Long startupTimestampMillis) {
        this.startupMode = startupMode;
        this.specificOffsetFile = specificOffsetFile;
        this.specificOffsetPos = specificOffsetPos;
        this.startupTimestampMillis = startupTimestampMillis;

        switch (startupMode) {
            case INITIAL:
            case SNAPSHOT:
            case EARLIEST_OFFSET:
            case LATEST_OFFSET:
                break;
            case SPECIFIC_OFFSETS:
                checkNotNull(specificOffsetFile, "specificOffsetFile shouldn't be null");
                checkNotNull(specificOffsetPos, "specificOffsetPos shouldn't be null");
                break;
            case TIMESTAMP:
                checkNotNull(startupTimestampMillis, "startupTimestampMillis shouldn't be null");
                break;
            default:
                throw new UnsupportedOperationException(startupMode + " mode is not supported.");
        }
    }

    public boolean isStreamOnly() {
        return startupMode == StartupMode.EARLIEST_OFFSET
                || startupMode == StartupMode.LATEST_OFFSET
                || startupMode == StartupMode.SPECIFIC_OFFSETS
                || startupMode == StartupMode.TIMESTAMP;
    }

    public boolean isSnapshotOnly() {
        return startupMode == StartupMode.SNAPSHOT;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StartupOptions that = (StartupOptions) o;
        return startupMode == that.startupMode
                && Objects.equals(specificOffsetFile, that.specificOffsetFile)
                && Objects.equals(specificOffsetPos, that.specificOffsetPos)
                && Objects.equals(startupTimestampMillis, that.startupTimestampMillis);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startupMode, specificOffsetFile, specificOffsetPos, startupTimestampMillis);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String mode;
        private String file;
        private int position;
        private long timestamp;

        public Builder mode(String mode) {
            this.mode = mode;
            return this;
        }

        public Builder file(String file) {
            this.file = file;
            return this;
        }

        public Builder position(int position) {
            this.position = position;
            return this;
        }


        public Builder timestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public StartupOptions build() {
            return switch (StartupMode.of(this.mode)) {
                case INITIAL -> initial();
                case EARLIEST_OFFSET -> earliest();
                case LATEST_OFFSET -> latest(this.timestamp);
                case SPECIFIC_OFFSETS -> specificOffset(this.file, this.position);
                case TIMESTAMP -> StartupOptions.timestamp(this.timestamp);
                case SNAPSHOT -> StartupOptions.snapshot();
                default -> throw new IllegalArgumentException();
            };
        }
    }
}
