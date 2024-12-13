package org.jdkxx.cdc.connectors.tidb.source.reader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jdkxx.cdc.connectors.tidb.schema.TiDBSchema;
import org.jdkxx.cdc.connectors.tidb.schema.TiTableMetadata;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.jdkxx.cdc.connectors.tidb.source.offset.TiKVOffset;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBBinlogSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiKVRecords;
import org.jdkxx.cdc.connectors.tidb.utils.TableKeyRangeUtils;
import org.tikv.cdc.CDCEvent;
import org.tikv.cdc.ChangeEventObserver;
import org.tikv.cdc.TiKVChangeEventObserver;
import org.tikv.cdc.reader.TiKVReader;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.kvproto.Cdcpb;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.tikv.kvproto.Cdcpb.Event.Row;

@Slf4j
public class TiKVBinlogSplitReader implements TiKVReader<TiKVRecords, TiDBSplit> {
    private static final long READER_CLOSE_TIMEOUT = 30L;
    private final TiDBSourceConfig sourceConfig;
    private final SourceReaderContext context;
    private TiDBBinlogSplit currentBinlogSplit;
    private volatile boolean running;
    private TiDBBinlogSplitReadTask task;
    private final ExecutorService executorService;
    private final TiSession session;
    private final TiKVRowQueue queue;

    public TiKVBinlogSplitReader(TiDBSourceConfig sourceConfig,
                                 SourceReaderContext context,
                                 TiSession session) {
        this.sourceConfig = sourceConfig;
        this.context = context;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("tidb-reader-" + context.getIndexOfSubtask())
                        .build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.session = session;
        this.queue = new TiKVRowQueue();
        this.running = true;
    }

    @Override
    public boolean isFinished() {
        return currentBinlogSplit == null || !running;
    }

    @Override
    public void submitSplit(TiDBSplit splitToRead) {
        currentBinlogSplit = splitToRead.asBinlogSplit();
        task = new TiDBBinlogSplitReadTask(sourceConfig,
                currentBinlogSplit,
                queue,
                context,
                session);
        task.open();
        MoreExecutors.listeningDecorator(executorService).execute(this.task);
    }

    @Override
    public void close() {
        stopBinlogReadTask();
        if (task != null) {
            task.close();
        }
        if (executorService != null) {
            try {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    log.warn("Failed to close the snapshot split reader in {} seconds.", READER_CLOSE_TIMEOUT);
                }
            } catch (Exception e) {
                log.error("Close binlog reader error", e);
            }
        }
    }

    @Nullable
    @Override
    public Iterator<TiKVRecords> pollSplitRecords() throws InterruptedException {
        TiKVOffset offset = this.currentBinlogSplit.getOffset();
        long position = session.getTimestamp().getVersion();
        if (offset != null) {
            position = offset.getPosition();
        }

        List<TiKVRecords> records = new ArrayList<>();
        if (this.running) {
            Row row = queue.poll();
            if (row != null) {
                records.add(TiKVRecords.fromRows(List.of(row), position));
            } else {
                records.add(TiKVRecords.fromRows(Collections.emptyList(), position));
            }
            return records.iterator();
        } else {
            return null;
        }
    }

    public void stopBinlogReadTask() {
        running = false;
    }

    private static class TiDBBinlogSplitReadTask implements Runnable {
        private final static int DEFAULT_RETRY_TIMES = 1000;
        private final TiSession session;
        private final TiDBSourceConfig sourceConfig;
        private final SourceReaderContext context;
        private final TiKVRowQueue queue;
        private final TiDBBinlogSplit binlogSplit;
        private final AtomicBoolean running = new AtomicBoolean(false);
        private final List<ChangeEventObserver> observers = new ArrayList<>(10);
        private long lastResolvedPosition;

        TiDBBinlogSplitReadTask(TiDBSourceConfig sourceConfig,
                                TiDBBinlogSplit binlogSplit,
                                TiKVRowQueue queue,
                                SourceReaderContext context,
                                TiSession session) {
            this.sourceConfig = sourceConfig;
            this.binlogSplit = binlogSplit;
            this.queue = queue;
            this.context = context;
            this.session = session;
        }

        public void open() {
            TiKVOffset offset = binlogSplit.getOffset();
            long position = session.getTimestamp().getVersion();
            if (offset != null && offset.hasPosition()) {
                position = offset.getPosition();
            }

            TiDBSchema schema = this.sourceConfig.getSchema();
            List<TiTableMetadata> tables = schema.getTableMetadataList();
            for (TiTableMetadata table : tables) {
                ChangeEventObserver observer = new TiKVChangeEventObserver(
                        sourceConfig,
                        session,
                        table,
                        context);
                observer.start(position);
                this.observers.add(observer);
            }
            running.set(true);
        }

        @Override
        public void run() {
            while (running.get()) {
                for (int i = 0; i < DEFAULT_RETRY_TIMES; i++) {
                    final Row row = take();
                    if (row == null) {
                        break;
                    }
                    this.queue.put(row);
                }
                this.flush(lastResolvedPosition);
            }
        }

        public synchronized void flush(long position) {
            this.queue.flush(position);
        }

        public synchronized void close() {
            this.flush(lastResolvedPosition);
            running.set(false);
            for (ChangeEventObserver observer : this.observers) {
                try {
                    observer.close();
                } catch (Exception ignore) {
                }
            }
            this.observers.clear();
        }

        private synchronized Row take() {
            for (ChangeEventObserver observer : this.observers) {
                try {
                    final CDCEvent event = observer.get();
                    if (event != null) {
                        this.lastResolvedPosition = event.resolvedTs;
                        switch (event.eventType) {
                            case ROW:
                                return event.row;
                            case RESOLVED_TS:
                                handleResolvedTs(event.regionId);
                                break;
                        }
                    }
                } catch (Exception e) {
                    log.error("Take row error", e);
                }
            }
            return null;
        }

        private void handleResolvedTs(final long regionId) {
            TiKVOffset offset = this.binlogSplit.getOffset();
            if (offset != null) {
                offset.setPosition(lastResolvedPosition);
            } else {
                offset = TiKVOffset.builder()
                        .position(lastResolvedPosition)
                        .nextKey(RowKey.EMPTY)
                        .endKey(RowKey.EMPTY)
                        .build();
                this.binlogSplit.setOffset(offset);
            }
            log.debug("handle resolvedTs: {}, regionId: {}", offset.getPosition(), regionId);
        }
    }

    private static class TiKVRowQueue {
        private final BlockingQueue<Row> committedEvents = new LinkedBlockingQueue<>();
        private final TreeMap<RowKeyWithTs, Row> rewrites = new TreeMap<>();
        private final TreeMap<RowKeyWithTs, Row> commits = new TreeMap<>();

        Row poll() {
            return this.committedEvents.poll();
        }

        void put(final Row row) {
            if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
                // Don't handle an index key for now
                return;
            }
            switch (row.getType()) {
                case COMMITTED:
                    this.rewrites.put(RowKeyWithTs.ofStart(row), row);
                    this.commits.put(RowKeyWithTs.ofCommit(row), row);
                    break;
                case COMMIT:
                    this.commits.put(RowKeyWithTs.ofCommit(row), row);
                    break;
                case PREWRITE:
                    this.rewrites.put(RowKeyWithTs.ofStart(row), row);
                    break;
                case ROLLBACK:
                    this.rewrites.remove(RowKeyWithTs.ofStart(row));
                    break;
                default:
                    log.warn("Unsupported row type:{}", row.getType());
            }
        }

        void flush(long version) {
            while (!this.commits.isEmpty() && this.commits.firstKey().timestamp <= version) {
                final Row commitRow = commits.pollFirstEntry().getValue();
                final Row rewriteRow = rewrites.remove(RowKeyWithTs.ofStart(commitRow));
                if (rewriteRow != null) {
                    Row committedRow = Row.newBuilder()
                            .setOpType(rewriteRow.getOpType())
                            .setKey(rewriteRow.getKey())
                            .setValue(rewriteRow.getValue())
                            .setOldValue(rewriteRow.getOldValue())
                            .setStartTs(commitRow.getStartTs())
                            .setCommitTs(commitRow.getCommitTs())
                            .setType(Cdcpb.Event.LogType.COMMITTED)
                            .build();
                    // if pull cdc event blocks when a region split, cdc event will lose.
                    boolean success = this.committedEvents.offer(committedRow);
                    log.debug("flush committed row, start:{}, end:{}, size:{}, success:{}",
                            commitRow.getStartTs(),
                            commitRow.getCommitTs(),
                            committedEvents.size(),
                            success);
                } else {
                    log.warn("Cannot flush committed row, maybe lose row from transaction. position: {}, size: {}",
                            version,
                            committedEvents.size());
                }
            }
        }

        @Override
        public String toString() {
            return new ToStringBuilder(this).
                    append("committedEvents", committedEvents.size())
                    .append("rewrites", rewrites.size())
                    .append("commits", commits.size())
                    .toString();
        }
    }

    private static class RowKeyWithTs implements Comparable<RowKeyWithTs> {
        private final long timestamp;
        private final RowKey rowKey;

        private RowKeyWithTs(final long timestamp, final RowKey rowKey) {
            this.timestamp = timestamp;
            this.rowKey = rowKey;
        }

        private RowKeyWithTs(final long timestamp, final byte[] key) {
            this(timestamp, RowKey.decode(key));
        }

        @Override
        public int compareTo(final RowKeyWithTs that) {
            int res = Long.compare(this.timestamp, that.timestamp);
            if (res == 0) {
                res = Long.compare(this.rowKey.getTableId(), that.rowKey.getTableId());
            }
            if (res == 0) {
                res = Long.compare(this.rowKey.getHandle(), that.rowKey.getHandle());
            }
            return res;
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.timestamp, this.rowKey.getTableId(), this.rowKey.getHandle());
        }

        @Override
        public boolean equals(final Object thatObj) {
            if (thatObj instanceof RowKeyWithTs that) {
                return this.timestamp == that.timestamp && this.rowKey.equals(that.rowKey);
            }
            return false;
        }

        static RowKeyWithTs ofStart(final Row row) {
            return new RowKeyWithTs(row.getStartTs(), row.getKey().toByteArray());
        }

        static RowKeyWithTs ofCommit(final Row row) {
            return new RowKeyWithTs(row.getCommitTs(), row.getKey().toByteArray());
        }
    }
}
