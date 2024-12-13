package org.jdkxx.cdc.connectors.tidb.source.reader;

import org.jdkxx.cdc.connectors.tidb.schema.TiTableMetadata;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.jdkxx.cdc.connectors.tidb.source.offset.TiKVOffset;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSnapshotSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiKVRecords;
import org.jdkxx.cdc.connectors.tidb.utils.TableKeyRangeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ListenableFuture;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.MoreExecutors;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.tikv.cdc.reader.TiKVReader;
import org.tikv.common.TiSession;
import org.tikv.common.exception.KeyException;
import org.tikv.common.key.Key;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;

import static org.tikv.kvproto.Coprocessor.KeyRange;
import static org.tikv.kvproto.Kvrpcpb.KvPair;

@Slf4j
public class TiKVSnapshotSplitReader implements TiKVReader<TiKVRecords, TiDBSplit> {
    private static final long READER_CLOSE_TIMEOUT = 30L;
    private final TiDBSourceConfig sourceConfig;
    private final SourceReaderContext context;
    private final TiSession session;
    private final ExecutorService executorService;
    private ListenableFuture<TiKVRecords> future;
    private volatile boolean running;

    public TiKVSnapshotSplitReader(TiDBSourceConfig sourceConfig,
                                   SourceReaderContext context,
                                   TiSession session) {
        this.sourceConfig = sourceConfig;
        this.context = context;
        this.session = session;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder()
                        .setNameFormat("tidb-reader-" + context.getIndexOfSubtask())
                        .build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.running = true;
    }

    @Override
    public boolean isFinished() {
        return future == null || future.isDone();
    }

    @Override
    public void submitSplit(TiDBSplit splitToRead) {
        TiDBSnapshotSplit snapshotSplit = splitToRead.asSnapshotSplit();
        TiDBSnapshotSplitReadTask task =
                new TiDBSnapshotSplitReadTask(session,
                        sourceConfig,
                        context,
                        snapshotSplit);
        future = MoreExecutors.listeningDecorator(executorService).submit(task);
    }

    @Override
    public void close() {
        this.running = false;
        try {
            if (executorService != null) {
                executorService.shutdown();
                if (!executorService.awaitTermination(READER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    log.warn("Failed to close the snapshot split reader in {} seconds.",
                            READER_CLOSE_TIMEOUT);
                }
            }
        } catch (Exception e) {
            log.error("Close snapshot reader error", e);
        }
        future = null;
    }

    @Nullable
    @Override
    public Iterator<TiKVRecords> pollSplitRecords() throws InterruptedException {
        if (this.running) {
            List<TiKVRecords> records = new ArrayList<>();
            try {
                TiKVRecords record = this.future != null ? this.future.get() : null;
                records.add(Objects.requireNonNullElseGet(record, () -> TiKVRecords.fromKvPairs(
                        new ArrayList<>(),
                        session.getTimestamp().getVersion(),
                        Key.EMPTY,
                        Key.EMPTY)));
            } catch (ExecutionException e) {
                throw new InterruptedException(e.getCause().getMessage());
            }
            return records.iterator();
        } else {
            return null;
        }
    }

    private static class TiDBSnapshotSplitReadTask implements Callable<TiKVRecords> {
        private final TiSession session;
        private final TiDBSourceConfig sourceConfig;
        private final SourceReaderContext context;
        private final TiDBSnapshotSplit snapshotSplit;
        private List<KvPair> currentCache;

        TiDBSnapshotSplitReadTask(TiSession session,
                                  TiDBSourceConfig sourceConfig,
                                  SourceReaderContext context,
                                  TiDBSnapshotSplit snapshotSplit) {
            this.session = session;
            this.sourceConfig = sourceConfig;
            this.context = context;
            this.snapshotSplit = snapshotSplit;
        }

        @Override
        public TiKVRecords call() throws Exception {
            TiKVOffset offset = snapshotSplit.getOffset();
            if (offset != null) {
                //we get StartKey and EndKey from TiKV
                Key startKey;
                Key endKey;
                if (offset.hasNextKey()) {
                    startKey = Key.toRawKey(offset.getNextKey());
                    endKey = Key.toRawKey(offset.getEndKey());
                } else {
                    TiTableMetadata table = sourceConfig.getSchema()
                            .getTableMetadata(snapshotSplit.getPath()).as(TiTableMetadata.class);
                    KeyRange keyRange = getKeyRange(table);
                    startKey = Key.toRawKey(keyRange.getStart());
                    endKey = Key.toRawKey(keyRange.getEnd());
                }
                long position = getCurrentOffset(offset);
                log.info("current tidb snapshot position is {}", position);
                TiRegion region = loadCurrentRegionToCache(startKey, position);
                if (currentCache == null) {
                    return TiKVRecords.fromKvPairs(List.of(), position, Key.EMPTY, Key.EMPTY);
                }

                Key curRegionEndKey = Key.toRawKey(region.getEndKey());
                Key lastKey;
                if (currentCache.size() < session.getConf().getScanBatchSize()) {
                    startKey = curRegionEndKey;
                    lastKey = curRegionEndKey;
                } else {
                    // Start new scan from exact next key in the current region
                    lastKey = Key.toRawKey(currentCache.getLast().getKey());
                    startKey = lastKey.next();
                }

                // notify last batch if lastKey is greater than or equal to endKey
                // if startKey is empty, it indicates +∞
                if (lastKey.compareTo(endKey) >= 0 || startKey.toByteString().isEmpty()) {
                    return TiKVRecords.fromKvPairs(currentCache, position, Key.EMPTY, Key.EMPTY);
                }

                List<KvPair> pairs = new ArrayList<>();
                for (KvPair current : currentCache) {
                    if (current.hasError()) {
                        ByteString val = resolveCurrentLock(current, position);
                        current = Kvrpcpb.KvPair.newBuilder().setKey(current.getKey()).setValue(val).build();
                    }
                    pairs.add(current);
                }
                return TiKVRecords.fromKvPairs(pairs, position, startKey, endKey);
            }
            return null;
        }

        private KeyRange getKeyRange(TiTableMetadata table) {
            return TableKeyRangeUtils.getTableKeyRange(table,
                    context.currentParallelism(),
                    context.getIndexOfSubtask());
        }

        public long getCurrentOffset(TiKVOffset offset) {
            return offset.hasPosition() ? offset.getPosition() : session.getTimestamp().getVersion();
        }

        private TiRegion loadCurrentRegionToCache(Key startKey, long position) {
            ByteString key = startKey.toByteString();
            TiRegion region;
            try (RegionStoreClient client = session.getRegionStoreClientBuilder().build(key)) {
                client.setTimeout(session.getConf().getScanTimeout());
                region = client.getRegion();
                BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
                currentCache = client.scan(backOffer, key, position);
                log.info("read snapshot segment size is {}", currentCache.size());
                return region;
            }
        }

        private ByteString resolveCurrentLock(KvPair current, long position) {
            RegionStoreClient.RegionStoreClientBuilder builder = session.getRegionStoreClientBuilder();
            log.warn("resolve current key error {}", current.getError().toString());
            Pair<TiRegion, TiStore> pair =
                    builder.getRegionManager().getRegionStorePairByKey(current.getKey());
            TiRegion region = pair.first;
            TiStore store = pair.second;
            BackOffer backOffer =
                    ConcreteBackOffer.newGetBackOff(builder.getRegionManager().getPDClient().getClusterId());
            try (RegionStoreClient client = builder.build(region, store)) {
                return client.get(backOffer, current.getKey(), position);
            } catch (Exception e) {
                throw new KeyException(current.getError());
            }
        }
    }
}
