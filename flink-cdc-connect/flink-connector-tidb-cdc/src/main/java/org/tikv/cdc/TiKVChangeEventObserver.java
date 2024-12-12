package org.tikv.cdc;

import org.apache.flink.shaded.guava31.com.google.common.collect.Range;
import com.lakala.cdc.connectors.tidb.schema.TiTableMetadata;
import com.lakala.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import com.lakala.cdc.connectors.tidb.utils.TableKeyRangeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.util.Preconditions;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.RangeSplitter;
import org.tikv.shade.io.grpc.ManagedChannel;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static org.tikv.kvproto.Coprocessor.KeyRange;

@Slf4j
public class TiKVChangeEventObserver implements ChangeEventObserver {
    private final BlockingQueue<CDCEvent> eventsQueue;
    private final TiDBSourceConfig config;
    private final Map<Long, TiKVRegionObservable> observables = new ConcurrentHashMap<>();
    private final TiSession session;
    private final TiTableMetadata table;
    private volatile boolean started;
    private final TiKVChannelFactory channelFactory;
    private final SourceReaderContext context;

    public TiKVChangeEventObserver(
            TiDBSourceConfig config,
            TiSession session,
            TiTableMetadata table,
            SourceReaderContext context) {
        this.config = config;
        this.session = session;
        this.table = table;
        this.eventsQueue = new LinkedBlockingQueue<>();
        this.context = context;
        this.channelFactory = TiKVChannelFactory.builder().build(this.session);
    }

    @Override
    public void onChangeEvent(CDCEvent event) {
        // try 2 times offer.
        for (int i = 0; i < 2; i++) {
            if (eventsQueue.offer(event)) {
                return;
            }
        }
        // else use put.
        try {
            eventsQueue.put(event);
        } catch (InterruptedException e) {
            log.error("TiKVChangeEventObserver occur exception", e);
        }
    }

    @Override
    public void start(long version) {
        Preconditions.checkState(!started, "Client is already started");
        applyKeyRange(version);
        this.started = true;
    }

    @Override
    public synchronized CDCEvent get() throws Exception {
        final CDCEvent event = this.eventsQueue.poll();
        if (event != null) {
            switch (event.eventType) {
                case ROW:
                case RESOLVED_TS:
                    return event;
                case ERROR:
                    handleError(event.regionId, event.error, event.resolvedTs);
                    break;
            }
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        clear();
    }

    public void handleError(final long regionId, final Throwable error, long resolvedTs) throws Exception {
        //log.warn("handle error: {}, regionId: {}", error, regionId);
        TiKVRegionObservable observable = this.observables.get(regionId);
        if (observable != null) {
            final TiRegion region = observable.getRegion();
            this.session.getRegionManager().onRequestFail(region); // invalidate cache for corresponding region
            clear();
            applyKeyRange(resolvedTs); // reapply the whole keyRange
        }
    }

    private synchronized void applyKeyRange(final long version) {
        final KeyRange keyRange = TableKeyRangeUtils.getTableKeyRange(
                this.table,
                context.currentParallelism(),
                context.getIndexOfSubtask());
        final RangeSplitter splitter = RangeSplitter.newSplitter(this.session.getRegionManager());
        List<TiRegion> regions = splitter.splitRangeByRegion(Collections.singletonList(keyRange)).stream()
                .map(RangeSplitter.RegionTask::getRegion)
                .sorted(Comparator.comparingLong(TiRegion::getId))
                .collect(Collectors.toList());

        addRegions(regions, keyRange, version);
        log.info("keyRange applied. table:{}.{}, regions.size:{}, version:{}",
                this.table.getDatabase().name(),
                this.table.getName(),
                regions.size(),
                version);
    }

    private void addRegions(final List<TiRegion> regions, final KeyRange keyRange, final long version) {
        for (final TiRegion region : regions) {
            if (overlapWithRegion(region, keyRange)) {
                final String address =
                        session.getRegionManager()
                                .getStoreById(region.getLeader().getStoreId())
                                .getStore()
                                .getAddress();
                final ManagedChannel channel = channelFactory.getChannel(address,
                        session.getPDClient().getHostMapping());
                TiKVRegionObservable observable = new TiKVRegionObservable(region,
                        channel,
                        keyRange,
                        new CDCConfig());
                observables.put(region.getId(), observable);
                observable.registerObserver(this);
                observable.start(version);
                log.debug("add region: {}, address:{}, channel:{}", region.getId(), address, channel.isShutdown());
            }
        }
    }

    private synchronized void clear() throws Exception {
        for (TiKVRegionObservable observable : this.observables.values()) {
            observable.close();
        }
        this.observables.clear();
        if (this.channelFactory != null) {
            this.channelFactory.close();
        }
    }

    private boolean overlapWithRegion(final TiRegion region, final KeyRange keyRange) {
        final Range<Key> regionRange =
                Range.closedOpen(
                        Key.toRawKey(region.getStartKey()), Key.toRawKey(region.getEndKey()));
        final Range<Key> clientRange =
                Range.closedOpen(
                        Key.toRawKey(keyRange.getStart()), Key.toRawKey(keyRange.getEnd()));
        final Range<Key> intersection = regionRange.intersection(clientRange);
        return !intersection.isEmpty();
    }
}
