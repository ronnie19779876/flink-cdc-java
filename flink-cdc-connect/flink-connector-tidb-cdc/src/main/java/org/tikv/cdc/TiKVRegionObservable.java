package org.tikv.cdc;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet;
import org.apache.flink.util.Preconditions;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.ChangeDataRequest;
import org.tikv.kvproto.Cdcpb.Event;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Cdcpb.Header;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

@Slf4j
public class TiKVRegionObservable implements AutoCloseable, StreamObserver<ChangeDataEvent> {
    private static final AtomicLong REQ_ID_COUNTER = new AtomicLong(0);
    private static final Set<Event.LogType> ALLOWED_LOGTYPE = ImmutableSet.of(
            Event.LogType.PREWRITE,
            Event.LogType.COMMIT,
            Event.LogType.COMMITTED,
            Event.LogType.ROLLBACK);

    @Getter
    private final TiRegion region;
    private final KeyRange keyRange;
    private final KeyRange regionKeyRange;
    private final CDCConfig config;
    private final ChangeDataGrpc.ChangeDataStub asyncStub;
    private final Predicate<Row> filter;
    private ChangeEventObserver observer;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private StreamObserver<ChangeDataRequest> requestObserver;
    private long resolvedTs;

    public TiKVRegionObservable(TiRegion region,
                                ManagedChannel channel,
                                KeyRange keyRange,
                                CDCConfig config) {
        this.region = region;
        this.keyRange = keyRange;
        this.config = config;
        this.asyncStub = ChangeDataGrpc.newStub(channel);
        this.regionKeyRange = KeyRange.newBuilder()
                .setStart(region.getStartKey())
                .setEnd(region.getEndKey())
                .build();
        this.filter =
                regionEnclosed()
                        ? ((row) -> true)
                        : new Predicate<>() {
                    final byte[] buffer = new byte[config.getMaxRowKeySize()];

                    final byte[] start = keyRange.getStart().toByteArray();
                    final byte[] end = keyRange.getEnd().toByteArray();

                    @Override
                    public boolean test(final Row row) {
                        final int len = row.getKey().size();
                        row.getKey().copyTo(buffer, 0);
                        return (FastByteComparisons.compareTo(
                                buffer, 0, len, start, 0, start.length)
                                >= 0)
                                && (FastByteComparisons.compareTo(
                                buffer, 0, len, end, 0, end.length)
                                < 0);
                    }
                };
    }

    public synchronized void registerObserver(ChangeEventObserver observer) {
        if (observer == null)
            throw new NullPointerException();
        this.observer = observer;
    }

    public synchronized void start(final long version) {
        Preconditions.checkState(!running.get(), "RegionCDCClient has already started");
        running.set(true);
        this.resolvedTs = version;
        log.debug("start streaming region: {}, running: {}", region.getId(), running.get());
        final ChangeDataRequest request = ChangeDataRequest.newBuilder()
                .setRequestId(REQ_ID_COUNTER.incrementAndGet())
                .setHeader(Header.newBuilder().setTicdcVersion("5.0.0").build())
                .setRegionId(region.getId())
                .setCheckpointTs(version)
                .setStartKey(keyRange.getStart())
                .setEndKey(keyRange.getEnd())
                .setRegionEpoch(region.getRegionEpoch())
                .setExtraOp(config.getExtraOp())
                .build();
        requestObserver = asyncStub.eventFeed(this);
        requestObserver.onNext(request);
    }

    @Override
    public void close() throws Exception {
        this.running.set(false);
        if (requestObserver != null) {
            requestObserver.onCompleted();
        }
    }

    @Override
    public void onNext(ChangeDataEvent changeDataEvent) {
        try {
            if (running.get()) {
                // fix: miss-to-process error event
                onErrorEventHandle(changeDataEvent);
                changeDataEvent.getEventsList().stream()
                        .flatMap(ev -> ev.getEntries().getEntriesList().stream())
                        .filter(row -> ALLOWED_LOGTYPE.contains(row.getType()))
                        .filter(this.filter)
                        .map(row -> CDCEvent.rowEvent(region.getId(), row))
                        .forEach(event -> this.observer.onChangeEvent(event));

                if (changeDataEvent.hasResolvedTs()) {
                    final Cdcpb.ResolvedTs resolvedTs = changeDataEvent.getResolvedTs();
                    this.resolvedTs = resolvedTs.getTs();
                    if (resolvedTs.getRegionsList().contains(region.getId())) {
                        this.observer.onChangeEvent(CDCEvent.resolvedTsEvent(region.getId(), resolvedTs.getTs()));
                    }
                }
            }
        } catch (final Exception e) {
            onError(e, resolvedTs);
        }
    }

    @Override
    public void onError(Throwable throwable) {
        log.error("TiKVRegionObservable onError. region: {}", region.getId(), throwable);
        onError(throwable, this.resolvedTs);
    }

    @Override
    public void onCompleted() {
        //log.info("TiKVRegionObservable has completed. region: {}", region.getId());
    }

    private boolean regionEnclosed() {
        return KeyRangeUtils.makeRange(keyRange.getStart(), keyRange.getEnd())
                .encloses(
                        KeyRangeUtils.makeRange(
                                regionKeyRange.getStart(), regionKeyRange.getEnd()));
    }

    private void onError(final Throwable error, long resolvedTs) {
        log.error("region CDC error: region: {}, resolvedTs:{}",
                region.getId(),
                resolvedTs,
                error);
        running.set(false);
        this.observer.onChangeEvent(CDCEvent.error(region.getId(), error, resolvedTs));
    }

    private void onErrorEventHandle(final ChangeDataEvent event) {
        List<Event> errorEvents =
                event.getEventsList().stream()
                        .filter(Event::hasError)
                        .toList();
        if (!errorEvents.isEmpty()) {
            onError(new RuntimeException("regionCDC error:" + errorEvents.getFirst().getError().toString()),
                    this.resolvedTs);
        }
    }
}
