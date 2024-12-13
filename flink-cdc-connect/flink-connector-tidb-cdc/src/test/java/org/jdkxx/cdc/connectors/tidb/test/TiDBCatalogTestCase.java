package org.jdkxx.cdc.connectors.tidb.test;

import org.jdkxx.cdc.connectors.tidb.schema.TiDBSchema;
import org.jdkxx.cdc.connectors.tidb.schema.TiTableMetadata;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.util.Preconditions;
import org.jdkxx.cdc.schema.metadata.table.TablePath;
import org.tikv.common.TiSession;
import org.tikv.common.exception.KeyException;
import org.tikv.common.key.Key;
import org.tikv.common.key.RowKey;
import org.tikv.common.region.RegionStoreClient;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.BackOffer;
import org.tikv.common.util.ConcreteBackOffer;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class TiDBCatalogTestCase {
    private static List<Kvrpcpb.KvPair> currentCache;
    private static final AtomicLong counter = new AtomicLong(0);
    static Map<String, String> options = new HashMap<>();

    static {
        options.put("tikv.pd.addresses", "10.182.15.5:2379");
        options.put("tikv.grpc.timeout_in_ms", "20000");
        options.put("tikv.grpc.scan_timeout_in_ms", "20000");
        options.put("source.hostname", "10.182.15.5");
        options.put("source.port", "4000");
        options.put("source.username", "cdcsync");
        options.put("source.password", "NDewt@673");
    }

    public static void load(TiSession session, ByteString startKey, ByteString endKey, int index, long position) throws Exception {
        TiRegion region = loadCurrentRegionToCache(session, Key.toRawKey(startKey), position);
        if (currentCache == null) {
            log.warn("scan is over.");
        }
        ByteString curRegionEndKey = region.getEndKey();
        Key lastKey;
        if (currentCache.size() < session.getConf().getScanBatchSize()) {
            startKey = curRegionEndKey;
            lastKey = Key.toRawKey(curRegionEndKey);
        } else {
            // Start new scan from exact next key in the current region
            lastKey = Key.toRawKey(currentCache.getLast().getKey());
            startKey = lastKey.next().toByteString();
        }

        // notify last batch if lastKey is greater than or equal to endKey
        // if startKey is empty, it indicates +∞
        if (lastKey.compareTo(Key.toRawKey(endKey)) >= 0 || startKey.isEmpty()) {
            return;
        }

        List<Kvrpcpb.KvPair> pairs = new ArrayList<>();
        for (Kvrpcpb.KvPair current : currentCache) {
            if (current.hasError()) {
                ByteString val = resolveCurrentLock(session, current, position);
                current = Kvrpcpb.KvPair.newBuilder().setKey(current.getKey()).setValue(val).build();
            }
            pairs.add(current);
        }

        load(session, startKey, endKey, index, position);
    }

    private static ByteString resolveCurrentLock(TiSession session, Kvrpcpb.KvPair current, long position) {
        RegionStoreClient.RegionStoreClientBuilder builder = session.getRegionStoreClientBuilder();
        log.warn(String.format("resolve current key error %s", current.getError().toString()));
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

    private static TiRegion loadCurrentRegionToCache(TiSession session, Key startKey, long position) {
        ByteString key = startKey.toByteString();
        TiRegion region;
        try (RegionStoreClient client = session.getRegionStoreClientBuilder().build(key)) {
            client.setTimeout(session.getConf().getScanTimeout());
            region = client.getRegion();
            BackOffer backOffer = ConcreteBackOffer.newScannerNextMaxBackOff();
            currentCache = client.scan(backOffer, key, position);
            counter.addAndGet(currentCache.size());
            log.info("read snapshot segment size is {}, counter is {}", currentCache.size(), counter);
            return region;
        }
    }

    public static void main(String[] args) throws Exception {
        TiDBSourceConfigFactory factory = new TiDBSourceConfigFactory();
        factory.database("test");
        factory.tableList("mysql_type_fields");
        factory.tikvOptions(options);
        TiDBSourceConfig config = factory.create();
        TiDBSchema schema = config.getSchema();
        TiTableMetadata table = schema.getTableMetadata(TablePath.of("test", "mysql_type_fields")).as(TiTableMetadata.class);
        log.info("table: {}", table);

        try (TiSession session = TiSession.create(schema.getTiConfiguration())) {
            int num = 1;
            long version = session.getTimestamp().getVersion();
            List<Coprocessor.KeyRange> keyRanges = TableKeyRangeUtils.getTableKeyRanges(table.id(), num);
            for (int i = 0; i < keyRanges.size(); i++) {
                load(session,
                        keyRanges.get(i).getStart(),
                        keyRanges.get(i).getEnd(),
                        i,
                        version
                );
            }
        }
    }

    private static class TableKeyRangeUtils {
        public static Coprocessor.KeyRange getTableKeyRange(final long tableId) {
            return KeyRangeUtils.makeCoprocRange(
                    RowKey.createMin(tableId).toByteString(),
                    RowKey.createBeyondMax(tableId).toByteString());
        }

        public static List<Coprocessor.KeyRange> getTableKeyRanges(final long tableId, final int num) {
            Preconditions.checkArgument(num > 0, "Illegal value of num");

            if (num == 1) {
                return ImmutableList.of(getTableKeyRange(tableId));
            }

            final long delta =
                    BigInteger.valueOf(Long.MAX_VALUE)
                            .subtract(BigInteger.valueOf(Long.MIN_VALUE + 1))
                            .divide(BigInteger.valueOf(num))
                            .longValueExact();
            final ImmutableList.Builder<Coprocessor.KeyRange> builder = ImmutableList.builder();
            for (int i = 0; i < num; i++) {
                final RowKey startKey =
                        (i == 0)
                                ? RowKey.createMin(tableId)
                                : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * i);
                final RowKey endKey =
                        (i == num - 1)
                                ? RowKey.createBeyondMax(tableId)
                                : RowKey.toRowKey(tableId, Long.MIN_VALUE + delta * (i + 1));
                builder.add(
                        KeyRangeUtils.makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
            }
            return builder.build();
        }

        public static Coprocessor.KeyRange getTableKeyRange(final long tableId, final int num, final int idx) {
            Preconditions.checkArgument(idx >= 0 && idx < num, "Illegal value of idx");
            return getTableKeyRanges(tableId, num).get(idx);
        }

        public static boolean isRecordKey(final byte[] key) {
            return key[9] == '_' && key[10] == 'r';
        }
    }
}
