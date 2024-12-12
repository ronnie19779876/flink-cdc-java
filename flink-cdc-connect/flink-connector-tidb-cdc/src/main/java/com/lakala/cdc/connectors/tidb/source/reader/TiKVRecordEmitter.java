package com.lakala.cdc.connectors.tidb.source.reader;

import com.lakala.cdc.connectors.tidb.source.TiKVDeserializationSchema;
import com.lakala.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import com.lakala.cdc.connectors.tidb.source.metrics.TiDBSourceMetrics;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;
import com.lakala.cdc.connectors.tidb.source.split.TiDBSplitState;
import com.lakala.cdc.connectors.tidb.source.split.TiKVRecords;
import com.lakala.cdc.connectors.tidb.utils.TableKeyRangeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.util.Collector;
import org.jdkxx.cdc.schema.metadata.table.TablePath;
import org.tikv.common.key.Key;
import org.tikv.common.meta.TiTimestamp;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Kvrpcpb;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TiKVRecordEmitter<T> implements RecordEmitter<TiKVRecords, T, TiDBSplitState> {
    private final TiDBSourceConfig sourceConfig;
    private final TiDBSourceMetrics sourceMetrics;
    private final TiKVDeserializationSchema<T> deserializer;
    private final OutputCollector<T> outputCollector;
    private final AtomicInteger counter = new AtomicInteger(0);

    public TiKVRecordEmitter(TiDBSourceConfig sourceConfig,
                             TiKVDeserializationSchema<T> deserializer,
                             TiDBSourceMetrics sourceMetrics) {
        this.sourceConfig = sourceConfig;
        this.deserializer = deserializer;
        this.sourceMetrics = sourceMetrics;
        this.outputCollector = new OutputCollector<>();
    }

    @Override
    public void emitRecord(TiKVRecords element, SourceOutput<T> output, TiDBSplitState splitState) throws Exception {
        outputCollector.output = output;
        if (element.isSnapshotEvent()) {
            TablePath path = splitState.toTiDBSplit().asSnapshotSplit().getPath();
            List<Kvrpcpb.KvPair> pairs = element.getKvPairs().stream()
                    .filter(pair -> TableKeyRangeUtils.isRecordKey(pair.getKey().toByteArray()))
                    .toList();

            TiKVOffset offset = TiKVOffset.builder()
                    .position(element.getPosition())
                    .nextKey(element.getNextKey())
                    .endKey(element.getEndKey())
                    .build();
            splitState.asSnapshotSplitState().setOffset(offset);
            for (Kvrpcpb.KvPair pair : pairs) {
                counter.incrementAndGet();
                this.deserializer.deserialize(pair, sourceConfig.getSchema(), outputCollector);
                reportMetrics(0L, offset.getPosition());
            }
            log.info("read snapshot events from {}, counter:{}.", path, counter.get());
            if (offset.endOfRows()) {
                //when we don't have any snapshot, we will reset the counter.
                counter.set(0);
            }
        } else if (element.isChangeEvent()) {
            TiKVOffset offset = TiKVOffset.builder()
                    .position(element.getPosition())
                    .nextKey(Key.EMPTY)
                    .endKey(Key.EMPTY)
                    .build();
            splitState.asBinlogSplitState().setOffset(offset);
            List<Row> rows = element.getRows();
            if (!rows.isEmpty()) {
                for (Row row : rows) {
                    this.deserializer.deserialize(row, sourceConfig.getSchema(), outputCollector);
                    reportMetrics(row.getStartTs(), row.getCommitTs());
                }
            }
        }
    }

    private void reportMetrics(long messageTs, long fetchTs) {
        long now = System.currentTimeMillis();
        // record the latest process time
        sourceMetrics.recordProcessTime(now);
        long messageTimestamp = TiTimestamp.extractPhysical(messageTs);
        long fetchTimestamp = TiTimestamp.extractPhysical(fetchTs);
        if (messageTimestamp > 0L) {
            // report fetch delay
            if (fetchTimestamp >= messageTimestamp) {
                sourceMetrics.recordFetchDelay(fetchTimestamp - messageTimestamp);
            }
            // report emit delay
            sourceMetrics.recordEmitDelay(now - messageTimestamp);
        }
    }

    private static class OutputCollector<T> implements Collector<T> {
        private SourceOutput<T> output;

        @Override
        public void collect(T record) {
            output.collect(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
