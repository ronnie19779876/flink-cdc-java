package org.jdkxx.cdc.connectors.tidb.source;

import org.jdkxx.cdc.connectors.tidb.source.assigners.TiDBBinlogSplitAssigner;
import org.jdkxx.cdc.connectors.tidb.source.assigners.TiDBHybridSplitAssigner;
import org.jdkxx.cdc.connectors.tidb.source.assigners.TiDBSplitAssigner;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.BinlogPendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.HybridPendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.PendingSplitsState;
import org.jdkxx.cdc.connectors.tidb.source.assigners.state.PendingSplitsStateSerializer;
import org.jdkxx.cdc.connectors.tidb.source.config.TiDBSourceConfig;
import org.jdkxx.cdc.connectors.tidb.source.enumerator.TiDBSourceEnumerator;
import org.jdkxx.cdc.connectors.tidb.source.metrics.TiDBSourceMetrics;
import org.jdkxx.cdc.connectors.tidb.source.reader.TiDBSourceReader;
import org.jdkxx.cdc.connectors.tidb.source.reader.TiDBSourceReaderContext;
import org.jdkxx.cdc.connectors.tidb.source.reader.TiDBSplitReader;
import org.jdkxx.cdc.connectors.tidb.source.reader.TiKVRecordEmitter;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplit;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplitSerializer;
import org.jdkxx.cdc.connectors.tidb.source.split.TiDBSplitState;
import org.jdkxx.cdc.connectors.tidb.source.split.TiKVRecords;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.tikv.common.TiSession;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.function.Supplier;

public class TiDBSource<T> implements Source<T, TiDBSplit, PendingSplitsState>, ResultTypeQueryable<T> {
    private final TiDBSourceConfig sourceConfig;
    private final TiKVDeserializationSchema<T> deserializer;
    private final RecordEmitterSupplier<T> recordEmitterSupplier;

    TiDBSource(TiDBSourceConfig sourceConfig,
               TiKVDeserializationSchema<T> deserializer) {
        this.sourceConfig = sourceConfig;
        this.deserializer = deserializer;
        this.recordEmitterSupplier = (sourceReaderMetrics, config) ->
                new TiKVRecordEmitter<>(config, deserializer, sourceReaderMetrics);
    }

    public static <T> TiDBSourceBuilder<T> builder() {
        return new TiDBSourceBuilder<>();
    }

    @Override
    public Boundedness getBoundedness() {
        if (sourceConfig.getStartupOptions().isSnapshotOnly()) {
            return Boundedness.BOUNDED;
        }
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<TiDBSplit, PendingSplitsState> createEnumerator(
            SplitEnumeratorContext<TiDBSplit> enumContext) throws Exception {
        final TiDBSplitAssigner splitAssigner;
        if (!sourceConfig.getStartupOptions().isStreamOnly()) {
            splitAssigner = new TiDBHybridSplitAssigner(sourceConfig);
        } else {
            splitAssigner = new TiDBBinlogSplitAssigner(sourceConfig);
        }
        return new TiDBSourceEnumerator(enumContext, splitAssigner, getBoundedness());
    }

    @Override
    public SplitEnumerator<TiDBSplit, PendingSplitsState> restoreEnumerator(
            SplitEnumeratorContext<TiDBSplit> enumContext, PendingSplitsState pendingSplitsState) throws Exception {
        final TiDBSplitAssigner splitAssigner;
        if (pendingSplitsState instanceof HybridPendingSplitsState checkpoint) {
            splitAssigner = new TiDBHybridSplitAssigner(
                    sourceConfig,
                    checkpoint);
        } else if (pendingSplitsState instanceof BinlogPendingSplitsState checkpoint) {
            splitAssigner = new TiDBBinlogSplitAssigner(sourceConfig, checkpoint);
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported restored SourceSplitsState: " + pendingSplitsState);
        }
        return new TiDBSourceEnumerator(enumContext, splitAssigner, getBoundedness());
    }

    @Override
    public SimpleVersionedSerializer<TiDBSplit> getSplitSerializer() {
        return TiDBSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<PendingSplitsState> getEnumeratorCheckpointSerializer() {
        return new PendingSplitsStateSerializer(getSplitSerializer());
    }

    @Override
    public SourceReader<T, TiDBSplit> createReader(SourceReaderContext readerContext) throws Exception {
        TiSession session = TiSession.create(sourceConfig.getTiConfiguration());
        TiDBSourceReaderContext context = new TiDBSourceReaderContext(readerContext, session);
        Supplier<TiDBSplitReader> splitReaderSupplier =
                () -> new TiDBSplitReader(sourceConfig, context);

        final Method metricGroupMethod = readerContext.getClass().getMethod("metricGroup");
        metricGroupMethod.setAccessible(true);
        final MetricGroup metricGroup = (MetricGroup) metricGroupMethod.invoke(readerContext);

        final TiDBSourceMetrics sourceReaderMetrics = new TiDBSourceMetrics(metricGroup);
        sourceReaderMetrics.registerMetrics();

        return new TiDBSourceReader<>(
                sourceConfig,
                splitReaderSupplier,
                recordEmitterSupplier.get(sourceReaderMetrics, sourceConfig),
                readerContext.getConfiguration(),
                context);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return deserializer.getProducedType();
    }

    @FunctionalInterface
    interface RecordEmitterSupplier<T> extends Serializable {
        RecordEmitter<TiKVRecords, T, TiDBSplitState> get(
                TiDBSourceMetrics metrics, TiDBSourceConfig sourceConfig);
    }
}
