package org.jdkxx.cdc.connectors.tidb.source.metrics;

import lombok.Getter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.jdkxx.cdc.connectors.tidb.source.reader.TiKVRecordEmitter;
import org.jdkxx.cdc.common.metrics.MetricNames;

public class TiDBSourceMetrics {
    private final MetricGroup metricGroup;
    /**
     * The last record processing time, which is updated after {@link
     * TiKVRecordEmitter} fetches a batch of data. It's mainly used to report metrics
     * sourceIdleTime for sourceIdleTime = System.currentTimeMillis() - processTime.
     */
    private long processTime = 0L;
    /**
     * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
     * record fetched into the source operator.
     */
    @Getter
    private long fetchDelay = 0L;
    /**
     * currentEmitEventTimeLag = EmitTime - messageTimestamp, where the EmitTime is the time the
     * record leaves the source operator.
     */
    @Getter
    private long emitDelay = 0L;

    public TiDBSourceMetrics(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
    }

    public void registerMetrics() {
        metricGroup.gauge(MetricNames.CURRENT_FETCH_EVENT_TIME_LAG, (Gauge<Long>) this::getFetchDelay);
        metricGroup.gauge(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG, (Gauge<Long>) this::getEmitDelay);
        metricGroup.gauge(MetricNames.SOURCE_IDLE_TIME, (Gauge<Long>) this::getIdleTime);
    }

    public long getIdleTime() {
        // no previous process time at the beginning, return 0 as idle time
        if (processTime == 0) {
            return 0;
        }
        return System.currentTimeMillis() - processTime;
    }

    public void recordProcessTime(long processTime) {
        this.processTime = processTime;
    }

    public void recordFetchDelay(long fetchDelay) {
        this.fetchDelay = fetchDelay;
    }

    public void recordEmitDelay(long emitDelay) {
        this.emitDelay = emitDelay;
    }
}
