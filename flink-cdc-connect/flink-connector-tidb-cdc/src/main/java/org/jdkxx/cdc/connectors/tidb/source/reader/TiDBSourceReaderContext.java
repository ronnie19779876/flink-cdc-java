package org.jdkxx.cdc.connectors.tidb.source.reader;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.tikv.common.TiSession;

@Slf4j
public class TiDBSourceReaderContext {
    @Getter
    private final SourceReaderContext sourceReaderContext;
    private volatile boolean isBinlogSplitReaderSuspended;
    @Getter
    @Setter
    private volatile boolean hasAssignedBinlogSplit;
    @Getter
    private final TiSession session;

    public TiDBSourceReaderContext(final SourceReaderContext sourceReaderContext, final TiSession session) {
        this.sourceReaderContext = sourceReaderContext;
        this.session = session;
    }

    public boolean isBinlogSplitReaderSuspended() {
        return isBinlogSplitReaderSuspended;
    }

    public void suspendBinlogSplitReader() {
        this.isBinlogSplitReaderSuspended = true;
    }

    public void wakeupSuspendedBinlogSplitReader() {
        this.isBinlogSplitReaderSuspended = false;
    }

    public void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }
}
