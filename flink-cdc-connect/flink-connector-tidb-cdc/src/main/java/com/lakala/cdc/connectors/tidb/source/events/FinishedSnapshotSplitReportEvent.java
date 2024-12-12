package com.lakala.cdc.connectors.tidb.source.events;

import org.apache.flink.api.connector.source.SourceEvent;
import com.lakala.cdc.connectors.tidb.source.offset.TiKVOffset;

import java.util.Map;

public record FinishedSnapshotSplitReportEvent(Map<String, TiKVOffset> offsets) implements SourceEvent {
}
