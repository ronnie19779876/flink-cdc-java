package com.lakala.cdc.connectors.tidb.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import java.util.List;

public record FinishedSnapshotSplitAckEvent(List<String> finishedSplits) implements SourceEvent {
}
