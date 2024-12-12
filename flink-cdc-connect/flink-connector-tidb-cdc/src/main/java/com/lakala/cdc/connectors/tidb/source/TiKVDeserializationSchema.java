package com.lakala.cdc.connectors.tidb.source;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.util.Collector;
import com.lakala.cdc.connectors.tidb.schema.TiDBSchema;

import static org.tikv.kvproto.Cdcpb.Event.Row;
import static org.tikv.kvproto.Kvrpcpb.KvPair;

import java.io.Serializable;

public interface TiKVDeserializationSchema<T> extends Serializable, ResultTypeQueryable<T> {
    void deserialize(KvPair record, TiDBSchema catalog, Collector<T> out) throws Exception;

    void deserialize(Row record, TiDBSchema catalog, Collector<T> out) throws Exception;
}
