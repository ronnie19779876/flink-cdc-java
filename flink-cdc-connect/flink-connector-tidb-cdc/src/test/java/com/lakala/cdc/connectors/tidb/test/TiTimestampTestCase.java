package com.lakala.cdc.connectors.tidb.test;

import org.tikv.common.meta.TiTimestamp;

public class TiTimestampTestCase {
    public static void main(String[] args) throws Exception {
        long ts = 453633636633935897L;
        long s = TiTimestamp.extractPhysical(ts);
        System.out.println(s);

        //TiTimestamp timestamp = new TiTimestamp(s, 164);
        //System.out.println(timestamp.getVersion());
    }
}
