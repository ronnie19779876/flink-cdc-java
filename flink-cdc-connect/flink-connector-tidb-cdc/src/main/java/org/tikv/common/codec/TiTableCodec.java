package org.tikv.common.codec;

import com.lakala.cdc.connectors.tidb.schema.TiTableMetadata;
import org.tikv.common.exception.CodecException;

public class TiTableCodec {
    public static Object[] decodeObjects(byte[] value, Long handle, TiTableMetadata table) {
        if (value.length == 0) {
            throw new CodecException("Decode fails: value length is zero");
        }
        if ((value[0] & 0xff) == TiKVRow.CODEC_VER) {
            return TiTableRowCodec.decodeObjects(value, handle, table);
        }
        return TiTableRowCodecV1.decodeObjects(value, handle, table);
    }
}
