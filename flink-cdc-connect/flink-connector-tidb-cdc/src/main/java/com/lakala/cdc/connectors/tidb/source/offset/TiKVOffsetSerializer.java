package com.lakala.cdc.connectors.tidb.source.offset;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.tikv.common.key.Key;

import java.io.IOException;

public class TiKVOffsetSerializer {
    public static final TiKVOffsetSerializer INSTANCE = new TiKVOffsetSerializer();

    public byte[] serialize(TiKVOffset offset) throws IOException {
        ByteBuf buffer = Unpooled.buffer();
        buffer.writeLong(offset.getPosition());
        buffer.writeBoolean(offset.hasNextKey());
        if (offset.hasNextKey()) {
            byte[] copy = offset.getNextKey().toByteArray();
            buffer.writeInt(copy.length);
            buffer.writeBytes(copy);
        }
        buffer.writeBoolean(offset.hasEndKey());
        if (offset.hasEndKey()) {
            byte[] copy = offset.getEndKey().toByteArray();
            buffer.writeInt(copy.length);
            buffer.writeBytes(copy);
        }
        return buffer.array();
    }

    public TiKVOffset deserialize(byte[] bytes) throws IOException {
        ByteBuf buffer = Unpooled.copiedBuffer(bytes);
        long position = buffer.readLong();
        Key nextKey = Key.EMPTY;
        Key endKey = Key.EMPTY;
        if (buffer.readBoolean()) {
            int length = buffer.readInt();
            byte[] copy = new byte[length];
            buffer.readBytes(copy);
            nextKey = Key.toRawKey(copy);
        }
        if (buffer.readBoolean()) {
            int length = buffer.readInt();
            byte[] copy = new byte[length];
            buffer.readBytes(copy);
            endKey = Key.toRawKey(copy);
        }
        return TiKVOffset.builder()
                .position(position)
                .nextKey(nextKey)
                .endKey(endKey)
                .build();
    }
}
