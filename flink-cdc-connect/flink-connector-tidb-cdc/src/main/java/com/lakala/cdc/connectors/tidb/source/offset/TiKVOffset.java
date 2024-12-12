package com.lakala.cdc.connectors.tidb.source.offset;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.tikv.common.key.Key;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class TiKVOffset implements Serializable {
    private long position;
    private ByteString nextKey;
    private ByteString endKey;

    public static TiKVOffset EMPTY = new TiKVOffset(0, ByteString.EMPTY, ByteString.EMPTY);

    public boolean hasPosition() {
        return position > 0;
    }

    public boolean isEmptyKey() {
        return nextKey.isEmpty();
    }

    public boolean hasNextKey() {
        return !isEmptyKey();
    }

    public boolean hasEndKey() {
        return !endKey.isEmpty();
    }

    public boolean endOfRows() {
        return isEmptyKey();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private long position;
        private ByteString nextKey;
        private ByteString endKey = ByteString.EMPTY;

        public Builder position(long position) {
            this.position = position;
            return this;
        }

        public Builder nextKey(Key nextKey) {
            this.nextKey = nextKey.toByteString();
            return this;
        }

        public Builder endKey(Key endKey) {
            this.endKey = endKey.toByteString();
            return this;
        }

        public TiKVOffset build() {
            return new TiKVOffset(position, nextKey, endKey);
        }
    }
}
