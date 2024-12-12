package org.tikv.common.codec;

import org.tikv.common.exception.InvalidCodecFormatException;

import java.io.Serializable;
import java.util.Arrays;

public class TiKVRow implements Serializable {
    public static int CODEC_VER = 0x80;
    boolean large;
    int numNotNullCols;
    int numNullCols;
    int[] colIDs;
    int[] offsets;
    byte[] data;

    private TiKVRow(byte[] rowData) {
        fromBytes(rowData);
    }

    private TiKVRow(boolean large, int numNotNullCols, int numNullCols) {
        this.large = large;
        this.numNotNullCols = numNotNullCols;
        this.numNullCols = numNullCols;
    }

    public static TiKVRow createNew(byte[] rowData) {
        return new TiKVRow(rowData);
    }

    public static TiKVRow createEmpty() {
        return new TiKVRow(false, 0, 0);
    }

    public byte[] getData(int i) {
        int start = 0, end = 0;
        if (i > 0) {
            start = this.offsets[i - 1];
        }
        end = this.offsets[i];
        return Arrays.copyOfRange(this.data, start, end);
    }

    private void fromBytes(byte[] rowData) {
        CodecDataInputLittleEndian cdi = new CodecDataInputLittleEndian(rowData);
        if (cdi.readUnsignedByte() != CODEC_VER) {
            throw new InvalidCodecFormatException("invalid codec version");
        }
        this.large = (cdi.readUnsignedByte() & 1) > 0;
        this.numNotNullCols = cdi.readUnsignedShort();
        this.numNullCols = cdi.readUnsignedShort();
        int cursor = 6;
        if (this.large) {
            int numCols = this.numNotNullCols + this.numNullCols;
            int colIDsLen = numCols * 4;
            this.colIDs = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                this.colIDs[i] = cdi.readInt();
            }
            cursor += colIDsLen;
            numCols = this.numNotNullCols;
            int offsetsLen = numCols * 4;
            this.offsets = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                this.offsets[i] = cdi.readInt();
            }
            cursor += offsetsLen;
        } else {
            int numCols = this.numNotNullCols + this.numNullCols;
            int colIDsLen = numCols;
            byte[] ids = new byte[numCols];
            this.colIDs = new int[numCols];
            cdi.readFully(ids, 0, numCols);
            for (int i = 0; i < numCols; i++) {
                this.colIDs[i] = ids[i] & 0xFF;
            }
            cursor += colIDsLen;
            numCols = this.numNotNullCols;
            int offsetsLen = numCols * 2;
            this.offsets = new int[numCols];
            for (int i = 0; i < numCols; i++) {
                this.offsets[i] = cdi.readUnsignedShort();
            }
            cursor += offsetsLen;
        }
        this.data = Arrays.copyOfRange(rowData, cursor, rowData.length);
    }

    private void writeIntArray(CodecDataOutput cdo, int[] arr) {
        for (int value : arr) {
            cdo.writeInt(value);
        }
    }

    public byte[] toBytes() {
        CodecDataOutputLittleEndian cdo = new CodecDataOutputLittleEndian();
        cdo.write(CODEC_VER);
        cdo.write(this.large ? 1 : 0);
        cdo.writeShort(this.numNotNullCols);
        cdo.writeShort(this.numNullCols);
        writeIntArray(cdo, this.colIDs);
        writeIntArray(cdo, this.offsets);
        cdo.write(this.data);
        return cdo.toBytes();
    }

    public ColIDSearchResult findColID(long colID) {
        int i = 0, j = this.numNotNullCols;
        ColIDSearchResult result = new ColIDSearchResult(-1, false, false);
        result.idx = binarySearch(i, j, colID);
        if (result.idx != -1) {
            return result;
        }

        // Search the column in null columns array.
        i = this.numNotNullCols;
        j = this.numNotNullCols + this.numNullCols;
        int id = binarySearch(i, j, colID);
        if (id != -1) {
            // colID found in null cols.
            result.isNull = true;
        } else {
            result.notFound = true;
        }
        return result;
    }

    private int binarySearch(int i, int j, long colID) {
        while (i < j) {
            int h = (int) ((i + (long) j) >> 1);
            // i <= h < j
            long v;
            v = this.colIDs[h];
            if (v < colID) {
                i = h + 1;
            } else if (v > colID) {
                j = h;
            } else {
                return h;
            }
        }
        return -1;
    }

    public static class ColIDSearchResult {
        int idx;
        boolean isNull;
        boolean notFound;

        private ColIDSearchResult(int idx, boolean isNull, boolean notFound) {
            this.idx = idx;
            this.isNull = isNull;
            this.notFound = notFound;
        }
    }
}
