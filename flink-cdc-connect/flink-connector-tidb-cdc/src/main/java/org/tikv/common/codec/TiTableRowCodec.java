package org.tikv.common.codec;

import com.lakala.cdc.connectors.tidb.schema.TiTableMetadata;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.tikv.common.types.DataType;
import org.tikv.common.types.DataTypeFactory;
import org.tikv.common.types.MySQLType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;

import static org.tikv.common.types.MySQLType.*;

public class TiTableRowCodec {
    protected static Object[] decodeObjects(byte[] value, Long handle, TiTableMetadata table) {
        if (handle == null && table.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }
        List<ColumnMetadata> columns = table.columns();
        int colSize = columns.size();
        // decode bytes to Map<ColumnID, Data>
        HashMap<Long, Object> decodedDataMap = new HashMap<>(colSize);
        TiKVRow row = TiKVRow.createNew(value);

        for (ColumnMetadata column : columns) {
            if (column.isPrimaryKey() && table.isPkHandle()) {
                decodedDataMap.put(column.id(), handle);
                continue;
            }
            TiKVRow.ColIDSearchResult searchResult = row.findColID(column.id());
            if (searchResult.isNull) {
                // current col is null, nothing should be added to decodedMap
                continue;
            }
            if (!searchResult.notFound) {
                // the corresponding column should be found
                assert (searchResult.idx != -1);
                byte[] colData = row.getData(searchResult.idx);

                boolean unsigned = column.isUnsigned();
                DataType type = toDataType(column);
                Object d;
                if (unsigned && (type.getType() == TypeLonglong)) {
                    d = decodeUnsignedLong(colData);
                } else if (unsigned && (type.getType() == TypeLong
                        || type.getType() == TypeInt24
                        || type.getType() == TypeShort
                        || type.getType() == TypeTiny)) {
                    d = decodeUnsignedInteger(colData);
                } else {
                    d = RowDecoderV2.decodeCol(colData, Objects.requireNonNull(toDataType(column)));
                }
                decodedDataMap.put(column.id(), d);
            }
        }

        Object[] res = new Object[colSize];

        // construct Row with Map<ColumnID, Data> & handle
        for (int i = 0; i < colSize; i++) {
            // skip pk is a handle case
            ColumnMetadata column = columns.get(i);
            res[i] = decodedDataMap.get(column.id());
        }
        return res;
    }

    private static DataType toDataType(ColumnMetadata col) {
        int code = col.code();
        MySQLType origin = MySQLType.fromTypeCode(code);
        return DataTypeFactory.of(origin);
    }

    private static Long decodeUnsignedLong(byte[] data) {
        byte[] bytes = new byte[8];
        Arrays.fill(bytes, (byte) 0);
        System.arraycopy(data, 0, bytes, 0, data.length);
        // Convert to unsigned long
        long unsignedLong = 0;
        for (int i = 0; i < bytes.length; i++) {
            // Combine each byte into the long, interpret as unsigned
            unsignedLong |= (Byte.toUnsignedLong(bytes[i]) << (8 * i));
        }
        return unsignedLong;
    }

    private static Integer decodeUnsignedInteger(byte[] data) {
        byte[] bytes = new byte[4];
        Arrays.fill(bytes, (byte) 0);
        System.arraycopy(data, 0, bytes, 0, data.length);
        // Convert to unsigned long
        int unsigneInteger = 0;
        for (int i = 0; i < bytes.length; i++) {
            // Combine each byte into the long, interpret as unsigned
            unsigneInteger |= (Byte.toUnsignedInt(bytes[i]) << (8 * i));
        }
        return unsigneInteger;
    }
}
