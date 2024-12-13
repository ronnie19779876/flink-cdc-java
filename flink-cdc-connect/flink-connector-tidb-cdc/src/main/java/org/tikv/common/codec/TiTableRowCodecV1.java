package org.tikv.common.codec;

import org.jdkxx.cdc.connectors.tidb.schema.TiTableMetadata;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.tikv.common.types.DataType;
import org.tikv.common.types.DataTypeFactory;
import org.tikv.common.types.IntegerType;
import org.tikv.common.types.MySQLType;

import java.util.HashMap;
import java.util.List;
import java.util.Objects;

public class TiTableRowCodecV1 {
    protected static Object[] decodeObjects(byte[] value, Long handle, TiTableMetadata table) {
        if (handle == null && table.isPkHandle()) {
            throw new IllegalArgumentException("when pk is handle, handle cannot be null");
        }

        List<ColumnMetadata> columns = table.columns();
        int colSize = columns.size();
        HashMap<Long, ColumnMetadata> idToColumn = new HashMap<>(colSize);
        for (ColumnMetadata column : columns) {
            idToColumn.put(column.id(), column);
        }

        // decode bytes to Map<ColumnID, Data>
        HashMap<Long, Object> decodedDataMap = new HashMap<>(colSize);
        CodecDataInput cdi = new CodecDataInput(value);
        Object[] res = new Object[colSize];
        while (!cdi.eof()) {
            long colID = (long) IntegerType.BIGINT.decode(cdi);
            Object colValue = Objects.requireNonNull(toDataType(idToColumn.get(colID))).decodeForBatchWrite(cdi);
            decodedDataMap.put(colID, colValue);
        }

        // construct Row with Map<ColumnID, Data> & handle
        for (int i = 0; i < colSize; i++) {
            // skip pk is a handle case
            ColumnMetadata column = columns.get(i);
            if (column.isPrimaryKey() && table.isPkHandle()) {
                res[i] = handle;
            } else {
                res[i] = decodedDataMap.get(column.id());
            }
        }
        return res;
    }

    private static DataType toDataType(ColumnMetadata column) {
        int code = column.code();
        MySQLType origin = MySQLType.fromTypeCode(code);
        return DataTypeFactory.of(origin);
    }
}
