package org.jdkxx.cdc.schema.metadata;

import org.apache.flink.table.types.DataType;

public interface ColumnMetadata extends Metadata {
    int NotNullFlag = 1; /* Field can't be NULL */
    int PriKeyFlag = 2; /* Field is part of a primary key */
    int UniqueKeyFlag = 4; /* Field is part of a unique key */
    int MultipleKeyFlag = 8; /* Field is part of a key */
    int BlobFlag = 16; /* Field is a blob */
    int UnsignedFlag = 32; /* Field is unsigned */
    int ZerofillFlag = 64; /* Field is zerofill */
    int BinaryFlag = 128; /* Field is binary   */
    int EnumFlag = 256; /* Field is an enum */
    int AutoIncrementFlag = 512; /* Field is an auto increment field */
    int TimestampFlag = 1024; /* Field is a timestamp */
    int SetFlag = 2048; /* Field is a set */
    int NoDefaultValueFlag = 4096; /* Field doesn't have a default value */
    int OnUpdateNowFlag = 8192; /* Field is set to NOW on UPDATE */
    int NumFlag = 32768; /* Field is a num (for clients) */

    DataType type();

    int code();

    Object defaultValue();

    String comment();

    boolean isAutoIncrement();

    boolean isPrimaryKey();

    default boolean isNull() {
        return !isNotNull();
    }

    boolean isNotNull();

    boolean isUnsigned();
}
