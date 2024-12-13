package org.jdkxx.cdc.connectors.tidb.schema;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.jdkxx.cdc.schema.metadata.ColumnMetadata;
import org.jdkxx.cdc.schema.metadata.table.PhysicalColumn;
import org.jdkxx.cdc.schema.types.*;
import org.tikv.common.meta.Collation;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.types.MySQLType;

@Slf4j
public class TiTableColumnBuilder {
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public ColumnMetadata build(TiColumnInfo columnInfo) {
            DataType type = DataTypeMapper.mapping(columnInfo);
            String collation = Collation.translate(columnInfo.getType().getCollationCode());
            return PhysicalColumn.builder()
                    .id(columnInfo.getId())
                    .name(columnInfo.getName())
                    .comment(columnInfo.getComment())
                    .charset(new CharacterSet(columnInfo.getType().getCharset(), collation))
                    .type(type)
                    .code(columnInfo.getType().getTypeCode())
                    .flag(columnInfo.getType().getFlag())
                    .build();
        }
    }

    private static class DataTypeMapper {
        private static final int RAW_TIMESTAMP_LENGTH = 19;
        private static final int RAW_TIME_LENGTH = 10;

        static DataType mapping(TiColumnInfo columnInfo) {
            boolean unsigned = columnInfo.getType().isUnsigned();
            long precision = columnInfo.getType().getLength();
            int scale = columnInfo.getType().getDecimal();
            boolean binary = columnInfo.getType().isBinary();

            MySQLType origin = MySQLType.fromTypeCode(columnInfo.getType().getTypeCode());
            DataType type = switch (origin) {
                case TypeBit -> DataTypes.BOOLEAN();
                case TypeNewDecimal, TypeDecimal -> DataTypes.of(new DecimalType(unsigned, (int) precision, scale));
                case TypeTiny -> DataTypes.of(new TinyIntType(unsigned));
                case TypeShort -> DataTypes.of(new SmallIntType(unsigned));
                case TypeLong -> DataTypes.of(new IntType(unsigned));
                case TypeInt24 -> DataTypes.of(new MediumIntType(unsigned));
                case TypeLonglong -> DataTypes.of(new BigIntType(unsigned));
                case TypeFloat -> {
                    if (unsigned) {
                        log.warn("FLOAT UNSIGNED will probably cause value overflow.");
                    }
                    yield DataTypes.of(new FloatType(unsigned));
                }
                case TypeDouble -> {
                    if (unsigned) {
                        log.warn("DOUBLE UNSIGNED will probably cause value overflow.");
                    }
                    yield DataTypes.of(new DoubleType(unsigned));
                }
                case TypeTimestamp -> {
                    boolean explicitPrecision = isExplicitPrecision((int) precision, RAW_TIMESTAMP_LENGTH);
                    if (explicitPrecision) {
                        int p = (int) precision - RAW_TIMESTAMP_LENGTH - 1;
                        if (p <= 6 && p >= 0) {
                            yield DataTypes.TIMESTAMP(p);
                        } else {
                            if (p > 6) {
                                yield DataTypes.TIMESTAMP(6);
                            } else {
                                yield DataTypes.TIMESTAMP(0);
                            }
                        }
                    } else {
                        yield DataTypes.TIMESTAMP(0);
                    }
                }
                case TypeDate, TypeNewDate -> DataTypes.DATE();
                case TypeDuration -> isExplicitPrecision((int) precision, RAW_TIME_LENGTH) ?
                        DataTypes.TIME((int) precision - RAW_TIME_LENGTH - 1) :
                        DataTypes.TIME(0);
                case TypeDatetime -> DataTypes.of(new DateTimeType());
                case TypeYear -> DataTypes.of(new YearType());
                case TypeString -> DataTypes.CHAR((int) precision);
                case TypeVarchar -> DataTypes.VARCHAR((int) precision);
                case TypeBlob -> binary ? DataTypes.of(new BlobType()) :
                        DataTypes.of(new TextType((int) precision));
                case TypeTinyBlob -> binary ? DataTypes.of(new TinyBlobType()) :
                        DataTypes.of(new TinyTextType((int) precision));
                case TypeMediumBlob -> binary ? DataTypes.of(new MediumBlobType()) :
                        DataTypes.of(new MediumTextType((int) precision));
                case TypeLongBlob -> binary ? DataTypes.of(new LongBlobType()) :
                        DataTypes.of(new LongTextType((int) precision));
                default -> throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support data type '%s' on column '%s' yet.",
                                origin, columnInfo.getName()));
            };

            if (type != null) {
                type = columnInfo.getType().isNotNull() ? type.notNull() : type.nullable();
            }
            return type;
        }

        private static boolean isExplicitPrecision(int precision, int defaultPrecision) {
            return precision > defaultPrecision && precision - defaultPrecision - 1 <= 9;
        }
    }
}
