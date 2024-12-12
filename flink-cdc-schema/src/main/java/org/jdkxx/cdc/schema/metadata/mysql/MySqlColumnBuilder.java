package org.jdkxx.cdc.schema.metadata.mysql;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.jdkxx.cdc.schema.metadata.table.PhysicalColumn;
import org.jdkxx.cdc.schema.types.*;

@Slf4j
public class MySqlColumnBuilder {
    private String name;
    private String code;
    private int precision;
    private int scale;
    private boolean nullable;
    private boolean unsigned;
    private boolean autoIncrement;
    private Object defaultValue;
    private String comment;
    private CharacterSet charset;

    private MySqlColumnBuilder() {
    }

    public static MySqlColumnBuilder builder() {
        return new MySqlColumnBuilder();
    }

    public MySqlColumnBuilder name(String name) {
        this.name = name;
        return this;
    }

    public MySqlColumnBuilder code(String code) {
        this.code = code;
        return this;
    }

    public MySqlColumnBuilder precision(int precision) {
        this.precision = precision;
        return this;
    }

    public MySqlColumnBuilder scale(int scale) {
        this.scale = scale;
        return this;
    }

    public MySqlColumnBuilder nullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    public MySqlColumnBuilder unsigned(boolean unsigned) {
        this.unsigned = unsigned;
        return this;
    }

    public MySqlColumnBuilder autoIncrement(boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
        return this;
    }

    public MySqlColumnBuilder defaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }

    public MySqlColumnBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public MySqlColumnBuilder charset(CharacterSet charset) {
        this.charset = charset;
        return this;
    }

    public PhysicalColumn build() {
        int flag = 0;
        if (!nullable) {
            flag |= PhysicalColumn.NotNullFlag;
        }
        if (unsigned) {
            flag |= PhysicalColumn.UnsignedFlag;
        }
        if (autoIncrement) {
            flag |= PhysicalColumn.AutoIncrementFlag;
        }
        if (defaultValue == null) {
            flag |= PhysicalColumn.NoDefaultValueFlag;
        }
        DataType type = MySqlDataTypeMapper.mapping(name, code, precision, scale, unsigned, nullable);
        return PhysicalColumn.builder()
                .name(name)
                .flag(flag)
                .comment(comment)
                .defaultValue(defaultValue)
                .charset(charset)
                .type(type)
                .build();
    }

    private static class MySqlDataTypeMapper {
        private static final String MYSQL_UNKNOWN = "UNKNOWN";
        private static final String MYSQL_BIT = "BIT";

        // -------------------------number----------------------------
        private static final String MYSQL_TINYINT = "TINYINT";
        private static final String MYSQL_SMALLINT = "SMALLINT";
        private static final String MYSQL_MEDIUMINT = "MEDIUMINT";
        private static final String MYSQL_INT = "INT";
        private static final String MYSQL_INTEGER = "INTEGER";
        private static final String MYSQL_BIGINT = "BIGINT";
        private static final String MYSQL_DECIMAL = "DECIMAL";
        private static final String MYSQL_FLOAT = "FLOAT";
        private static final String MYSQL_DOUBLE = "DOUBLE";

        // -------------------------string----------------------------
        private static final String MYSQL_CHAR = "CHAR";
        private static final String MYSQL_VARCHAR = "VARCHAR";
        private static final String MYSQL_TINYTEXT = "TINYTEXT";
        private static final String MYSQL_MEDIUMTEXT = "MEDIUMTEXT";
        private static final String MYSQL_TEXT = "TEXT";
        private static final String MYSQL_LONGTEXT = "LONGTEXT";
        private static final String MYSQL_JSON = "JSON";

        // ------------------------------time-------------------------
        private static final String MYSQL_DATE = "DATE";
        private static final String MYSQL_DATETIME = "DATETIME";
        private static final String MYSQL_TIME = "TIME";
        private static final String MYSQL_TIMESTAMP = "TIMESTAMP";
        private static final String MYSQL_YEAR = "YEAR";

        // ------------------------------blob-------------------------
        private static final String MYSQL_TINYBLOB = "TINYBLOB";
        private static final String MYSQL_MEDIUMBLOB = "MEDIUMBLOB";
        private static final String MYSQL_BLOB = "BLOB";
        private static final String MYSQL_LONGBLOB = "LONGBLOB";
        private static final String MYSQL_BINARY = "BINARY";
        private static final String MYSQL_VARBINARY = "VARBINARY";
        private static final String MYSQL_GEOMETRY = "GEOMETRY";

        private static final int RAW_TIME_LENGTH = 10;
        private static final int RAW_TIMESTAMP_LENGTH = 19;

        public static DataType mapping(String columnName,
                                       String code,
                                       int precision,
                                       int scale,
                                       boolean unsigned,
                                       boolean nullable) {
            DataType type;
            switch (code.toUpperCase()) {
                case MYSQL_BIT:
                    type = DataTypes.BOOLEAN();
                    break;
                case MYSQL_TINYBLOB:
                    type = DataTypes.of(new TinyBlobType());
                    break;
                case MYSQL_MEDIUMBLOB:
                    type = DataTypes.of(new MediumBlobType());
                    break;
                case MYSQL_BLOB:
                    type = DataTypes.of(new BlobType());
                    break;
                case MYSQL_LONGBLOB:
                    type = DataTypes.of(new LongBlobType());
                    break;
                case MYSQL_VARBINARY:
                    type = DataTypes.VARBINARY(precision);
                    break;
                case MYSQL_BINARY:
                    type = DataTypes.BINARY(precision);
                    break;
                case MYSQL_TINYINT:
                    type = DataTypes.of(new TinyIntType(unsigned));
                    break;
                case MYSQL_SMALLINT:
                    type = DataTypes.of(new SmallIntType(unsigned));
                    break;
                case MYSQL_MEDIUMINT:
                    type = DataTypes.of(new MediumIntType(unsigned));
                    break;
                case MYSQL_INT:
                    type = DataTypes.of(new IntType(unsigned));
                    break;
                case MYSQL_INTEGER:
                    type = DataTypes.of(new IntegerType(unsigned));
                    break;
                case MYSQL_BIGINT:
                    type = DataTypes.of(new BigIntType(unsigned));
                    break;
                case MYSQL_DECIMAL:
                    type = DataTypes.of(new DecimalType(unsigned, precision, scale));
                    break;
                case MYSQL_FLOAT:
                    if (unsigned) {
                        log.warn("FLOAT UNSIGNED will probably cause value overflow.");
                    }
                    type = DataTypes.of(new FloatType(unsigned));
                    break;
                case MYSQL_DOUBLE:
                    if (unsigned) {
                        log.warn("DOUBLE UNSIGNED will probably cause value overflow.");
                    }
                    type = DataTypes.of(new DoubleType(unsigned));
                    break;
                case MYSQL_CHAR:
                    type = DataTypes.CHAR(precision);
                    break;
                case MYSQL_VARCHAR:
                    type = DataTypes.VARCHAR(precision);
                    break;
                case MYSQL_TINYTEXT:
                    type = DataTypes.of(new TinyTextType(precision));
                    break;
                case MYSQL_MEDIUMTEXT:
                    type = DataTypes.of(new MediumTextType(precision));
                    break;
                case MYSQL_TEXT:
                    type = DataTypes.of(new TextType(precision));
                    break;
                case MYSQL_LONGTEXT:
                    log.warn(
                            "Type '{}' has a maximum precision of 536870911 in MySQL. "
                                    + "Due to limitations in the Flink type system, "
                                    + "the precision will be set to 2147483647.",
                            MYSQL_LONGTEXT);
                    type = DataTypes.of(new LongTextType(precision));
                    break;
                case MYSQL_DATE:
                    type = DataTypes.DATE();
                    break;
                case MYSQL_TIME:
                    type = isExplicitPrecision(precision, RAW_TIME_LENGTH)
                            ? DataTypes.TIME(precision - RAW_TIME_LENGTH - 1)
                            : DataTypes.TIME(0);
                    break;
                case MYSQL_DATETIME:
                    type = DataTypes.of(new DateTimeType());
                    break;
                case MYSQL_TIMESTAMP:
                    if (isExplicitPrecision(precision, RAW_TIMESTAMP_LENGTH)) {
                        int p = precision - RAW_TIMESTAMP_LENGTH - 1;
                        if (p <= 6 && p >= 0) {
                            type = DataTypes.TIMESTAMP(p);
                        } else {
                            type = p > 6 ? DataTypes.TIMESTAMP(6) : DataTypes.TIMESTAMP(0);
                        }
                    } else {
                        type = DataTypes.TIMESTAMP(0);
                    }
                    break;
                case MYSQL_YEAR:
                    type = DataTypes.of(new YearType());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "Doesn't support data type '%s' on column '%s' yet.", code, columnName));
            }
            if (type != null) {
                type = nullable ? type.nullable() : type.notNull();
            }
            return type;
        }

        private static boolean isExplicitPrecision(int precision, int defaultPrecision) {
            return precision > defaultPrecision && precision - defaultPrecision - 1 <= 9;
        }
    }
}
