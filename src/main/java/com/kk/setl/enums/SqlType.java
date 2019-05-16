/**
 * Copyright (c) 2016 Vijay Vijayaram
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH
 * THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.kk.setl.enums;

import java.sql.Types;

public enum SqlType {
    BIT(Types.BIT),
    TINYINT(Types.TINYINT),
    SMALLINT(Types.SMALLINT),
    INTEGER(Types.INTEGER),
    BIGINT(Types.BIGINT),
    FLOAT(Types.FLOAT),
    REAL(Types.REAL),
    DOUBLE(Types.DOUBLE),
    NUMERIC(Types.NUMERIC),
    DECIMAL(Types.DECIMAL),
    CHAR(Types.CHAR),
    VARCHAR(Types.VARCHAR),
    LONGVARCHAR(Types.LONGVARCHAR),
    DATE(Types.DATE),
    TIME(Types.TIME),
    TIMESTAMP(Types.TIMESTAMP),
    BINARY(Types.BINARY),
    VARBINARY(Types.VARBINARY),
    LONGVARBINARY(Types.LONGVARBINARY),
    NULL(Types.NULL),
    OTHER(Types.OTHER),
    JAVA_OBJECT(Types.JAVA_OBJECT),
    DISTINCT(Types.DISTINCT),
    STRUCT(Types.STRUCT),
    ARRAY(Types.ARRAY),
    BLOB(Types.BLOB),
    CLOB(Types.CLOB),
    REF(Types.REF),
    DATALINK(Types.DATALINK),
    BOOLEAN(Types.BOOLEAN),
    ROWID(Types.ROWID),
    NCLOB(Types.NCLOB),
    SQLXML(Types.SQLXML);

    SqlType(int sqlTypeInt) {
        this.sqlTypeInt = sqlTypeInt;
    }

    private int sqlTypeInt;

    public static SqlType forValue(int sqlTypeInt) {
        for (SqlType st : values()) {
            if (st.sqlTypeInt == sqlTypeInt) {
                return st;
            }
        }
        return null;
    }

    public Object convert(Object val) {
        if (val == null) {
            return null;
        }
        String strVal = val.toString(); 

        if (sqlTypeInt == Types.BIT || sqlTypeInt == Types.TINYINT || sqlTypeInt == Types.SMALLINT
                || sqlTypeInt == Types.INTEGER || sqlTypeInt == Types.BINARY) {
            return Integer.valueOf(strVal);
        } else if (sqlTypeInt == Types.BIGINT) {
            return Long.valueOf(strVal);
        } else if (sqlTypeInt == Types.FLOAT || sqlTypeInt == Types.REAL) {
            return Float.valueOf(strVal);
        } else if (sqlTypeInt == Types.DOUBLE || sqlTypeInt == Types.NUMERIC || sqlTypeInt == Types.DECIMAL) {
            return Double.valueOf(strVal);
        } else if (sqlTypeInt == Types.CHAR) {
            return strVal.charAt(0);
        }
        return strVal;
    }
}
