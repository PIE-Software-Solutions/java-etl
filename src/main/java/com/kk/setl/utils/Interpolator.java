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
package com.kk.setl.utils;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Interpolator {

    public static Interpolator getInstance() {
        return new Interpolator();
    }

    public String interpolate(final String str, final Map<String, Object> values) {
        if (str == null || MapUtils.isEmpty(values)) {
            return str;
        }
        String istr = str;
        for(String col : values.keySet()) {
            istr = interpolate(istr, Constants.COLON + col, values.get(col));
        }
        return istr.trim();
    }

    String interpolate(final String str, final String key, final Object value) {
        if (StringUtils.isEmpty(str) || StringUtils.isEmpty(key)) {
            return str;
        }

        String istr = str;
        Pattern p = Pattern.compile(key + "\\b");
        Matcher m = p.matcher(istr);
        istr = m.replaceAll(toInterpolateValue(value));
        return istr;
    }

    String toInterpolateValue(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof String ||
                v instanceof Timestamp ||
                v instanceof Date ||
                v instanceof java.util.Date) {
            return Constants.QUOTE_SINGLE + v.toString() + Constants.QUOTE_SINGLE;
        }
        return v.toString();
    }
}
