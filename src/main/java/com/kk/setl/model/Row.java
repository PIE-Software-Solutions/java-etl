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
package com.kk.setl.model;

import org.apache.commons.collections4.MapUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Row implements Serializable {
    private final Map<String, Integer> columns;
    private final Map<String, Object> data;

    public static Row DONE = new Row(new HashMap<>(), new HashMap<>()) {
        @Override
        public boolean isDone() {
            return true;
        }
    };

    public Row(Map<String, Integer> columns, Map<String, Object> data) {
        this.columns = MapUtils.emptyIfNull(columns);
        this.data = MapUtils.emptyIfNull(data);
    }

    public Map<String, Integer> getColumns() {
        return columns;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public Integer typeOf(String col) {
        return columns.get(col);
    }

    public Object get(String col) {
        return data.get(col);
    }

    public boolean isDone() {
        return false;
    }
}
