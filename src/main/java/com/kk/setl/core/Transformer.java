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
package com.kk.setl.core;

import com.kk.setl.model.Column;
import com.kk.setl.model.Def;
import com.kk.setl.utils.Interpolator;
import com.kk.setl.utils.SetlCache;
import com.kk.setl.utils.SqlRunner;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pmw.tinylog.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Transformer {
    private final Def def;

    protected SetlCache cache;
    protected Interpolator interpolator;
    protected SqlRunner sqlRunner;

    /**
     * constructor
     *
     * @param def
     */
    public Transformer(final Def def) {
        this.def = def;
        this.cache = SetlCache.getInstance();
        this.interpolator = Interpolator.getInstance();
        this.sqlRunner = SqlRunner.getInstance();
    }

    /**
     * - identifies transform columns and process them one by one
     * - interpolates sql variables with values
     * - check cache for sql results, if not available runs sql and caches
     *
     * @param row
     * @return
     */
    public Map<String, Object> transform(final Map<String, Object> row) {
        if (def == null || def.getTransform() == null) {
            return new HashMap<>();
        }

        final List<Column> columns = def.getTransform().getColumns();
        if (CollectionUtils.isEmpty(columns)) {
            Logger.trace("no columns in transform config");
            return new HashMap<>();
        }

        columns.stream().forEach(col -> transformSql(row, col));
        return row;
    }

    /**
     * - interpolates sql variables with values
     * - check cache for sql results, if not available runs sql and caches
     *
     * @param row
     * @param col
     */
    void transformSql(final Map<String, Object> row, final Column col) {
        if (row == null || col == null || StringUtils.isEmpty(col.getSql())) {
            return;
        }
        String sql = interpolator.interpolate(col.getSql(), row);

        if (col.getCache() && cache.exists(sql)) {
            row.put(col.getName(), cache.get(sql));
        } else {
            Object v = sqlRunner.getSingleValue(sql, def.getToDS());
            row.put(col.getName(), v);
            cache.set(sql, v);
        }
    }
}
