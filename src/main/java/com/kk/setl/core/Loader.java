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

import com.kk.setl.enums.SqlType;
import com.kk.setl.model.*;
import com.kk.setl.utils.*;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.pmw.tinylog.Logger;
import org.pmw.tinylog.LoggingContext;

import javax.sql.rowset.JdbcRowSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public class Loader implements Runnable {
    private String id;
    protected final BlockingQueue<Row> queue;
    protected final Status status;
    protected final Def def;
    protected final SetlCache cache;
    protected final SystemVar systemVar;

    protected Interpolator interpolator;
    protected SqlRunner sqlRunner;
    protected RowSetUtil rowSetUtil;
    protected Transformer transformer;

    protected int processed = 0;

    /**
     * constructor
     *
     * @param id
     * @param queue
     * @param status
     * @param def
     */
    public Loader(String id,
                  BlockingQueue<Row> queue,
                  Status status,
                  Def def) {
        this.id = id;
        this.queue = queue;
        this.status = status;
        this.def = def;
        this.cache = SetlCache.getInstance();
        this.interpolator = Interpolator.getInstance();
        this.sqlRunner = SqlRunner.getInstance();
        this.rowSetUtil = RowSetUtil.getInstance();
        transformer = new Transformer(def);

        this.systemVar = SystemVar.getInstance();

        buildSqlExistsAll();
    }

    /**
     * thread runner
     */
    @Override
    public void run() {
        load();
    }

    /**
     * load definitions
     */
    protected void load() {
        Logger.debug("Loader started.");
        processed = 0;
        while(true) {
            Row row = getRowFromQueue();
            if (row == null || row.isDone()) {
                break;
            }

            if (transformer != null) {
                transformer.transform(row.getData());
            }

            boolean result = loadRow(row);
            if (status != null) {
                status.incrementProcessed(true);
                if (!result) {
                    status.incrementFailed();
                }
            }
            processed++;
        }
        Logger.debug("Loader completed (total processed: {}).", processed);
    }

    /**
     * take the next available row from extractor queue
     *
     * @return
     */
    protected Row getRowFromQueue() {
        if (queue == null) {
            return null;
        }

        try {
            return queue.take();
        } catch (InterruptedException ie) {}

        return null;
    }

    /**
     * loads row into database
     *
     * @param row
     * @return
     */
    protected boolean loadRow(Row row) {
        if (def == null) {
            return false;
        }

        try (JdbcRowSet jrs = rowSetUtil.getRowSet(def.getToDS())) {
            for (Load load: def.getLoads()) {
                LoggingContext.put("load", load.getTable());
                insertOrUpdateRow(load, row, jrs);
            }
            return true;
        } catch (SQLException sqle) {
            Logger.error("Load row error: {}", sqle.getMessage());
            return false;
        }
    }

    /**
     * insert or update row into database
     *
     * @param load
     * @param row
     * @param jrs
     * @return
     * @throws SQLException
     */
    protected boolean insertOrUpdateRow(Load load, Row row, JdbcRowSet jrs) throws SQLException {
        if (load == null || row == null || jrs == null) {
            return false;
        }

        boolean exists = isRowExists(load, row, jrs);
        initLoadToColumns(load, jrs);

        boolean result = true;
        if (exists) {
            if (load.isShouldUpdate()) {
                result = updateRow(load, row, jrs);
                if (result && status != null) {
                    status.incrementUpdated();
                }
            }
        } else {
            if (load.isShouldInsert()) {
                result = insertRow(load, row, jrs);
                if (result && status != null) {
                    status.incrementInserted();
                }
            }
        }
        processReturns(load, row, jrs);
        return result;
    }

    /**
     * process and store return values from previous load
     *
     * @param load
     * @param row
     * @param jrs
     * @return
     */
    protected boolean processReturns(Load load, Row row, JdbcRowSet jrs) {
        if (load == null || row == null || jrs == null) {
            return false;
        }

        if (CollectionUtils.isEmpty(load.getReturns())) {
            return true;
        }

        for (String rc : load.getReturns()) {
            try {
                Object rcv = jrs.getObject(rc);
                row.getData().put("rc_" + load.getTable() + "_" + rc, rcv);
            } catch (SQLException sqle) {
                Logger.warn("return value error: " + sqle.getMessage());
                Logger.trace(sqle);
            }
        }
        return true;
    }

    /**
     * check if row exists in target database
     *
     * @param load
     * @param row
     * @param jrs
     * @return
     * @throws SQLException
     */
    protected boolean isRowExists(Load load, Row row, JdbcRowSet jrs) throws SQLException {
        if (load == null || row == null || jrs == null) {
            return false;
        }

        String sql = interpolator.interpolate(load.getSqlExists(), row.getData());

        jrs.setCommand(sql);
        jrs.execute();
        return jrs.next();
    }

    /**
     * prepares and sets columns value and updates row
     * 
     * @param load
     * @param row
     * @param jrs
     * @return
     */
    protected boolean updateRow(Load load, Row row, JdbcRowSet jrs) {
        if (load == null || row == null || jrs == null) {
            return false;
        }
        try {
            for (Column col : CollectionUtils.emptyIfNull(load.getColumns())) {
                if (col.getAuto() || col.getNk() || !col.isUpdate() || isNotEmpty(col.getGenerator())) {
                    continue;
                }
                setColumnValue(load, row, col, jrs);
            }

            jrs.updateRow();
            return true;
        } catch (SQLException e) {
            Logger.warn("updateRow failed: " + e.getMessage());
            Logger.trace(e);
            return false;
        }
    }

    /**
     * prepare and set columns value and inserts row
     *
     * @param load
     * @param row
     * @param jrs
     * @return
     */
    protected boolean insertRow(Load load, Row row, JdbcRowSet jrs) {
        if (load == null || row == null || jrs == null) {
            return false;
        }
        try {
            jrs.moveToInsertRow();

            for (Column col : CollectionUtils.emptyIfNull(load.getColumns())) {
                if (col.getAuto()) {
                    continue;
                }
                setColumnValue(load, row, col, jrs);
            }

            jrs.insertRow();
            // jrs.first(); TODO check the need of first() method
            return true;
        } catch (SQLException sqle) {
            Logger.warn("insertRow failed: " + sqle.getMessage());
            Logger.trace(sqle);
            return false;
        }
    }

    /**
     * sets column value to rowSet from given row data or generators
     *
     * @param load
     * @param row
     * @param column
     * @param jrs
     * @throws SQLException
     */
    protected void setColumnValue(Load load, Row row, Column column, JdbcRowSet jrs) throws SQLException {
        if (load == null || column == null || jrs == null) {
            return;
        }
        Object val = null;
        if (isNotEmpty(column.getGenerator())) {
            val = sqlRunner.getSingleValue(column.getGenerator(), def.getToDS());
        } else {
            val = getColumnValue(load, row, column);
        }
        if (val != null) {
        	if(isNotEmpty(column.getType()) && column.getType().contains("SDO_GEOMETRY")){
        		/*@SuppressWarnings("deprecation")
				JGeometry j_geom = JGeometry.load((STRUCT) val);*/
        		jrs.updateObject(column.getName(), val);
        	}else {
        		jrs.updateObject(column.getName(), val);
        	}
        }
    }

    /**
     * returns columnValue from Row, Value or Sql
     * also processes system variables
     *
     * @param load
     * @param row
     * @param column
     * @return
     */
    protected Object getColumnValue(Load load, Row row, Column column) {
        if (load == null || column == null) {
            return null;
        }

        Object val = null;
        if (isNotEmpty(column.getValue())) {
            val = column.getValue();
        } else if (isNotEmpty(column.getRef()) && row != null && MapUtils.isNotEmpty(row.getData())) {
            val = row.get(column.getRef());
            if(isNotEmpty(column.getType()) && column.getType().contains("SDO_GEOMETRY")){
            	return val;
            }
        } else if (isNotEmpty(column.getSql()) && row != null  && MapUtils.isNotEmpty(row.getData())) {
            String sql = interpolator.interpolate(column.getSql(), row.getData());
            val = sqlRunner.getSingleValue(sql, def.getToDS());
        }

        if (val == null) {
            return null;
        }
        val = systemVar.process(val);

        SqlType sqlType = SqlType.forValue(load.getToColumnType(column.getName()));
        if (sqlType != null) {
            return sqlType.convert(val);
        }
        return val.toString();
    }

    /**
     * initialize load TO columns - from datasource if not found already
     *
     * @param load
     * @param jrs
     */
    protected void initLoadToColumns(Load load, JdbcRowSet jrs) {
        if (load == null || MapUtils.isNotEmpty(load.getToColumns())) {
            return;
        }
        Map<String, Integer> mcs = rowSetUtil.getMetaColumns(jrs);
        Map<String, Integer> lcs = new HashMap<>();
        for (String c : MapUtils.emptyIfNull(mcs).keySet()) {
            lcs.put(c, mcs.get(c));
        }
        load.setToColumns(lcs);
    }

    /**
     * builds sqlExists for loads and persists
     */
    protected void buildSqlExistsAll() {
        if (def == null) {
            return;
        }

        for (Load load : def.getLoads()) {
            load.setSqlExists(buildSqlExists(load));
        }
    }

    /**
     * build "exists" sql
     *
     * @param load
     * @return
     */
    protected String buildSqlExists(Load load) {
        if (load == null || StringUtils.isEmpty(load.getTable())) {
            return "";
        }

        final List<Column> nks = new ArrayList<>();
        load.getColumns().stream()
                .filter(Column::getNk)
                .forEach(nks::add);

        String cols = buildSelectColumns(load);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(cols);
        sql.append(" FROM ").append(load.getTable());
        sql.append(" WHERE ");

        if (CollectionUtils.isEmpty(nks)) {
            sql.append("1 = 2");
        } else {
            List<String> nkStr = nks.stream()
                    .filter(nk -> isNotEmpty(nk.getRef()))
                    .map((nk) -> nk.getName() + " = :" + nk.getRef())
                    .collect(Collectors.toList());
            sql.append(StringUtils.join(nkStr, " AND "));
        }

        Logger.debug("load={}, sqlExists={}", load.getTable(), sql.toString());
        return sql.toString();
    }

    /**
     * build select columns from database
     *
     * @param load
     * @return
     */
    protected String buildSelectColumns(Load load) {
        if (load == null || StringUtils.isEmpty(load.getTable())) {
            return "";
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM ").append(load.getTable()).append(" WHERE 1>2");

        final Set<String> cols = new LinkedHashSet<>();
        try (JdbcRowSet cjrs = rowSetUtil.getRowSet(def.getToDS())) {
            cjrs.setCommand(sql.toString());
            cjrs.execute();
            ResultSetMetaData meta = cjrs.getMetaData();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                cols.add(meta.getColumnName(i).toLowerCase());
            }
        } catch (SQLException sqle) {
            Logger.error("buildSelectColumns failed: " + sqle.getMessage());
            Logger.trace(sqle);
        }
        String colStr = StringUtils.join(cols, ", ");
        Logger.debug("load={}, selectColumns={}", load.getTable(), sql.toString());
        return colStr;
    }
}
