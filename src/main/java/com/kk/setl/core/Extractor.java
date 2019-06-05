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

import com.kk.setl.model.Def;
import com.kk.setl.model.Row;
import com.kk.setl.model.Status;
import com.kk.setl.utils.CsvParser;
import com.kk.setl.utils.RowSetUtil;

import oracle.sql.ROWID;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.pmw.tinylog.Logger;

import javax.sql.rowset.JdbcRowSet;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Consumer;

public class Extractor implements Runnable {
    final BlockingQueue<Row> queue;
    final Status status;
    final Def def;
    final Map<String, Integer> fromColumns;
    final Consumer<Boolean> doneCallback;
    final ROWID minval;
    final ROWID maxval;
    RowSetUtil rowSetUtil;

    /**
     * constructor
     *
     * @param queue
     * @param def
     * @param status
     * @param doneCallback
     */
    public Extractor(BlockingQueue<Row> queue,
                     Def def,
                     Status status,
                     ROWID minval,
                     ROWID maxval,
                     /*long minval,
                     long maxval,*/
                     Consumer<Boolean> doneCallback) {
        this.queue = queue;
        this.def = def;
        this.status = status;
        this.fromColumns = new HashMap<>();
        this.doneCallback = doneCallback;
        this.rowSetUtil = RowSetUtil.getInstance();
        this.minval = minval;
        this.maxval = maxval;
    }
    
    public Extractor(BlockingQueue<Row> queue,
            Def def,
            Status status,
            Consumer<Boolean> doneCallback) {
			this.queue = queue;
			this.def = def;
			this.status = status;
			this.fromColumns = new HashMap<>();
			this.doneCallback = doneCallback;
			this.rowSetUtil = RowSetUtil.getInstance();
			this.minval = null;
	        this.maxval = null;
	}

    /**
     * thread runner
     */
    @Override
    public void run() {
        boolean result = true;
        Logger.info("Extractor is starting...");
        try {
            result = extract();
        } finally {
            Logger.info("Extractor is completed. result={}", result);
            doneCallback.accept(result);
        }
    }

    /**
     * extracts data from three type of sources (in priority order)
     * - sql
     * - data elements
     * - csv file
     *
     * @return
     */
    boolean extract() {
        status.reset();

        if (def == null || def.getExtract() == null) {
            return false;
        }

        boolean result = false;
        if (StringUtils.isNotEmpty(def.getExtract().getSql())) {
            //Logger.debug("extracting from sql: {}", def.getExtract().getSql());
            //Logger.info("extracting from sql: {}", def.getExtthread());
            result = extractDataFromSql();
        } else if (CollectionUtils.isNotEmpty(def.getExtract().getData())) {
            Logger.info("extracting from data");
            result = extractDataFromData();
        } else if (def.getExtract().getCsv() != null) {
            Logger.info("extracting from CSV: {}", def.getExtract().getCsv().getFilePath());
            result = extractDataFromCsv();
        } else {
            Logger.warn("invalid source sql/csv configuration. skipping ETL.");
            return false;
        }

        return result;
    }

    /**
     * extract data from data element in json
     *
     * @return
     */
    boolean extractDataFromData() {
        if (def.getExtract() == null || def.getExtract().getData() == null) {
            Logger.info("extract config is missing. skipping extraction");
            return true;
        }

        def.getExtract().getData().stream().forEach(row -> {
            addToQueue(new Row(fromColumns, row.getValues()));
        });

        return true;
    }

    /**
     * extract data from csv file
     *
     * @return
     */
    boolean extractDataFromCsv() {
        if (def.getExtract() == null || def.getExtract().getCsv() == null) {
            Logger.info("extract config is missing. skipping extraction");
            return true;
        }

        final CsvParser parser = CsvParser.getInstance(def.getExtract().getCsv());
        final List<Map<String, Object>> data = parser.parse();

        data.stream().forEach(row -> {
            addToQueue(new Row(fromColumns, row));
        });

        return true;
    }

    /**
     * extract data using sql definition
     *
     * @return
     */
    boolean extractDataFromSql() {
        if (def.getExtract() == null || StringUtils.isEmpty(def.getExtract().getSql())) {
            Logger.info("extract config is missing. skipping extraction");
            return true;
        }
        String sql = def.getExtract().getSql();
        
        if(null != def.getExtract().getTable() && !def.getExtract().getTable().isEmpty()) {
        	if(null != def.getExtract().getWhereexists() && !def.getExtract().getWhereexists().isEmpty() && def.getExtract().getWhereexists().equalsIgnoreCase("true")) {
        		sql = sql + " AND ROWID BETWEEN '" + this.minval + "' AND '" + this.maxval + "'";
        	}else {
        		sql = sql + " WHERE ROWID BETWEEN '" + this.minval + "' AND '" + this.maxval + "'";
        	}
        }
        
        Logger.debug("error in extraction: {}", sql);
        
        try (JdbcRowSet jrs = rowSetUtil.getRowSet(def.getFromDS())) {
            jrs.setCommand(sql);
            jrs.execute();
            jrs.setFetchDirection(ResultSet.FETCH_FORWARD);
            jrs.setFetchSize(200);

            ResultSetMetaData meta = jrs.getMetaData();
            
            initFromColumns(meta);
            parseData(jrs, meta);
            return true;
        } catch (Exception e) {
            Logger.error("error in extraction: {}", e.getMessage());
            Logger.debug(e);
            return false;
        }
    }

    /**
     * initializes from columns
     *
     * @param meta
     * @throws SQLException
     */
    void initFromColumns(ResultSetMetaData meta) throws SQLException {
        fromColumns.putAll(rowSetUtil.getMetaColumns(meta));
    }

    /**
     * parse data from jdbc row set
     *
     * @param jrs
     * @param meta
     * @return
     * @throws SQLException
     */
    long parseData(JdbcRowSet jrs, ResultSetMetaData meta) throws SQLException {
        if (jrs == null || meta == null) {
            return 0;
        }

        long rowCount = 0;
        while (jrs.next()) {
            rowCount++;

            parseDataRow(jrs, meta);
            if (!isWithinLimit(rowCount)) {
                break;
            }
        }
        return rowCount;
    }

    /**
     * parses data row to {@link Row} object
     *
     * @param jrs
     * @param meta
     * @return {@link Row}
     * @throws SQLException
     */
    Row parseDataRow(JdbcRowSet jrs, ResultSetMetaData meta) throws SQLException {
        if (jrs == null || meta == null) {
            return null;
        }

        int colCount = meta.getColumnCount();

        Map<String, Object> row = new HashMap<>();
        for (int c = 1; c <= colCount; c++) {
        	//JGeometry j_geom = JGeometry.load((STRUCT) jrs.getObject(c));
            row.put(meta.getColumnName(c).toLowerCase(), jrs.getObject(c));
            //row.put(meta.getColumnName(c).toLowerCase(), j_geom);
        }

        Row ro = new Row(fromColumns, row);
        addToQueue(ro);
        return ro;
    }

    /**
     * adds the row to blocking queue and holds the queue any loader threads
     *
     * @param row
     */
    void addToQueue(Row row) {
        try {
            queue.put(row);
            status.incrementFound();
        } catch (InterruptedException ie) {}
    }

    /**
     * checks if rowcount is within the specified limit in json config
     *
     * @param rowCount
     * @return true | false
     */
    boolean isWithinLimit(long rowCount) {
        return def.getExtract().getLimitRows() == 0 || rowCount <= def.getExtract().getLimitRows();
    }

}
