package com.kk.setl.core;

import com.kk.setl.model.Def;
import com.kk.setl.model.Row;
import com.kk.setl.model.Status;
import com.kk.setl.utils.Chrono;
import com.kk.setl.utils.RowSetUtil;

import oracle.sql.ROWID;

import org.pmw.tinylog.Logger;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.IntStream;

import javax.sql.rowset.JdbcRowSet;

public class SetlProcessor implements Runnable {
    public final int DEFAULT_NUM_THREADS = 6;

    final BlockingQueue<Row> queue;
    final Status status;
    final Def def;
    final Map<String, Integer> fromColumns;
    RowSetUtil rowSetUtil;
    long minvalue = 0;
    long maxvalue = 0;
    long recCounter = 0;
    int recIncCounter = 0;
    long diff = 0;
    /**
     * constructor
     *
     * @param status
     * @param def
     */
    public SetlProcessor(final Status status, final Def def) {
        this.status = status;
        this.def = def;
        this.queue = new LinkedBlockingDeque<>(getNumThreads());
        this.fromColumns = new HashMap<>();
        this.rowSetUtil = RowSetUtil.getInstance();
    }

    /**
     * Thread runner - initiates process
     */
    @Override
    public void run() {
        process();
    }

    void process() {
        status.reset();
        Chrono ch = Chrono.start("Processor");
        
        if(null != def.getExtract().getTable() && !def.getExtract().getTable().isEmpty())
        {
	        //Thread et = startExtractor();
	        List<Thread> et = startExtract();
	        List<Thread> lts = startLoaders();
	
	        try {
	            
	            for (Thread lt : et) {
	            	lt.join();
	            }
	            
	            //et.join();
	            
	            for (Thread lt : lts) {
	                lt.join();
	            }
	        } catch (InterruptedException ie) {}
	        ch.stop();
	        
        }else    {
        	Thread et = startExtractor();
	        List<Thread> lts = startLoaders();
	
	        try {
	            
	        	et.join();	            
	            for (Thread lt : lts) {
	                lt.join();
	            }
	        } catch (InterruptedException ie) {}
	        ch.stop();
        }
    }

    /**
     * start extractor thread
     *
     * @return
     */
    Thread startExtractor() {
        Logger.info("Starting Extractor thread");
        Extractor extractor = new Extractor(queue, def, status, (result) -> {
            IntStream.range(0, getNumThreads()).forEach((i) -> addDoneRow());
        });
        Thread et = new Thread(extractor, "extractor");
        et.start();
        Logger.debug("Extractor thread {} started.", et.getName());

        return et;
    }
    
    List<Thread> startExtract() {
        Logger.info("Starting Extracter threads. noOfThreads={}", getNumExtThreads());
        List<Row> row = null;
        String sql = "";
        
        List<Thread> lts = new ArrayList<>();        
        try (JdbcRowSet jrs = rowSetUtil.getRowSet(def.getFromDS())) {
        	sql = "select min(r) start_id, max(r) end_id from (SELECT ntile("+getNumExtThreads()+") over (order by rowid) grp, rowid r FROM   "+def.getExtract().getTable()+") group  by grp";
            jrs.setCommand(sql);
            jrs.execute();
            jrs.setFetchDirection(ResultSet.FETCH_FORWARD);

            ResultSetMetaData meta = jrs.getMetaData();
            
            initFromColumns(meta);
            row = parseData(jrs, meta);
        } catch (Exception e) {
            Logger.error("error in extraction: {}", e.getMessage());
            Logger.debug(e);
        }
        diff = (maxvalue - minvalue)/getNumExtThreads();
        diff++ ;
        recCounter = minvalue;
        if(null != row && row.size() > 0) {
        	for(Row irow : row) {
	        	IntStream.range(0, getNumExtThreads()).forEach((k) -> {
	        		Extractor extractor = new Extractor(queue, def, status, (ROWID)irow.get("start_id"), (ROWID)irow.get("end_id"), (result) -> {
		                IntStream.range(0, getNumThreads()).forEach((j) -> addDoneRow());
		            });
		            Thread et = new Thread(extractor, "extractor"+k);
		            et.start();
		            Logger.info("Extractor thread {} started."+" "+irow.get("start_id")+" "+irow.get("end_id"), et.getName());
		            lts.add(et);
	        	});
        	}
        }
        return lts;
    }

    /**
     * adds DONE row to indicate the loader thread can end
     */
    void addDoneRow() {
        try {
            queue.put(Row.DONE);
        } catch (InterruptedException ie) {}
    }

    /**
     * starts loader threads
     *
     * @return
     */
    List<Thread> startLoaders() {
        Logger.info("Starting Loader threads. noOfThreads={}", getNumThreads());
        List<Thread> lts = new ArrayList<>();
        IntStream.range(0, getNumThreads()).forEach((i) -> {
            Loader loader = new Loader("l"+i, queue, status, def);
            Thread lt = new Thread(loader);
            lt.start();
            lts.add(lt);
            Logger.debug("Loader thread {} started.", lt.getName());
        });

        return lts;
    }

    /**
     * determines the number of threads from definition or default
     *
     * @return
     */
    int getNumThreads() {
        if (def != null && def.getThreads() > 0) {
            return def.getThreads();
        }
        return DEFAULT_NUM_THREADS;
    }
    
    int getNumExtThreads() {
        if (def != null && def.getExtthread() > 0) {
            return def.getExtthread();
        }
        return DEFAULT_NUM_THREADS;
    }
    
    void initFromColumns(ResultSetMetaData meta) throws SQLException {
        fromColumns.putAll(rowSetUtil.getMetaColumns(meta));
    }
    
    List<Row> parseData(JdbcRowSet jrs, ResultSetMetaData meta) throws SQLException {
    	Row row = null;
    	List<Row> rowArray = new ArrayList<Row>();
        if (jrs == null || meta == null) {
            return null;
        }

        while (jrs.next()) {
            row = parseDataRow(jrs, meta);
            rowArray.add(row);
        }
        return rowArray;
    }

    Row parseDataRow(JdbcRowSet jrs, ResultSetMetaData meta) throws SQLException {
        if (jrs == null || meta == null) {
            return null;
        }

        int colCount = meta.getColumnCount();

        Map<String, Object> row = new HashMap<>();
        for (int c = 1; c <= colCount; c++) {
            row.put(meta.getColumnName(c).toLowerCase(), jrs.getObject(c));
        }

        Row ro = new Row(fromColumns, row);
        return ro;
    }
    
}
