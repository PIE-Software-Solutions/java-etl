package com.kk.setl;

import com.kk.setl.core.SetlProcessor;
import com.kk.setl.model.DS;
import com.kk.setl.model.Def;
import com.kk.setl.model.Load;
import com.kk.setl.model.Status;
import com.kk.setl.utils.Chrono;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.pmw.tinylog.Logger;
import org.pmw.tinylog.LoggingContext;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class Setl {
    private static final int STATUS_EVERY = 1000;

    /**
     * load definition file
     * @param filePath
     * @return
     */
    protected Def loadFile(String filePath) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Path path = Paths.get(filePath);
            final Def def = mapper.readValue(path.toFile(), Def.class);
            def.setFilePath(path);

            initCsvPaths(def, path);

            loadDataStores(def);

            LoggingContext.put("def", StringUtils.defaultIfBlank(def.getName(), ""));

            if (def.getFromDS() != null) {
                Logger.info("fromDS = {}@{}", def.getFromDS().getUsername(), def.getFromDS().getUrl());
            }
            if (def.getToDS() != null) {
                Logger.info("toDS = {}@{}", def.getToDS().getUsername(), def.getToDS().getUrl());
            }

            return def;
        } catch (Exception e) {
            Logger.error("Invalid definition file: {}", e.getMessage());
            Logger.trace(e);
        }
        return null;
    }

    /**
     * load dataSources from default db.json file when def file skips them
     *
     * @param def
     */
    protected void loadDataStores(final Def def) {
        if (def == null || def.getFilePath() == null) {
            return;
        }

        Path dbPath = def.getFilePath().resolveSibling("db.json");
        if (dbPath == null || !dbPath.toFile().exists()) {
            // resolve from parent folders
            Path parent = def.getFilePath().getParent();
            while (parent != null) {
                dbPath = parent.resolveSibling("db.json");
                if (dbPath != null && dbPath.toFile().exists()) {
                    break;
                }
                parent = parent.getParent();
            }
            if (dbPath == null || !dbPath.toFile().exists()) {
                return;
            }
        }

        try {
            Def dbDef = new ObjectMapper().readValue(dbPath.toFile(), Def.class);

            if (def.getFromDS() == null && dbDef.getFromDS() != null) {
                def.setFromDS(new DS());
                def.getFromDS().setUrl(dbDef.getFromDS().getUrl());
                def.getFromDS().setUsername(dbDef.getFromDS().getUsername());
                def.getFromDS().setPassword(dbDef.getFromDS().getPassword());
            }
            if (def.getToDS() == null && dbDef.getToDS() != null) {
                def.setToDS(new DS());
                def.getToDS().setUrl(dbDef.getToDS().getUrl());
                def.getToDS().setUsername(dbDef.getToDS().getUsername());
                def.getToDS().setPassword(dbDef.getToDS().getPassword());
            }
        } catch (Exception e) {
            Logger.error("DB.json error: {}", e.getMessage());
            // just ignore
        }
    }

    /**
     * init csv paths
     *
     * @param def
     * @param defPath
     */
    protected void initCsvPaths(final Def def, Path defPath) {
        if (def.getExtract().getCsv() != null && StringUtils.isNotEmpty(def.getExtract().getCsv().getFile())) {
            Path csvPath = defPath.getParent().resolve(def.getExtract().getCsv().getFile()).normalize();
            def.getExtract().getCsv().setFilePath(csvPath.toAbsolutePath().toString());
        }

        for (Load load : def.getLoads()) {
            if (load.getPre() != null && load.getPre().getCsv() != null && StringUtils.isNotEmpty(load.getPre().getCsv().getFile())) {
                Path csvPath = defPath.getParent().resolve(load.getPre().getCsv().getFile()).normalize();
                load.getPre().getCsv().setFilePath(csvPath.toAbsolutePath().toString());
            }
            if (load.getPost() != null && load.getPost().getCsv() != null && StringUtils.isNotEmpty(load.getPost().getCsv().getFile())) {
                Path csvPath = defPath.getParent().resolve(load.getPost().getCsv().getFile()).normalize();
                load.getPost().getCsv().setFilePath(csvPath.toAbsolutePath().toString());
            }
        }
    }

    /**
     * start the process
     * @param def
     */
    protected void start(final Def def) {
        if (def == null) {
            Logger.error("Invalid or blank definition");
            return;
        }

        LoggingContext.put("defName", def.getName());
        LoggingContext.put("loadTable", "");

        Logger.info("Processing " + def.getName());

        Status status = new Status((s) -> printStatEvery(s));
        SetlProcessor processor = new SetlProcessor(status, def);
        Thread t = new Thread(processor);
        t.run();

        try {
            t.join();
        } catch (InterruptedException ie) {}

        printStatAll(status);
    }

    /**
     * print stats every
     * @param status
     */
    protected void printStatEvery(Status status) {
        if (status.getRowsFound() % STATUS_EVERY == 0) {
            Logger.info("Found: {}, Processed: {}", status.getRowsFound(), status.getRowsProcessed());
        }
    }

    /**
     * print stats
     * @param status
     */
    protected void printStatAll(Status status) {
        Logger.info("Found: {}, Processed: {}, failed: {}, inserted: {}, updated: {}",
                status.getRowsFound(), status.getRowsProcessed(), status.getRowsFailed(), status.getRowsInserted(), status.getRowsUpdated());
    }

    /**
     * check if valid file
     *
     * @param file
     * @return
     */
    protected String getValidPath(final String file) {
        if (StringUtils.isEmpty(file)) {
            return null;
        }
        // mac ~ fix
        String path = file.replaceFirst("^~", System.getProperty("user.home"));

        File f = Paths.get(path).toFile();
        if (!f.exists()) {
            System.out.println("Invalid file path: " + f.getAbsolutePath());
            return null;
        }

        LoggingContext.put("file", f.getName());
        return f.getAbsolutePath();
    }

    /**
     * main runner
     * @param args
     */
    public static void main(String[] args) {
        LoggingContext.put("def", "");
        LoggingContext.put("load", "");
        LoggingContext.put("file", "");

        if (args == null || args.length == 0) {
            Logger.error("Definition file (json) is missing");
            return;
        }

        Chrono kch = Chrono.start("all");
        Setl me = new Setl();
        for (String file : args) {
            String filePath = me.getValidPath(file);
            if (StringUtils.isNotEmpty(filePath)) {
                Logger.info("Processing definiton: " + file);
                Def def = me.loadFile(filePath);
                me.start(def);
            }
        }

        kch.stop();
        Logger.info("ALL DONE!");
    }

}
