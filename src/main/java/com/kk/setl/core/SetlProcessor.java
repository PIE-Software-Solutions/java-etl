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
import com.kk.setl.utils.Chrono;

import org.pmw.tinylog.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.IntStream;

public class SetlProcessor implements Runnable {
    public final int DEFAULT_NUM_THREADS = 6;

    final BlockingQueue<Row> queue;
    final Status status;
    final Def def;

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
}
