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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class Status {
    private AtomicLong rowsFound = new AtomicLong(0);
    private AtomicLong rowsProcessed = new AtomicLong(0);
    private AtomicLong rowsInserted = new AtomicLong(0);
    private AtomicLong rowsUpdated = new AtomicLong(0);
    private AtomicLong rowsFailed = new AtomicLong(0);

    private AtomicBoolean done = new AtomicBoolean(false);

    private final Consumer<Status> onUpdate;

    public Status(Consumer<Status> onUpdate) {
        this.onUpdate = onUpdate;
    }

    protected void onUpdateCallback() {
        if (onUpdate != null) {
            onUpdate.accept(this);
        }
    }

    public long getRowsFound() {
        return rowsFound.get();
    }

    public long getRowsProcessed() {
        return rowsProcessed.get();
    }

    public long getRowsInserted() {
        return rowsInserted.get();
    }

    public long getRowsUpdated() {
        return rowsUpdated.get();
    }

    public long getRowsFailed() {
        return rowsFailed.get();
    }

    public long incrementFound(boolean callback) {
        long c =  rowsFound.incrementAndGet();
        if (callback) {
            onUpdateCallback();
        }
        return c;
    }

    public long incrementFound() {
        return incrementFound(false);
    }

    public long incrementProcessed(boolean callback) {
        long c =  rowsProcessed.incrementAndGet();
        if (callback) {
            onUpdateCallback();
        }
        return c;
    }

    public long incrementProcessed() {
        return incrementProcessed(false);
    }

    public long incrementInserted(boolean callback) {
        long c = rowsInserted.incrementAndGet();
        if (callback) {
            onUpdateCallback();
        }
        return c;
    }

    public long incrementInserted() {
        return incrementInserted(false);
    }

    public long incrementUpdated(boolean callback) {
        long c = rowsUpdated.incrementAndGet();
        if (callback) {
            onUpdateCallback();
        }
        return c;
    }

    public long incrementUpdated() {
        return incrementUpdated(false);
    }

    public long incrementFailed(boolean callback) {
        long c = rowsFailed.incrementAndGet();
        if (callback) {
            onUpdateCallback();
        }
        return c;
    }

    public long incrementFailed() {
        return incrementFailed(false);
    }

    public boolean isDone() {
        return done.get();
    }

    public void markAsDone() {
        done.set(true);
        onUpdateCallback();
    }

    public void reset() {
        rowsProcessed = new AtomicLong(0);
        rowsInserted = new AtomicLong(0);
        rowsUpdated = new AtomicLong(0);
        rowsFailed = new AtomicLong(0);
        done = new AtomicBoolean(false);
    }
}
