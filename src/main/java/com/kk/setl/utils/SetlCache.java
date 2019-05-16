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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SetlCache {

    private static final int MAX_SIZE = 25000;
    private static SetlCache _instance = null;

    private final ConcurrentMap<String, Object> _cache;

    protected SetlCache() {
        this._cache = new ConcurrentHashMap<>(128);
    }

    public static SetlCache getInstance() {
        if (_instance == null) {
            _instance = new SetlCache();
        }
        return _instance;
    }

    public boolean exists(String key) {
        if (key == null) {
            return false;
        }
        return _cache.containsKey(key);
    }

    public Object get(String key) {
        if (key == null) {
            return null;
        }
        return _cache.get(key);
    }

    public void set(String key, Object val) {
        if (key == null || val == null) {
            return;
        }
        synchronized (_cache) {
            if (_cache.size() < MAX_SIZE) {
                _cache.put(key, val);
            }
        }
    }

    public int size() {
        return _cache.size();
    }

    public void clear() {
        synchronized (_cache) {
            _cache.clear();
        }
    }

}
