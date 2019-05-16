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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

import com.kk.setl.model.Csv;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvParser {

    protected Csv csv;

    public static CsvParser getInstance(Csv csv) {
        CsvParser parser = new CsvParser();
        parser.csv = csv;
        return parser;
    }

    protected CsvParser() {
        // not allowed
    }

    /**
     * parse csv file data
     * @return
     */
    public List<Map<String, Object>> parse() {
        final List<Map<String, Object>> data = new ArrayList<>();
        if (csv == null || StringUtils.isEmpty(csv.getFilePath())) {
            return data;
        }

        File file = new File(csv.getFilePath());
        if (!file.exists()) {
            return data;
        }

        try {
            CSVParser parser = parseFile(file);
            for (CSVRecord record : parser) {
                processRecord(data, record);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return data;
    }

    /**
     * parse file
     * @param file
     * @return
     * @throws IOException
     */
    protected CSVParser parseFile(File file) throws IOException {
        if (file == null) {
            return null;
        }
        return CSVParser.parse(file, Charset.defaultCharset(), CSVFormat.DEFAULT);
    }

    /**
     * parse csv record
     * @param data
     * @param record
     */
    protected void processRecord(final List<Map<String, Object>> data, final CSVRecord record) {
        if (data == null || record == null) {
            return;
        }

        final Map<String, Object> row = new HashMap<>();
        for (int i = 0; i < csv.getColumns().size(); i++) {
            if (i >= record.size()) {
                break;
            }
            row.put(csv.getColumns().get(i), record.get(i));
        }
        data.add(row);
    }

}
