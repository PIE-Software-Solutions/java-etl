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

import org.codehaus.jackson.annotate.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.annotation.Generated;
import java.sql.Types;
import java.util.*;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "table",
        "columns",
        "mode"
})
public class Load {

    @JsonProperty("table")
    private String table;
    @JsonProperty("columns")
    private List<Column> columns = new ArrayList<Column>();
    @JsonProperty("mode")
    private String mode;
    @JsonProperty("pre")
    private PrePost pre;
    @JsonProperty("post")
    private PrePost post;
    @JsonProperty("returns")
    private Set<String> returns = new HashSet<>();

    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    @JsonIgnore
    private String sqlExists;
    @JsonIgnore
    private boolean shouldUpdate = true;
    @JsonIgnore
    private boolean shouldInsert = true;
    @JsonIgnore
    private Map<String, Integer> toColumns;

    /**
     *
     * @return
     * The table
     */
    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    /**
     *
     * @param table
     * The table
     */
    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    /**
     *
     * @return
     * The columns
     */
    @JsonProperty("columns")
    public List<Column> getColumns() {
        return columns;
    }

    /**
     *
     * @param columns
     * The columns
     */
    @JsonProperty("columns")
    public void setColumns(List<Column> columns) {
        this.columns = columns;
    }

    /**
     *
     * @return
     * The mode
     */
    @JsonProperty("mode")
    public String getMode() {
        return mode;
    }

    /**
     *
     * @param mode
     * The mode
     */
    @JsonProperty("mode")
    public void setMode(String mode) {
        this.mode = mode;
    }

    /**
     *
     * @return Pre
     */
    @JsonProperty("pre")
    public PrePost getPre() {
        return pre;
    }

    /**
     *
     * @param pre
     */
    @JsonProperty("pre")
    public void setPre(PrePost pre) {
        this.pre = pre;
    }

    /**
     *
     * @return post
     */
    @JsonProperty("post")
    public PrePost getPost() {
        return post;
    }

    /**
     * return columns
     * @return
     */
    @JsonProperty("returns")
    public Set<String> getReturns() {
        return returns;
    }

    /**
     * return columns
     * @return
     */
    @JsonProperty("returns")
    public void setReturns(Set<String> returns) {
        this.returns = returns;
    }

    /**
     *
     * @param post
     */
    @JsonProperty("post")
    public void setPost(PrePost post) {
        this.post = post;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    /**
     * sqlExists
     * @return
     */
    public String getSqlExists() {
        return sqlExists;
    }

    public void setSqlExists(String sqlExists) {
        this.sqlExists = sqlExists;
    }

    /**
     * should update
     * @return
     */
    public boolean isShouldUpdate() {
        return shouldUpdate;
    }

    public void setShouldUpdate(boolean shouldUpdate) {
        this.shouldUpdate = shouldUpdate;
    }

    /**
     * shoud insert
     * @return
     */
    public boolean isShouldInsert() {
        return shouldInsert;
    }

    public void setShouldInsert(boolean shouldInsert) {
        this.shouldInsert = shouldInsert;
    }

    /**
     * toColumns
     * @return
     */
    public Map<String, Integer> getToColumns() {
        return toColumns;
    }

    public void setToColumns(Map<String, Integer> toColumns) {
        this.toColumns = toColumns;
    }

    /**
     * get to column type
     * @param column
     * @return
     */
    public int getToColumnType(String column) {
        if (toColumns == null) {
            return Types.STRUCT;
        }
        Integer sqlType = toColumns.get(column);
        if (sqlType != null) {
            return sqlType;
        }
        return Types.VARCHAR;
    }
}