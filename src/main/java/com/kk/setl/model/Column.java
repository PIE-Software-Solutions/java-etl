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
import java.util.HashMap;
import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "name",
        "sql",
        "cache"
})
public class Column {

    @JsonProperty("name")
    private String name;
    @JsonProperty("type")
    private String type;
    @JsonProperty("sql")
    private String sql;
    @JsonProperty("cache")
    private boolean cache;
    @JsonProperty("auto")
    private boolean auto; 
    @JsonProperty("nk")
    private boolean nk;
    @JsonProperty("generator")
    private String generator;
    @JsonProperty("ref")
    private String ref;
    @JsonProperty("value")
    private String value;
    @JsonProperty("is_update")
    private boolean isUpdate = true;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    /**
     *
     * @return
     * The name
     */
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    /**
     *
     * @param name
     * The name
     */
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return
     * The sql
     */
    @JsonProperty("sql")
    public String getSql() {
        return sql;
    }

    /**
     *
     * @param sql
     * The sql
     */
    @JsonProperty("sql")
    public void setSql(String sql) {
        this.sql = sql;
    }

    /**
     *
     * @return
     * The cache
     */
    @JsonProperty("cache")
    public boolean getCache() {
        return cache;
    }

    /**
     *
     * @param cache
     * The cache
     */
    @JsonProperty("cache")
    public void setCache(boolean cache) {
        this.cache = cache;
    }

    /**
     *
     * @return
     * The auto
     */
    @JsonProperty("auto")
    public boolean getAuto() {
        return auto;
    }

    /**
     *
     * @param auto
     * The auto
     */
    @JsonProperty("auto")
    public void setAuto(boolean auto) {
        this.auto = auto;
    }
    
    /**
     *
     * @return
     * The nk
     */
    @JsonProperty("nk")
    public boolean getNk() {
        return nk;
    }

    /**
     *
     * @param nk
     * The nk
     */
    @JsonProperty("nk")
    public void setNk(boolean nk) {
        this.nk = nk;
    }

    /**
     *
     * @return
     * The generator
     */
    @JsonProperty("generator")
    public String getGenerator() {
        return generator;
    }

    /**
     *
     * @param generator
     * The generator
     */
    @JsonProperty("generator")
    public void setGenerator(String generator) {
        this.generator = generator;
    }

    /**
     *
     * @return
     * The ref
     */
    @JsonProperty("ref")
    public String getRef() {
        return ref;
    }

    /**
     *
     * @param ref
     * The ref
     */
    @JsonProperty("ref")
    public void setRef(String ref) {
        this.ref = ref;
    }

    /**
     *
     * @return
     * The value
     */
    @JsonProperty("value")
    public String getValue() {
        return value;
    }

    /**
     * is update
     * @return
     */
    @JsonProperty("is_update")
    public boolean isUpdate() {
        return isUpdate;
    }

    @JsonProperty("is_update")
    public void setUpdate(boolean update) {
        isUpdate = update;
    }

    /**
     *
     * @param value
     * The value
     */
    @JsonProperty("value")
    public void setValue(String value) {
        this.value = value;
    }
    
    @JsonProperty("type")
    public void setType(String type) {
        this.type = type;
    }
    
    @JsonProperty("type")
    public String getType() {
        return type;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}