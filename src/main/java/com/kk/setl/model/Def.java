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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "fromDS",
        "toDS",
        "defs",
        "extract",
        "transform",
        "load"
})
public class Def {

    @JsonProperty("name")
    private String name;
    @JsonProperty("fromDS")
    private DS fromDS;
    @JsonProperty("toDS")
    private DS toDS;
    @JsonProperty("defs")
    private List<Defs> defs = new ArrayList<Defs>();
    @JsonProperty("extract")
    private Extract extract;
    @JsonProperty("transform")
    private Transform transform;
    @JsonProperty("load")
    private List<Load> loads = new ArrayList<Load>();
    @JsonProperty("threads")
    private int threads = 0;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();
    @JsonIgnore
    private Path filePath;

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
     * The fromDS
     */
    @JsonProperty("fromDS")
    public DS getFromDS() {
        return fromDS;
    }

    /**
     *
     * @param fromDS
     * The fromDS
     */
    @JsonProperty("fromDS")
    public void setFromDS(DS fromDS) {
        this.fromDS = fromDS;
    }

    /**
     *
     * @return
     * The toDS
     */
    @JsonProperty("toDS")
    public DS getToDS() {
        return toDS;
    }

    /**
     *
     * @param toDS
     * The toDS
     */
    @JsonProperty("toDS")
    public void setToDS(DS toDS) {
        this.toDS = toDS;
    }

    /**
     *
     * @return
     * The defs
     */
    @JsonProperty("defs")
    public List<Defs> getDefs() {
        return defs;
    }

    /**
     *
     * @param defs
     * The defs
     */
    @JsonProperty("defs")
    public void setDefs(List<Defs> defs) {
        this.defs = defs;
    }

    /**
     *
     * @return
     * The extract
     */
    @JsonProperty("extract")
    public Extract getExtract() {
        return extract;
    }

    /**
     *
     * @param extract
     * The extract
     */
    @JsonProperty("extract")
    public void setExtract(Extract extract) {
        this.extract = extract;
    }

    /**
     *
     * @return
     * The transform
     */
    @JsonProperty("transform")
    public Transform getTransform() {
        return transform;
    }

    /**
     *
     * @param transform
     * The transform
     */
    @JsonProperty("transform")
    public void setTransform(Transform transform) {
        this.transform = transform;
    }

    /**
     *
     * @return
     * The load
     */
    @JsonProperty("load")
    public List<Load> getLoads() {
        return loads;
    }

    /**
     *
     * @param load
     * The load
     */
    @JsonProperty("load")
    public void setLoad(List<Load> loads) {
        this.loads = loads;
    }

    /**
     * loader threads
     * @return
     */
    @JsonProperty("threads")
    public int getThreads() {
        return threads;
    }

    /**
     * loader threads
     * @param threads
     */
    @JsonProperty("threads")
    public void setThreads(int threads) {
        this.threads = threads;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

    @JsonIgnore
    public Path getFilePath() {
        return filePath;
    }

    @JsonIgnore
    public void setFilePath(Path filePath) {
        this.filePath = filePath;
    }
}