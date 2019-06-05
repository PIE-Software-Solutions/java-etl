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
    @JsonProperty("extthread")
    private Integer extthread;

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

    @JsonProperty("extthread")
	public Integer getExtthread() {
		return extthread;
	}

	@JsonProperty("extthread")
	public void setExtthread(Integer extthread) {
		this.extthread = extthread;
	}
    
    
}
