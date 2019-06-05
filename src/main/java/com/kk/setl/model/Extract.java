package com.kk.setl.model;

import org.codehaus.jackson.annotate.*;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.annotation.Generated;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
@Generated("org.jsonschema2pojo")
@JsonPropertyOrder({
        "sql",
        "csv",
        "limitRows"
})
public class Extract {

    @JsonProperty("sql")
    private String sql;
    @JsonProperty("table")
    private String table;
    @JsonProperty("csv")
    private Csv csv;
    @JsonProperty("data")
    private List<Data> data;
    @JsonProperty("limitRows")
    private int limitRows;
    @JsonProperty("whereexists")
    private String whereexists;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

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
     * The csv
     */
    @JsonProperty("csv")
    public Csv getCsv() {
        return csv;
    }

    /**
     *
     * @param csv
     * The csv
     */
    @JsonProperty("csv")
    public void setCsv(Csv csv) {
        this.csv = csv;
    }

    @JsonProperty("table")
    public String getTable() {
		return table;
	}

    @JsonProperty("table")
	public void setTable(String table) {
		this.table = table;
	}

	/**
     * get data
     * @return
     */
    @JsonProperty("data")
    public List<Data> getData() {
        return data;
    }

    /**
     * set data
     * @param data
     */
    @JsonProperty("data")
    public void setData(List<Data> data) {
        this.data = data;
    }

    /**
     *
     * @return
     * The limitRows
     */
    @JsonProperty("limitRows")
    public int getLimitRows() {
        return limitRows;
    }

    /**
     *
     * @param limitRows
     * The limitRows
     */
    @JsonProperty("limitRows")
    public void setLimitRows(int limitRows) {
        this.limitRows = limitRows;
    }

    @JsonProperty("whereexists")
    public String getWhereexists() {
		return whereexists;
	}

    @JsonProperty("whereexists")
	public void setWhereexists(String whereexists) {
		this.whereexists = whereexists;
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
