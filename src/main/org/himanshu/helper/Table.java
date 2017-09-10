package org.himanshu.helper;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import java.io.Serializable;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "tableName",
        "headerString",
        "location"
})
public class Table implements Serializable {

    @JsonProperty("tableName")
    private String tableName;
    @JsonProperty("headerType")
    private String headerString;
    @JsonProperty("location")
    private String location;

    @JsonProperty("tableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("tableName")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonProperty("headerString")
    public String getHeaderString() {
        return headerString;
    }

    @JsonProperty("headerString")
    public void setHeaderString(String headerString) {
        this.headerString = headerString;
    }

    @JsonProperty("location")
    public String getLocation() {
        return location;
    }

    @JsonProperty("location")
    public void setLocation(String location) {
        this.location = location;
    }

}
