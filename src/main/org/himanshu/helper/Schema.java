package org.himanshu.helper;

import com.fasterxml.jackson.annotation.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "columnName",
        "dataType",
        "maxLength",
        "mandatory",
        "validationRequired"
})
public class Schema implements Serializable {

    @JsonProperty("columnName")
    private String columnName;
    @JsonProperty("dataType")
    private String dataType;
    @JsonProperty("maxLength")
    private Integer maxLength;
    @JsonProperty("mandatory")
    private Boolean mandatory;
    @JsonProperty("validationRequired")
    private Boolean validationRequired;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("columnName")
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty("columnName")
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @JsonProperty("dataType")
    public String getDataType() {
        return dataType;
    }

    @JsonProperty("dataType")
    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @JsonProperty("maxLength")
    public Integer getMaxLength() {
        return maxLength;
    }

    @JsonProperty("maxLength")
    public void setMaxLength(Integer maxLength) {
        this.maxLength = maxLength;
    }

    @JsonProperty("mandatory")
    public Boolean getMandatory() {
        return mandatory;
    }

    @JsonProperty("mandatory")
    public void setMandatory(Boolean mandatory) {
        this.mandatory = mandatory;
    }

    @JsonProperty("validationRequired")
    public Boolean getValidationRequired() {
        return validationRequired;
    }

    @JsonProperty("validationRequired")
    public void setValidationRequired(Boolean validationRequired) {
        this.validationRequired = validationRequired;
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
