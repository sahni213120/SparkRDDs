package org.himanshu.helper;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by himanshu on 8/5/2017.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "objectName",
        "objectDescription",
        "fileDelimiter",
        "schema"
})
public class ObjectSchema implements Serializable {


    @JsonProperty("objectName")
    private String objectName;
    @JsonProperty("objectDescription")
    private String objectDescription;
    @JsonProperty("fileDelimiter")
    private String fileDelimiter;
    @JsonProperty("schema")
    private List<Schema> schema = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("objectName")
    public String getObjectName() {
        return objectName;
    }

    @JsonProperty("objectName")
    public void setObjectName(String objectName) {
        this.objectName = objectName;
    }

    @JsonProperty("objectDescription")
    public String getObjectDescription() {
        return objectDescription;
    }

    @JsonProperty("objectDescription")
    public void setObjectDescription(String objectDescription) {
        this.objectDescription = objectDescription;
    }

    public String getFileDelimiter() {
        return fileDelimiter;
    }

    public void setFileDelimiter(String fileDelimiter) {
        this.fileDelimiter = fileDelimiter;
    }

    @JsonProperty("schema")
    public List<Schema> getSchema() {
        return schema;
    }

    @JsonProperty("schema")
    public void setSchema(List<Schema> schema) {
        this.schema = schema;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }


    public static void main(String[] args) {
        String jsonFileLocation = "src/main/resources/Country.json";

        ObjectMapper mapper = new ObjectMapper();
        try {
            ObjectSchema json = mapper.readValue(new File(jsonFileLocation), ObjectSchema.class);

            System.out.println(json.getSchema().size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

