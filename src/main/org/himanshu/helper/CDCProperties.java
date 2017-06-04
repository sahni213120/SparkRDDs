package org.himanshu.helper;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;

/**
 * Created by himanshu on 6/4/2017.
 */
public class CDCProperties {

    private static CDCProperties cdcProperties;

    private CDCProperties() {

    }

    private String sourceTableName;

    private String targetTableName;

    private String sourceKeyColumn;

    private String targetKeyColumn;

    private String sourceHeaderString;

    private String targetHeaderString;

    private String fileDelimiter;

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public void setTargetTableName(String targetTableName) {
        this.targetTableName = targetTableName;
    }

    public String getSourceKeyColumn() {
        return sourceKeyColumn;
    }

    public void setSourceKeyColumn(String sourceKeyColumn) {
        this.sourceKeyColumn = sourceKeyColumn;
    }

    public String getTargetKeyCOlumn() {
        return targetKeyColumn;
    }

    public void setTargetKeyCOlumn(String targetKeyCOlumn) {
        this.targetKeyColumn = targetKeyCOlumn;
    }

    public String getSourceHeaderString() {
        return sourceHeaderString;
    }

    public void setSourceHeaderString(String sourceHeaderString) {
        this.sourceHeaderString = sourceHeaderString;
    }

    public String getTargetHeaderString() {
        return targetHeaderString;
    }

    public void setTargetHeaderString(String targetHeaderString) {
        this.targetHeaderString = targetHeaderString;
    }

    public String getFileDelimiter() {
        return fileDelimiter;
    }

    public void setFileDelimiter(String fileDelimiter) {
        this.fileDelimiter = fileDelimiter;
    }

    public String[] getSourceColumns() {
        return sourceHeaderString.split(fileDelimiter);
    }

    public String[] getTargetColumns() {
        return targetHeaderString.split(fileDelimiter);
    }

    public static CDCProperties getCDCProperties(String jsonFileLocation) throws IOException {

        if (cdcProperties == null) {
            ObjectMapper mapper = new ObjectMapper();
            cdcProperties = mapper.readValue(new File(jsonFileLocation), CDCProperties.class);
        }

        return cdcProperties;
    }
}
