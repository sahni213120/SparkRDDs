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

    private String jobName;

    private String jobDescription;

    private Table[] table;

    private String newInsertsQuery;

    private String updatedRecordsInsertsQuery;

    private String updatedRecordsUpdatesQuery;

    private String unchangedRecordsQuery;

    public static CDCProperties getCdcProperties() {
        return cdcProperties;
    }

    public static void setCdcProperties(CDCProperties cdcProperties) {
        CDCProperties.cdcProperties = cdcProperties;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getJobDescription() {
        return jobDescription;
    }

    public void setJobDescription(String jobDescription) {
        this.jobDescription = jobDescription;
    }

    public Table[] getTable() {
        return table;
    }

    public void setTable(Table[] table) {
        this.table = table;
    }

    public String getNewInsertsQuery() {
        return newInsertsQuery;
    }

    public void setNewInsertsQuery(String newInsertsQuery) {
        this.newInsertsQuery = newInsertsQuery;
    }

    public String getUpdatedRecordsInsertsQuery() {
        return updatedRecordsInsertsQuery;
    }

    public void setUpdatedRecordsInsertsQuery(String updatedRecordsInsertsQuery) {
        this.updatedRecordsInsertsQuery = updatedRecordsInsertsQuery;
    }

    public String getUpdatedRecordsUpdatesQuery() {
        return updatedRecordsUpdatesQuery;
    }

    public void setUpdatedRecordsUpdatesQuery(String updatedRecordsUpdatesQuery) {
        this.updatedRecordsUpdatesQuery = updatedRecordsUpdatesQuery;
    }

    public String getUnchangedRecordsQuery() {
        return unchangedRecordsQuery;
    }

    public void setUnchangedRecordsQuery(String unchangedRecordsQuery) {
        this.unchangedRecordsQuery = unchangedRecordsQuery;
    }

    public static CDCProperties getCDCProperties(String jsonFileLocation) throws IOException {

        if (cdcProperties == null) {
            ObjectMapper mapper = new ObjectMapper();
            cdcProperties = mapper.readValue(new File(jsonFileLocation), CDCProperties.class);
        }

        return cdcProperties;
    }
}
