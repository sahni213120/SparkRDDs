package org.himanshu.helper;

/**
 * Created by himanshu on 6/4/2017.
 */
public class Constants {

    public static final String CDC_TABLE_ALIAS = "cdc";
    public static final String MASTER_TABLE_ALIAS = "master";
    public static final String FULL_OUTER_JOIN = "fullouter";
    public static final String STATUS_COLUMN = "EDM_ACTIVE_FL";
    public static final String EFFCTV_DT = "EDM_EFFCTV_DT";
    public static final String XPRTN_DT = "EDM_XPRTN_DT";
    public static final String CREATE_TS = "EDM_CREATE_TS";
    public static final String UPDT_TS = "EDM_UPDT_TS";


    public enum Status {
        Y,
        N
    }


}
