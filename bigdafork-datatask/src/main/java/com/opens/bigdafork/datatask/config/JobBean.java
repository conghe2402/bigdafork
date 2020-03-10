package com.opens.bigdafork.datatask.config;

import lombok.Data;

/**
 * JobBean.
 */
@Data
public class JobBean {
    private String flowId; //0
    private String jobId; //1
    private String jobName; //2
    private String className; //3
    private String businessDate; //4
    private String masterSwitch; //5
    private int useEngine; //6
    private int toleranceEngine; //7
    private int useResLevel; //8
    private int timeout; //9
    private String jobStatus; //10
    private String[] taskParams; // 11-N

}
