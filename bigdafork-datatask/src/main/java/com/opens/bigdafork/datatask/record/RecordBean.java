package com.opens.bigdafork.datatask.record;

import com.opens.bigdafork.datatask.config.DataTaskConstants;
import lombok.Data;

/**
 * RecordBean.
 */
@Data
public class RecordBean {
    private boolean needStop;
    private boolean needRec = true;
    //fields
    private String tid; // def  schTaskFlowId+schTaskId+sqlId
    private String schTaskFlowId; // job -> def
    private String schTaskId; // job -> def
    private String taskId; // job -> def
    private String taskDesc; // job -> def
    private String sqlId; // -> def
    private String sqlContent; // -> def
    private int status; // -> def
    private String statusDesc; // -> def
    private String lastElapse; // -> def
    private String lastCompTime; // -> def
    //enable : def
    //author : def
    //insert_time : def

    // temp params.
    private long lastStartLongTime;

    public String toString() {
        StringBuilder line = new StringBuilder();
        line.append(tid).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(schTaskFlowId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(schTaskId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(taskId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(taskDesc).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(sqlId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(status).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(statusDesc).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(lastElapse).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(lastCompTime);

        return line.toString();
    }
}
