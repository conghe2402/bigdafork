package com.opens.bigdafork.datatask.record;

import com.opens.bigdafork.datatask.config.DataTaskConstants;
import lombok.Data;

/**
 * RecordLogBean.
 */
@Data
public class RecordLogBean {
    private String rid;
    private String tid;
    private String schTaskFlowId;
    private String schTaskId;
    private String taskId;
    private String taskName;
    private String sqlId;
    private String engine;
    private String engineChooseType;
    private String startTime;
    private String endTime;
    private int elapse;
    private String businessDate;
    private int status;
    private String statusDesc;
    private String parDate;

    public String toString() {
        StringBuilder line = new StringBuilder();
        line.append(tid).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(schTaskFlowId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(schTaskId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(taskId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(taskName).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(sqlId).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(engine).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(engineChooseType).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(startTime).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(endTime).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(elapse).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(businessDate).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(status).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(statusDesc).append(DataTaskConstants.RECORD_SEPERATOR)
                .append(parDate);
        return line.toString();
    }
}
