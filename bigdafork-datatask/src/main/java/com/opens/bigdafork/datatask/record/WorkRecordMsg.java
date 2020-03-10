package com.opens.bigdafork.datatask.record;

import lombok.Data;

/**
 * WorkRecordMsg.
 */
@Data
public class WorkRecordMsg {
    private RecordBean recordBean;
    private RecordLogBean recordLogBean;
    private Boolean configStatus;
    private String configStatusMsg;
}
