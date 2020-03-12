package com.opens.bigdafork.datatask.utils;

import com.opens.bigdafork.datatask.config.JobBean;
import com.opens.bigdafork.datatask.config.TaskPropertiesConfig;
import com.opens.bigdafork.datatask.record.WorkRecorder;
import lombok.Getter;
import lombok.Setter;

/**
 * SingleContext.
 */
public final class SingleContext {

    private static final SingleContext INSTANCE = new SingleContext();

    @Setter @Getter
    private boolean newTag;

    @Setter @Getter
    private TaskPropertiesConfig c1Config;

    @Setter @Getter
    private TaskPropertiesConfig c2Config;

    @Setter @Getter
    private String taskId;

    @Setter @Getter
    private JobBean jobBean;

    @Setter @Getter
    private WorkRecorder workRecorder;

    @Setter @Getter
    private RecordNotifyer recordNotifyer;

    @Setter @Getter
    private String c2CofigPath;

    @Setter @Getter
    private String nasRootPath;

    public static SingleContext get() {
        return INSTANCE;
    }

    public void switchNew() {
        this.newTag = true;
    }

    private SingleContext() {

    }
}
