package com.opens.bigdafork.datatask.config;

import com.opens.bigdafork.common.base.config.props.AbstractPropertiesConfig;
import com.opens.bigdafork.common.base.exception.LoadConfigException;
import com.opens.bigdafork.common.base.observe.AbstractBasicObserver;
import com.opens.bigdafork.datatask.record.WorkRecordMsg;

/**
 * TaskConfigLoader.
 * loading fail do not lead to stop sys.
 */
public class TaskPropertiesConfig
        extends AbstractPropertiesConfig<TaskPropertiesConfigLoader> {
    private String fileName;

    public TaskPropertiesConfig(String fileName) {
        this(fileName, true, null);
    }

    public TaskPropertiesConfig(String fileName, AbstractBasicObserver observer) {
        this(fileName, false, observer);
    }

    public TaskPropertiesConfig(String fileName, boolean stopWhenFail,
                                AbstractBasicObserver observer) {
        super(stopWhenFail, null, false);
        this.fileName = fileName;
        this.init();
    }

    @Override
    protected TaskPropertiesConfigLoader newConfigsLoader() throws LoadConfigException {
        return new TaskPropertiesConfigLoader(fileName);
    }

    @Override
    protected Object makePayload(boolean status, String msg) {
        WorkRecordMsg workRecordMsg = new WorkRecordMsg();
        workRecordMsg.setConfigStatus(status);
        workRecordMsg.setConfigStatusMsg(msg);
        return workRecordMsg;
    }
}
