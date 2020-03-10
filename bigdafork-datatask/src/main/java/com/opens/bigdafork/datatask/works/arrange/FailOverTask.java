package com.opens.bigdafork.datatask.works.arrange;

import com.opens.bigdafork.datatask.exception.TaskFailException;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FailOverTask.
 */
public class FailOverTask extends TaskChainPart {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailOverTask.class);

    public FailOverTask(TaskRunnableBackend taskRunnableBackend) {
        super(taskRunnableBackend);
    }

    @Override
    public TaskChainContext iDo(TaskChainContext chainContext) {
        LOGGER.debug("FailOverTask Task run...");
        try {
            chainContext = super.iDo(chainContext);
            return chainContext;
        } catch (TaskFailException e) {
            // notify and next.
            this.pass2NextPart(chainContext);
            throw e;
        }
    }
}
