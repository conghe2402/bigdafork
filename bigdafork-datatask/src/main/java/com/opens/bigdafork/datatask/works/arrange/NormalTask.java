package com.opens.bigdafork.datatask.works.arrange;

import com.opens.bigdafork.datatask.works.TaskRunnableBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NormalTask without tolerance.
 */
public class NormalTask extends TaskChainPart {
    private static final Logger LOGGER = LoggerFactory.getLogger(NormalTask.class);

    public NormalTask(TaskRunnableBackend taskRunnableBackend) {
        super(taskRunnableBackend);
    }

    @Override
    public TaskChainContext iDo(TaskChainContext chainContext) {
        LOGGER.debug("Normal Task run...");
        return super.iDo(chainContext);
    }
}
