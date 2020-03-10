package com.opens.bigdafork.datatask.works.arrange;

import com.opens.bigdafork.datatask.exception.TaskFailException;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.RTask;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.SubmitTaskInfo;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.TaskResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * FailoverAndTimeoutTask.
 */
public class FailoverAndTimeoutTask extends TaskChainPart {
    private static final Logger LOGGER = LoggerFactory.getLogger(FailoverAndTimeoutTask.class);

    public FailoverAndTimeoutTask(TaskRunnableBackend taskRunnableBackend) {
        super(taskRunnableBackend);
    }

    @Override
    public TaskChainContext iDo(TaskChainContext chainContext) {
        LOGGER.debug("FailoverAndTimeoutTask Task run...");
        SubmitTaskInfo submitTaskInfo =
                (SubmitTaskInfo)chainContext.get(TaskChainContext.KEY_SUBMIT_TASKINFO);
        try {
            RTask runnableTask =
                    (RTask)chainContext.get(TaskChainContext.KEY_RUN_TASK);

            TaskResult taskResult =
                    taskRunnableBackend.runSingleTimeout(runnableTask,
                            submitTaskInfo.getTimeout());
            this.getResult(chainContext, taskResult);
            this.finish(chainContext);
            return chainContext;
        } catch (TaskFailException e) {
            // notify and next.
            this.pass2NextPart(chainContext);
            throw e;
        }
    }
}
