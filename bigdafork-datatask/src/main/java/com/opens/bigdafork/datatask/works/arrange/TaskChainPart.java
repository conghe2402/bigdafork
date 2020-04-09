package com.opens.bigdafork.datatask.works.arrange;

import com.opens.bigdafork.common.base.chain.simple.IChainPart;
import com.opens.bigdafork.datatask.exception.TaskFailException;
import com.opens.bigdafork.datatask.record.RecordBean;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.TaskResult;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.RTask;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.RTaskType;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend.SubmitTaskInfo;

/**
 * TaskChainPart.
 */
public abstract class TaskChainPart implements IChainPart<TaskChainContext, TaskChainContext> {
    protected TaskRunnableBackend taskRunnableBackend;

    public TaskChainPart(TaskRunnableBackend taskRunnableBackend) {
        this.taskRunnableBackend = taskRunnableBackend;
    }

    @Override
    public TaskChainContext iDo(TaskChainContext chainContext) {
        RecordBean recordBean = (RecordBean)chainContext.get(TaskChainContext.KEY_RECODE);
        try {
            RTask runnableTask =
                    (RTask)chainContext.get(TaskChainContext.KEY_RUN_TASK);
            TaskResult taskResult = taskRunnableBackend.runSingle(runnableTask);
            this.getResult(chainContext, taskResult);
            this.finish(chainContext);
            return chainContext;
        } catch (TaskFailException e) {
            // notify and stop.
            recordBean.setNeedStop(true);
            this.finish(chainContext);
            throw e;
        }
    }

    /**
     * Get result value from Task Result according to RTaskType.
     * @param chainContext
     * @param taskResult
     */
    protected void getResult(TaskChainContext chainContext,
                             TaskResult taskResult) {
        SubmitTaskInfo submitTaskInfo =
                (SubmitTaskInfo)chainContext.get(TaskChainContext.KEY_SUBMIT_TASKINFO);
        if (RTaskType.set == submitTaskInfo.getTaskType()) {
            chainContext.put(TaskChainContext.KEY_RESULT_SET, taskResult.getResultSet());
        } else if (RTaskType.num == submitTaskInfo.getTaskType()) {
            chainContext.put(TaskChainContext.KEY_COUNT_NUM, taskResult.getCountNum());
        }
    }

    /**
     * Pass submit task info to next TaskChainPart and notify message.
     * @param chainContext
     */
    protected void pass2NextPart(TaskChainContext chainContext) {
        RecordBean recordBean = (RecordBean)chainContext.get(TaskChainContext.KEY_RECODE);
        SubmitTaskInfo submitTaskInfo =
                (SubmitTaskInfo)chainContext.get(TaskChainContext.KEY_SUBMIT_TASKINFO);
        chainContext.next(true);
        recordBean.setNeedStop(false);

        RTask nextRunnableTask = taskRunnableBackend.getRunnableTaskByInfo(
                submitTaskInfo.getAppName(),
                submitTaskInfo.getStandbyEngine(),
                submitTaskInfo.getTaskType(),
                submitTaskInfo.getSql(),
                submitTaskInfo.getStandByConfigs());
        chainContext.put(TaskChainContext.KEY_RUN_TASK, nextRunnableTask);
    }

    protected void finish(TaskChainContext chainContext) {
        chainContext.next(false);
    }

}
