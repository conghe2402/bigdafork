package com.opens.bigdafork.datatask.works.arrange;

import com.opens.bigdafork.common.base.chain.simple.impl.DefaultServiceChain;
import com.opens.bigdafork.datatask.record.RecordBean;
import com.opens.bigdafork.datatask.utils.TimeUtils;
import com.opens.bigdafork.datatask.works.TaskRunnableBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ProxyAddWorkRecord.
 * Record and Log.
 */
public class ProxyAddWorkRecord implements DefaultServiceChain.IProxyAddWork<TaskChainContext> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyAddWorkRecord.class);
    private TaskRunnableBackend taskRunnableBackend;

    public ProxyAddWorkRecord(TaskRunnableBackend taskRunnableBackend) {
        this.taskRunnableBackend = taskRunnableBackend;
    }

    @Override
    public void doBefore(TaskChainContext arg) {
        long startTime = System.currentTimeMillis();
        RecordBean recordBean = (RecordBean)arg.get("recordBean");

        LOGGER.info(String.format("%s %s start at %s",
                recordBean.getTid(),
                recordBean.getTaskId(), TimeUtils.getSimpleDate(startTime)));
        taskRunnableBackend.getNotifyer().notifyBeginRecord(recordBean, startTime);
    }

    @Override
    public void doAfter(TaskChainContext arg) {
        long overTime = System.currentTimeMillis();
        RecordBean recordBean = (RecordBean)arg.get("recordBean");
        LOGGER.info(String.format("%s %s complete , elapse %s",
                recordBean.getTid(),
                recordBean.getTaskId(),
                TimeUtils.getTimeDiffFormat(recordBean.getLastStartLongTime(), overTime)));

        taskRunnableBackend.getNotifyer().notifySucRecord(recordBean,
                recordBean.getLastStartLongTime(), overTime);
    }

    /**
     * Not be responsible for controlling if stop.
     * @param arg
     * @param e
     */
    @Override
    public void doException(TaskChainContext arg, Exception e) {
        long overTime = System.currentTimeMillis();
        RecordBean recordBean = (RecordBean)arg.get("recordBean");
        //enter here, the exception e only is InvocationException.
        //String errMsg = e.getMessage() == null ? e.getCause().getMessage() : e.getMessage();
        String errMsg = e.getCause().getMessage();
        LOGGER.info(String.format("%s %s fail and end at %s cause : %s",
                recordBean.getTid(),
                recordBean.getTaskId(), TimeUtils.getSimpleDate(overTime),
                errMsg));

        taskRunnableBackend.getNotifyer()
                .notifyFailRecord(recordBean, errMsg,
                        recordBean.getLastStartLongTime(), overTime);
    }
}
