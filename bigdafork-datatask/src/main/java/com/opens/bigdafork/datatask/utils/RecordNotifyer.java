package com.opens.bigdafork.datatask.utils;

import com.opens.bigdafork.common.base.observe.AbstractBasicObservable;
import com.opens.bigdafork.common.base.observe.AbstractBasicObserver;
import com.opens.bigdafork.common.base.observe.NotifyEvent;
import com.opens.bigdafork.datatask.record.RecordBean;
import com.opens.bigdafork.datatask.record.WorkRecordMsg;

/**
 * RecordNotifyer.
 *
 */
public class RecordNotifyer extends AbstractBasicObservable {

    public RecordNotifyer(AbstractBasicObserver observer) {
        super(observer);
    }

    /**
     * To notify beginning msg at first.
     * @param recordBean
     * @param startTime
     */
    public void notifyBeginRecord(RecordBean recordBean, long startTime) {
        NotifyEvent notifyEvent = new NotifyEvent();
        WorkRecordMsg workRecordMsg = new WorkRecordMsg();
        workRecordMsg.setRecordBean(recordBean);
        recordBean.setNeedStop(false);
        recordBean.setNeedRec(true);
        recordBean.setStatus(3);
        recordBean.setStatusDesc("run");
        recordBean.setLastStartLongTime(startTime);
        notifyEvent.setPayload(workRecordMsg);
        this.notify(notifyEvent);
    }

    /**
     * To notify success.
     * @param recordBean
     * @param startTime
     * @param overTime
     */
    public void notifySucRecord(RecordBean recordBean,
                                long startTime, long overTime) {
        NotifyEvent notifyEvent = new NotifyEvent();
        WorkRecordMsg workRecordMsg = new WorkRecordMsg();
        workRecordMsg.setRecordBean(recordBean);
        recordBean.setNeedStop(false);
        recordBean.setNeedRec(true);
        recordBean.setStatus(1);
        recordBean.setStatusDesc("ok");
        recordBean.setLastCompTime(TimeUtils.getSimpleDate(overTime));
        recordBean.setLastElapse(TimeUtils.getTimeDiffFormat(startTime, overTime));

        notifyEvent.setPayload(workRecordMsg);
        this.notify(notifyEvent);
    }

    /**
     * To notify fail.
     * @param recordBean
     * @param err
     * @param startTime
     * @param overTime
     */
    public void notifyFailRecord(RecordBean recordBean, String err,
                                        long startTime, long overTime) {
        NotifyEvent notifyEvent = new NotifyEvent();
        WorkRecordMsg workRecordMsg = new WorkRecordMsg();
        workRecordMsg.setRecordBean(recordBean);
        recordBean.setNeedRec(true);
        recordBean.setStatus(2);
        recordBean.setStatusDesc(err);
        recordBean.setLastCompTime(TimeUtils.getSimpleDate(overTime));
        recordBean.setLastElapse(TimeUtils.getTimeDiffFormat(startTime, overTime));

        notifyEvent.setPayload(workRecordMsg);
        this.notify(notifyEvent);

    }

    /**
     * To notify stop but do not record.
     */
    public void notifyStopWithoutRecord() {
        NotifyEvent notifyEvent = new NotifyEvent();
        WorkRecordMsg workRecordMsg = new WorkRecordMsg();
        RecordBean recordBean = new RecordBean();
        recordBean.setNeedRec(false);
        recordBean.setNeedStop(true);
        workRecordMsg.setRecordBean(recordBean);
        notifyEvent.setPayload(workRecordMsg);
        this.notify(notifyEvent);

    }

}
