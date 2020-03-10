package com.opens.bigdafork.common.base.observe;

import java.util.Observable;
import java.util.Observer;

/**
 * Add notification functionality via observable pattern.
 * This part is observer.
 */
public abstract class AbstractBasicObserver implements Observer {

    @Override
    public void update(Observable o, Object arg) {
        NotifyEvent event = (NotifyEvent)arg;
        receive(event);
    }

    /**
     * receive msg.
     * @param obj
     */
    public abstract void receive(NotifyEvent obj);
}
