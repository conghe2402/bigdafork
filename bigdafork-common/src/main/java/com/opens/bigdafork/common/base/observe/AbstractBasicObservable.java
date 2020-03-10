package com.opens.bigdafork.common.base.observe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Observable;


/**
 * add notification functionality via observable pattern.
 */
public abstract class AbstractBasicObservable extends Observable {

    private AbstractBasicObserver observer;
    private boolean stopWhenFail = false;

    public AbstractBasicObservable() {
        this(true, null);
    }

    public AbstractBasicObservable(boolean stopWhenFail) {
        this(stopWhenFail, null);
    }

    public AbstractBasicObservable(AbstractBasicObserver observer) {
        this(true, observer);
    }

    public AbstractBasicObservable(boolean stopWhenFail, AbstractBasicObserver observer) {
        this.stopWhenFail = stopWhenFail;
        this.observer = observer;
        if (this.observer == null) {
            this.setDefaultOb();
        }
        this.addObserver(this.observer);
    }

    /**
     * The behaviour of default ob is to shutdown app.
     * Its msg type is boolean type,
     * if true it is ok with nothing to do,
     * else[false] app will shutdown immediately.
     */
    public void setDefaultOb() {
        this.observer = new AbstractBasicObserver() {
            private final Logger obLogger = LoggerFactory.getLogger(this.getClass());

            @Override
            public void receive(NotifyEvent arg) {
                if (!arg.isStatus()) {
                    obLogger.warn("operate failure");
                    if (stopWhenFail) {
                        obLogger.warn("program will shutdown");
                        System.exit(0);
                    }
                } else {
                    obLogger.debug("operate success");
                }
            }
        };
    }

    /**
     * Unify notify method.
     * @param notifyMsg
     */
    protected void notify(NotifyEvent notifyMsg) {
        this.setChanged();
        this.notifyObservers(notifyMsg);
    }

}
